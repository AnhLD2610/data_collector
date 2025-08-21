
import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, List
from web3 import Web3

try:
	from lending_vault import (
		HYPERFI_UI_DATA_PROVIDER,
		HYPERFI_POOL_PROVIDER,
		HYPERLEND_UI_DATA_PROVIDER,
		HYPERLEND_POOL_PROVIDER,
		WHYPE_ADDRESS,
		HYPERFI_UI_DATA_PROVIDER_ABI,
		HYPERLEND_UI_DATA_PROVIDER_ABI,
		ERC20_ABI,
	)
except ModuleNotFoundError:
	# Fallback: add repo root and get_new_data to sys.path, then retry
	import sys as _sys
	import os as _os
	_here = _os.path.dirname(__file__)
	_root = _os.path.abspath(_os.path.join(_here, ".."))
	_candidate = _os.path.join(_root, "get_new_data")
	for _p in [_candidate, _root, _here]:
		if _p not in _sys.path:
			_sys.path.insert(0, _p)
	try:
		from lending_vault import (
			HYPERFI_UI_DATA_PROVIDER,
			HYPERFI_POOL_PROVIDER,
			HYPERLEND_UI_DATA_PROVIDER,
			HYPERLEND_POOL_PROVIDER,
			WHYPE_ADDRESS,
			HYPERFI_UI_DATA_PROVIDER_ABI,
			HYPERLEND_UI_DATA_PROVIDER_ABI,
			ERC20_ABI,
		)
	except ModuleNotFoundError:
		from get_new_data.lending_vault import (
			HYPERFI_UI_DATA_PROVIDER,
			HYPERFI_POOL_PROVIDER,
			HYPERLEND_UI_DATA_PROVIDER,
			HYPERLEND_POOL_PROVIDER,
			WHYPE_ADDRESS,
			HYPERFI_UI_DATA_PROVIDER_ABI,
			HYPERLEND_UI_DATA_PROVIDER_ABI,
			ERC20_ABI,
		)

RPC_URL = "https://rpc.hyperliquid.xyz/evm"
STEP_SECONDS = 300  # 5 minutes
DEFAULT_ANCHOR_MINUTES = 120
DEFAULT_SLEEP_SECONDS = 2
DEFAULT_AVG_BLOCK_TIME = 2.0
MAX_BACKOFF_ON_LIMIT_S = 600

HEADERS = [
	"timestamp",
	"block",
	"protocol",
	"supply_rate",
	"borrow_rate",
	"tvl_whype",
	"utilization_pct",
	"tvl_usd",
	"hype_price_usd",
]

def parse_time_to_unix(value: str) -> int:
	s = value.strip()
	try:
		return int(s)
	except Exception:
		pass
	if s.endswith("Z"):
		s = s[:-1] + "+00:00"
	try:
		dt = datetime.fromisoformat(s)
	except Exception:
		dt = datetime.fromisoformat(s.replace(" ", "T"))
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return int(dt.timestamp())

def floor_to_step(ts: int, step: int) -> int:
	return ts - (ts % step)

def get_price_series_usd(start_ts: int, end_ts: int, coin_id: str = "hyperliquid", timeout: float = 10.0) -> Dict[int, float]:
	url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
	params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}
	try:
		resp = requests.get(url, params=params, timeout=timeout)
		resp.raise_for_status()
		data = resp.json()
		prices = data.get("prices", [])
		price_map: Dict[int, float] = {}
		for p in prices:
			if len(p) >= 2:
				ts = int(p[0] // 1000)
				price = float(p[1])
				bucket = floor_to_step(ts, STEP_SECONDS)
				price_map[bucket] = price
		return price_map
	except Exception as e:
		print(f"Could not fetch price series: {e}")
		return {}

class ProtocolConfig:
	def __init__(self, name: str, ui: str, pool: str, abi: List[Dict], idx: Dict[str, int]):
		self.name = name
		self.ui = ui
		self.pool = pool
		self.abi = abi
		self.idx = idx

class BackfillPrecise:
	def __init__(self, rpc_url: str, sleep_seconds: int, avg_block_time: float, tolerance_s: int, refine_iters: int):
		self.w3 = Web3(Web3.HTTPProvider(rpc_url))
		self.sleep_seconds = sleep_seconds
		if not self.w3.is_connected():
			raise RuntimeError("Cannot connect to RPC")
		try:
			self.latest_block = int(self.w3.eth.block_number)
		except Exception:
			self.latest_block = 0
		self.latest_ts = int(time.time())
		self.avg_block_time = avg_block_time
		self.tolerance_s = tolerance_s
		self.refine_iters = max(1, refine_iters)
		self.archived_calls_used = 0

	def estimate_block_for_ts(self, ts: int) -> int:
		delta_sec = max(0, self.latest_ts - ts)
		blocks_back = int(round(delta_sec / self.avg_block_time))
		return max(0, self.latest_block - blocks_back)

	def _get_block_ts(self, block_number: int) -> Optional[int]:
		try:
			b = self.w3.eth.get_block(block_number)
			return int(b.timestamp)
		except Exception:
			return None

	def refine_block_for_ts(self, target_ts: int) -> int:
		"""Iteratively refine block number so its timestamp is close to target_ts.
		Avoids full binary search; typically converges in a few steps.
		"""
		b_est = self.estimate_block_for_ts(target_ts)
		for _ in range(self.refine_iters):
			ts_est = self._get_block_ts(b_est)
			if ts_est is None:
				break
			dt = target_ts - ts_est
			if abs(dt) <= self.tolerance_s:
				return b_est
			# Take a proportional step, clamp to at least 1 block
			delta_blocks = int(round(dt / self.avg_block_time))
			if delta_blocks == 0:
				delta_blocks = 1 if dt > 0 else -1
			b_est = max(0, min(self.latest_block, b_est + delta_blocks))
		return b_est

	def _fetch_for_protocol(self, cfg: ProtocolConfig, block_number: int) -> Optional[Dict[str, float]]:
		try:
			ui_contract = self.w3.eth.contract(address=Web3.to_checksum_address(cfg.ui), abi=cfg.abi)
			reserves_data, _ = ui_contract.functions.getReservesData(
				Web3.to_checksum_address(cfg.pool)
			).call(block_identifier=block_number)
			self.archived_calls_used += 1
			for reserve in reserves_data:
				if reserve[0].lower() == WHYPE_ADDRESS.lower():
					decimals = int(reserve[3])
					liquidity_rate_ray = int(reserve[cfg.idx["liquidity_rate_idx"]])
					variable_borrow_rate_ray = int(reserve[cfg.idx["variable_borrow_rate_idx"]])
					atoken_address = reserve[cfg.idx["atoken_idx"]]
					available_liquidity = int(reserve[cfg.idx["available_liquidity_idx"]])
					supply_rate = (liquidity_rate_ray / 1e27) * 100.0
					borrow_rate = (variable_borrow_rate_ray / 1e27) * 100.0
					atoken = self.w3.eth.contract(address=Web3.to_checksum_address(atoken_address), abi=ERC20_ABI)
					total_supply_raw = atoken.functions.totalSupply().call(block_identifier=block_number)
					self.archived_calls_used += 1
					scale = 10 ** decimals
					tvl_whype = total_supply_raw / scale
					avail = available_liquidity / scale
					util_pct = ((tvl_whype - avail) / tvl_whype * 100.0) if tvl_whype > 0 else 0.0
					return {
						"supply_rate": float(supply_rate),
						"borrow_rate": float(borrow_rate),
						"tvl_whype": float(tvl_whype),
						"utilization_pct": float(util_pct),
					}
			return None
		except Exception as e:
			msg = str(e)
			if "3000 archived" in msg or "-32005" in msg or "archived" in msg.lower():
				print(f" Provider archived limit hit while fetching {cfg.name} @ block {block_number}. Backing off...")
				time.sleep(MAX_BACKOFF_ON_LIMIT_S)
				return None
			print(f" Error fetching {cfg.name} @ block {block_number}: {e}")
			return None

	def protocols(self) -> List[ProtocolConfig]:
		return [
			ProtocolConfig(
				"HyperFi",
				HYPERFI_UI_DATA_PROVIDER,
				HYPERFI_POOL_PROVIDER,
				HYPERFI_UI_DATA_PROVIDER_ABI,
				{"liquidity_rate_idx": 15, "variable_borrow_rate_idx": 16, "atoken_idx": 19, "available_liquidity_idx": 23},
			),
			ProtocolConfig(
				"HyperLend",
				HYPERLEND_UI_DATA_PROVIDER,
				HYPERLEND_POOL_PROVIDER,
				HYPERLEND_UI_DATA_PROVIDER_ABI,
				{"liquidity_rate_idx": 14, "variable_borrow_rate_idx": 15, "atoken_idx": 17, "available_liquidity_idx": 20},
			),
		]


def ensure_csv(outfile: str):
	if not os.path.exists(outfile):
		with open(outfile, "w", newline="", encoding="utf-8") as f:
			w = csv.DictWriter(f, fieldnames=HEADERS)
			w.writeheader()

def append_rows(outfile: str, rows: List[Dict[str, str]]):
	if not rows:
		return
	with open(outfile, "a", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=HEADERS)
		for r in rows:
			w.writerow(r)


def main():
	parser = argparse.ArgumentParser(description="Accurate-but-fast 5m backfill (refined block timestamps)")
	parser.add_argument("--out", required=True, help="Output CSV file")
	parser.add_argument("--from", dest="from_ts", required=True, help="Start time (unix or ISO)")
	parser.add_argument("--to", dest="to_ts", required=True, help="End time (unix or ISO)")
	parser.add_argument("--anchor-minutes", type=int, default=DEFAULT_ANCHOR_MINUTES, help="Anchor spacing in minutes (default 120)")
	parser.add_argument("--sleep-seconds", type=int, default=DEFAULT_SLEEP_SECONDS, help="Sleep between anchors (default 2)")
	parser.add_argument("--daily-archived-budget", type=int, default=2400, help="Max archived calls to use (default 2400)")
	parser.add_argument("--tolerance-seconds", type=int, default=3, help="Refinement tolerance in seconds (default 3)")
	parser.add_argument("--refine-iters", type=int, default=6, help="Max refinement iterations per anchor (default 6)")
	parser.add_argument("--exact-fill", action="store_true", help="If set, fetch exact metrics at every 5m step (uses more archived calls)")

	args = parser.parse_args()

	start = parse_time_to_unix(args.from_ts)
	end = parse_time_to_unix(args.to_ts)
	if end < start:
		start, end = end, start

	print("=== Accurate-but-fast 5m backfill ===")
	print(f"Out: {args.out}")
	print(f"Window: {datetime.fromtimestamp(start, tz=timezone.utc).isoformat()} -> {datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}")
	print(f"Anchor: every {args.anchor_minutes} minutes; Fill: {STEP_SECONDS}s steps; Exact fill: {args.exact_fill}")
	print()

	bf = BackfillPrecise(RPC_URL, sleep_seconds=args.sleep_seconds, avg_block_time=DEFAULT_AVG_BLOCK_TIME, tolerance_s=args.tolerance_seconds, refine_iters=args.refine_iters)
	ensure_csv(args.out)

	# Price series once
	price_map = get_price_series_usd(start, end)

	# Build anchor timestamps
	anchor_step = args.anchor_minutes * 60
	anchors: List[int] = []
	cur = floor_to_step(start, STEP_SECONDS)
	while cur <= end:
		if (cur - start) % anchor_step == 0:
			anchors.append(cur)
		cur += STEP_SECONDS
	if anchors and anchors[-1] != end:
		anchors.append(floor_to_step(end, STEP_SECONDS))

	print(f"Anchors: {len(anchors)}")

	last_metrics_per_protocol: Dict[str, Dict[str, float]] = {}
	written = 0

	for i, anchor_ts in enumerate(anchors):
		# Before making state calls, check budget for this anchor (2 per protocol)
		if bf.archived_calls_used + (len(bf.protocols()) * 2) > args.daily_archived_budget:
			print(f"Budget reached ({bf.archived_calls_used}/{args.daily_archived_budget}). Stop and resume tomorrow.")
			break
		try:
			block_refined = bf.refine_block_for_ts(anchor_ts)
			iso_anchor = datetime.fromtimestamp(anchor_ts, tz=timezone.utc).isoformat()
			print(f"Anchor {i+1}/{len(anchors)} @ {iso_anchor} → refined block {block_refined} (archived used {bf.archived_calls_used})")

			# Fetch anchor metrics
			anchor_rows: List[Dict[str, str]] = []
			price_anchor = price_map.get(floor_to_step(anchor_ts, STEP_SECONDS))
			for cfg in bf.protocols():
				m = bf._fetch_for_protocol(cfg, block_refined)
				if m:
					last_metrics_per_protocol[cfg.name] = m
					price = price_anchor  # may be None
					tvl_usd = (m["tvl_whype"] * price) if price is not None else None
					anchor_rows.append({
						"timestamp": iso_anchor,
						"block": str(block_refined),
						"protocol": cfg.name,
						"supply_rate": f"{m['supply_rate']:.6f}",
						"borrow_rate": f"{m['borrow_rate']:.6f}",
						"tvl_whype": f"{m['tvl_whype']:.6f}",
						"utilization_pct": f"{m['utilization_pct']:.6f}",
						"tvl_usd": f"{tvl_usd:.6f}" if tvl_usd is not None else "",
						"hype_price_usd": f"{price:.6f}" if price is not None else "",
					})
			# Write anchor rows
			append_rows(args.out, anchor_rows)
			written += len(anchor_rows)

			# Fill rows between this anchor and next
			next_ts = anchors[i+1] if i+1 < len(anchors) else None
			if next_ts is not None and last_metrics_per_protocol:
				fill_ts = anchor_ts + STEP_SECONDS
				while fill_ts < next_ts:
					fill_iso = datetime.fromtimestamp(fill_ts, tz=timezone.utc).isoformat()
					fill_rows: List[Dict[str, str]] = []
					if args.exact_fill:
						# Per 5m exact: budget check (2 per protocol)
						if bf.archived_calls_used + (len(bf.protocols()) * 2) > args.daily_archived_budget:
							print(f"⏹ Budget reached during fill ({bf.archived_calls_used}/{args.daily_archived_budget}).")
							break
						block_f = bf.refine_block_for_ts(fill_ts)
						price = price_map.get(floor_to_step(fill_ts, STEP_SECONDS))
						for cfg in bf.protocols():
							m = bf._fetch_for_protocol(cfg, block_f)
							if not m:
								continue
							tvl_usd = (m["tvl_whype"] * price) if price is not None else None
							fill_rows.append({
								"timestamp": fill_iso,
								"block": str(block_f),
								"protocol": cfg.name,
								"supply_rate": f"{m['supply_rate']:.6f}",
								"borrow_rate": f"{m['borrow_rate']:.6f}",
								"tvl_whype": f"{m['tvl_whype']:.6f}",
								"utilization_pct": f"{m['utilization_pct']:.6f}",
								"tvl_usd": f"{tvl_usd:.6f}" if tvl_usd is not None else "",
								"hype_price_usd": f"{price:.6f}" if price is not None else "",
							})
					else:
						# Fast fill: reuse last-known protocol metrics; price per 5m bucket
						price = price_map.get(floor_to_step(fill_ts, STEP_SECONDS))
						for proto, m in last_metrics_per_protocol.items():
							tvl_usd = (m["tvl_whype"] * price) if price is not None else None
							fill_rows.append({
								"timestamp": fill_iso,
								"block": str(block_refined),  # reference anchor block
								"protocol": proto,
								"supply_rate": f"{m['supply_rate']:.6f}",
								"borrow_rate": f"{m['borrow_rate']:.6f}",
								"tvl_whype": f"{m['tvl_whype']:.6f}",
								"utilization_pct": f"{m['utilization_pct']:.6f}",
								"tvl_usd": f"{tvl_usd:.6f}" if tvl_usd is not None else "",
								"hype_price_usd": f"{price:.6f}" if price is not None else "",
							})
					append_rows(args.out, fill_rows)
					written += len(fill_rows)
					fill_ts += STEP_SECONDS

			# pause slightly between anchors
			time.sleep(max(1, args.sleep_seconds))

		except Exception as e:
			print(f" Anchor error @ {anchor_ts}: {e}")
			# still sleep and continue
			time.sleep(max(1, args.sleep_seconds))

	print(f"Done. Wrote ~{written} rows to {args.out}; archived used {bf.archived_calls_used}/{args.daily_archived_budget}")

if __name__ == "__main__":
	main()


