#!/usr/bin/env python3
"""
Real-time 5-minute data collection (absolute accuracy)
- Fetches exact on-chain data for every 5-minute timestamp
- Uses binary search to find the precise block for a timestamp
- More accurate than block estimation (no time skew)

Fields per row:
- timestamp, block, protocol, supply_rate, borrow_rate, tvl_whype, utilization_pct, tvl_usd, hype_price_usd
"""

import os
import csv
import time
import argparse
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, List
from web3 import Web3

# Add parent directory to path to import from get_new_data
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "get_new_data"))

# Reuse definitions from existing code
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

RPC_URL = "https://rpc.hyperliquid.xyz/evm"
STEP_SECONDS = 300  # 5 minutes
DEFAULT_SLEEP_SECONDS = 3
MAX_BACKOFF_ON_LIMIT_S = 600  # 10 minutes
BATCH_SIZE = 20

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
        print(f"⚠️ Could not fetch price series: {e}")
        return {}

class ProtocolConfig:
    def __init__(self, name: str, ui: str, pool: str, abi: List[Dict], idx: Dict[str, int]):
        self.name = name
        self.ui = ui
        self.pool = pool
        self.abi = abi
        self.idx = idx

class RealTimeCollector:
    def __init__(self, rpc_url: str, sleep_seconds: int):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.sleep_seconds = sleep_seconds
        if not self.w3.is_connected():
            raise RuntimeError("Cannot connect to RPC")

        self.total_calls_made = 0
        self.successful_fetches = 0
        self.failed_fetches = 0

    def find_block_by_timestamp(self, target_ts: int) -> int:
        """
        Find the exact block with timestamp <= target_ts (binary search).
        """
        latest = self.w3.eth.block_number
        # Start from block 1 instead of 0 to avoid genesis block issues
        earliest = 1

        try:
            ts_earliest = self.w3.eth.get_block(earliest)["timestamp"]
            ts_latest = self.w3.eth.get_block(latest)["timestamp"]
        except Exception as e:
            if "invalid block height" in str(e):
                # If we can't access early blocks, try a higher starting point
                earliest = max(1, latest - 1000000)  # Go back ~1M blocks
                try:
                    ts_earliest = self.w3.eth.get_block(earliest)["timestamp"]
                except Exception:
                    # If still failing, just use a recent block range
                    earliest = max(1, latest - 10000)
                    ts_earliest = self.w3.eth.get_block(earliest)["timestamp"]
            else:
                raise e

        # Check if target timestamp is in the future
        current_time = int(time.time())
        if target_ts > current_time:
            raise ValueError(f"Target timestamp {target_ts} is in the future (current: {current_time})")
        
        # Check if target timestamp is beyond available blockchain data
        if target_ts > ts_latest:
            print(f"⚠️ Target timestamp {target_ts} is beyond latest block timestamp {ts_latest}, using latest block")
            return latest
        
        if target_ts <= ts_earliest:
            return earliest

        lo, hi = earliest, latest
        while lo <= hi:
            mid = (lo + hi) // 2
            try:
                block = self.w3.eth.get_block(mid)
                ts = block["timestamp"]

                if ts == target_ts:
                    return mid
                elif ts < target_ts:
                    lo = mid + 1
                else:
                    hi = mid - 1
            except Exception as e:
                if "invalid block height" in str(e):
                    # If this block is not accessible, adjust the search range
                    hi = mid - 1
                else:
                    raise e

        return hi

    def _fetch_for_protocol(self, cfg: ProtocolConfig, block_number: int) -> Optional[Dict[str, float]]:
        try:
            ui_contract = self.w3.eth.contract(address=Web3.to_checksum_address(cfg.ui), abi=cfg.abi)
            reserves_data, _ = ui_contract.functions.getReservesData(
                Web3.to_checksum_address(cfg.pool)
            ).call(block_identifier=block_number)
            self.total_calls_made += 1

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
                    self.total_calls_made += 1

                    scale = 10 ** decimals
                    tvl_whype = total_supply_raw / scale
                    avail = available_liquidity / scale
                    util_pct = ((tvl_whype - avail) / tvl_whype * 100.0) if tvl_whype > 0 else 0.0

                    self.successful_fetches += 1
                    return {
                        "supply_rate": float(supply_rate),
                        "borrow_rate": float(borrow_rate),
                        "tvl_whype": float(tvl_whype),
                        "utilization_pct": float(util_pct),
                    }
            return None
        except Exception as e:
            self.failed_fetches += 1
            msg = str(e)
            if "archived" in msg.lower():
                print(f"⛔ Archived block limit hit for {cfg.name} @ block {block_number}. Backing off...")
                time.sleep(MAX_BACKOFF_ON_LIMIT_S)
                return None
            elif "429" in msg or "rate limit" in msg.lower():
                print(f"⏱️ Rate limit hit, sleeping longer...")
                time.sleep(30)
                return None
            print(f"⚠️ Error fetching {cfg.name} @ block {block_number}: {e}")
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
    parser = argparse.ArgumentParser(description="Real-time 5m data collection (absolute accuracy)")
    parser.add_argument("--out", required=True, help="Output CSV file")
    parser.add_argument("--from", dest="from_ts", required=True, help="Start time (unix or ISO)")
    parser.add_argument("--to", dest="to_ts", required=True, help="End time (unix or ISO)")
    parser.add_argument("--sleep-seconds", type=int, default=DEFAULT_SLEEP_SECONDS, help="Sleep between timestamps (default 3)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Process in batches (default 20)")
    parser.add_argument("--resume", action="store_true", help="Resume from last timestamp in existing file")

    args = parser.parse_args()

    start = parse_time_to_unix(args.from_ts)
    end = parse_time_to_unix(args.to_ts)
    if end < start:
        start, end = end, start

    # Validate that dates are not in the future
    current_time = int(time.time())
    if start > current_time:
        print(f"❌ Error: Start time {datetime.fromtimestamp(start, tz=timezone.utc).isoformat()} is in the future")
        print(f"   Current time: {datetime.fromtimestamp(current_time, tz=timezone.utc).isoformat()}")
        return
    if end > current_time:
        print(f"⚠️ Warning: End time {datetime.fromtimestamp(end, tz=timezone.utc).isoformat()} is in the future")
        print(f"   Current time: {datetime.fromtimestamp(current_time, tz=timezone.utc).isoformat()}")
        print(f"   Adjusting end time to current time")
        end = current_time

    print("=== Real-time 5m data collection (absolute accuracy) ===")
    print(f"Out: {args.out}")
    print(f"Window: {datetime.fromtimestamp(start, tz=timezone.utc).isoformat()} -> {datetime.fromtimestamp(end, tz=timezone.utc).isoformat()}")
    print(f"Step: {STEP_SECONDS}s; Sleep: {args.sleep_seconds}s; Batch: {args.batch_size}")
    print()

    collector = RealTimeCollector(RPC_URL, sleep_seconds=args.sleep_seconds)
    ensure_csv(args.out)

    # Build all timestamps
    timestamps: List[int] = []
    cur = floor_to_step(start, STEP_SECONDS)
    while cur <= end:
        timestamps.append(cur)
        cur += STEP_SECONDS

    # Resume functionality
    if args.resume and os.path.exists(args.out):
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                last_ts = None
                for row in reader:
                    if row.get("timestamp"):
                        last_ts = parse_time_to_unix(row["timestamp"])

                if last_ts:
                    timestamps = [ts for ts in timestamps if ts > last_ts]
                    print(f"📍 Resuming from {datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()}")
                    print(f"📍 Remaining timestamps: {len(timestamps)}")
        except Exception as e:
            print(f"⚠️ Could not resume: {e}")

    # Get price series once
    price_map = get_price_series_usd(start, end)
    print(f"💰 Loaded {len(price_map)} price points")

    total_timestamps = len(timestamps)
    written = 0

    for batch_start in range(0, len(timestamps), args.batch_size):
        batch_end = min(batch_start + args.batch_size, len(timestamps))
        batch_timestamps = timestamps[batch_start:batch_end]

        print(f"\n📦 Processing batch {batch_start//args.batch_size + 1} ({batch_start+1}-{batch_end}/{total_timestamps})")

        for i, ts in enumerate(batch_timestamps):
            try:
                block_est = collector.find_block_by_timestamp(ts)
                iso_time = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

                print(f"⏰ {batch_start + i + 1}/{total_timestamps}: {iso_time} → block {block_est}")

                rows: List[Dict[str, str]] = []
                price = price_map.get(floor_to_step(ts, STEP_SECONDS))

                for cfg in collector.protocols():
                    metrics = collector._fetch_for_protocol(cfg, block_est)
                    if metrics:
                        tvl_usd = (metrics["tvl_whype"] * price) if price is not None else None
                        rows.append({
                            "timestamp": iso_time,
                            "block": str(block_est),
                            "protocol": cfg.name,
                            "supply_rate": f"{metrics['supply_rate']:.6f}",
                            "borrow_rate": f"{metrics['borrow_rate']:.6f}",
                            "tvl_whype": f"{metrics['tvl_whype']:.6f}",
                            "utilization_pct": f"{metrics['utilization_pct']:.6f}",
                            "tvl_usd": f"{tvl_usd:.6f}" if tvl_usd is not None else "",
                            "hype_price_usd": f"{price:.6f}" if price is not None else "",
                        })

                append_rows(args.out, rows)
                written += len(rows)

                if (batch_start + i + 1) % 10 == 0:
                    success_rate = (collector.successful_fetches / max(1, collector.successful_fetches + collector.failed_fetches)) * 100
                    print(f"  📊 Stats: {collector.total_calls_made} calls, {success_rate:.1f}% success rate")

                if i < len(batch_timestamps) - 1:
                    time.sleep(args.sleep_seconds)

            except KeyboardInterrupt:
                print(f"\n⏹️ Interrupted by user. Written {written} rows so far.")
                return
            except Exception as e:
                print(f"❌ Error at timestamp {ts}: {e}")
                time.sleep(args.sleep_seconds)

        if batch_end < len(timestamps):
            print(f"  💤 Batch complete, sleeping 10s before next batch...")
            time.sleep(10)

    final_success_rate = (collector.successful_fetches / max(1, collector.successful_fetches + collector.failed_fetches)) * 100
    print(f"\n✅ Done! Written {written} rows to {args.out}")
    print(f"📊 Final stats: {collector.total_calls_made} total calls, {final_success_rate:.1f}% success rate")

if __name__ == "__main__":
    main()
