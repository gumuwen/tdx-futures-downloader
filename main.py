# -*- coding: utf-8 -*-
"""通达信期货通 TQ 数据下载器命令行程序。"""
import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import pandas as pd

from config import (
    TQ_URL, DEFAULT_COUNT, KLINE_DIR, TICK_DIR, META_DIR, PERIODS,
    REFRESH_BASE, MAX_ATTEMPTS, RETRY_WAIT, TICK_CLOSE_TIME
)
from tdx.api import TQ, clean_code
from tdx.calendar import build_calendar, kline_frame
from tdx.contract import discover_contracts, is_future

_print_lock = threading.Lock()
_refresh_lock = threading.Lock()
_refreshed_lock = threading.Lock()
_refreshed = set()
_sample_done = False

def date8(x):
    import re
    return datetime.strptime(re.sub(r"\D", "", str(x))[:8], "%Y%m%d").date()

def fmtdate(d):
    return d.strftime("%Y%m%d")

def atomic_write(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, path)

def read_parquet(path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"[WARN] 读取失败，将重建 {path}: {e}")
        return pd.DataFrame()

def ensure_cache(tq, code, name):
    base = REFRESH_BASE[name]
    key = (code, base)
    with _refreshed_lock:
        if key in _refreshed:
            return True, "已刷新"
    ok, msg = tq.refresh(code, PERIODS[base][0], refresh_lock=_refresh_lock)
    if ok:
        with _refreshed_lock:
            _refreshed.add(key)
    return ok, msg

def update_kline(tq, cal, code, name, count, start, end, refresh, rebuild, log):
    global _sample_done
    period, folder = PERIODS[name]
    daily = name == "1d"
    path = KLINE_DIR / folder / f"{code}.parquet"
    local = pd.DataFrame() if rebuild else read_parquet(path)
    if not local.empty and not {"trade_date", "datetime"} <= set(local.columns):
        log(f"    {name}: 本地为旧格式，忽略并重建")
        local = pd.DataFrame()
    last_trade = None
    if not local.empty:
        local["datetime"] = pd.to_datetime(local["datetime"], errors="coerce")
        last_trade = date8(local["trade_date"].max())

    req_start = ""
    if start:
        s = date8(start)
        if last_trade and date8(local["trade_date"].min()) <= s:
            s = max(s, last_trade - timedelta(days=5))
        req_start = fmtdate(s)
    elif last_trade:
        req_start = fmtdate(last_trade - timedelta(days=5))

    def read():
        if req_start:
            r = kline_frame(tq.kline(code, period, -1, req_start,
                            end or fmtdate(date.today() + timedelta(days=4))), code, daily)
            if r.empty and not end:
                r = kline_frame(tq.kline(code, period, -1, req_start, ""), code, daily)
            return r
        return kline_frame(tq.kline(code, period, count, "", end), code, daily).tail(count)

    remote = pd.DataFrame()
    for attempt in range(MAX_ATTEMPTS):
        if refresh:
            if attempt == 0:
                ok, msg = ensure_cache(tq, code, name)
            else:
                ok, msg = tq.refresh(code, period, refresh_lock=_refresh_lock)
            if not ok:
                log(f"    {name}: refresh_kline 失败：{msg}")
        remote = read()
        suspicious = remote.empty or (daily and local.empty and count > 1 and len(remote) <= 1)
        if not suspicious:
            break
        if attempt + 1 < MAX_ATTEMPTS:
            time.sleep(RETRY_WAIT)
    if remote.empty:
        log(f"    {name}: TQ返回空数据")
        return

    if daily:
        remote["trade_date"] = remote["datetime"].dt.strftime("%Y%m%d")
        remote["is_night"] = False
    else:
        remote = cal.fix_kline(remote)
        if not _sample_done and remote["is_night"].any():
            _sample_done = True
            s = remote[remote["is_night"]].tail(3)
            log("    [校验] 夜盘/凌晨 tdx_time→真实datetime: " +
                "; ".join(f"{a}→{b}" for a, b in zip(s["tdx_time"], s["datetime"])))
    if start:
        remote = remote[remote["trade_date"] >= fmtdate(date8(start))]
    if end:
        remote = remote[remote["trade_date"] <= fmtdate(date8(end))]
    if remote.empty:
        log(f"    {name}: 指定日期范围内无数据")
        return
    before = len(local)
    old_last = local["datetime"].max() if before else None
    merged = remote if local.empty else pd.concat([local, remote], ignore_index=True)
    merged = (merged.sort_values("datetime").drop_duplicates("datetime", keep="last")
              .reset_index(drop=True))
    atomic_write(merged, path)
    log(f"    {name}: 远端={len(remote)} 原有={before} 合并后={len(merged)} "
        f"原末根={old_last} 现末根={merged['datetime'].max()}")

def tick_frame(raw, code):
    if not isinstance(raw, dict):
        return pd.DataFrame()
    val = raw.get("Value", raw)
    if isinstance(val, dict) and code in val:
        val = val[code]
    if not isinstance(val, dict):
        return pd.DataFrame()
    cols = {k: v for k, v in val.items() if isinstance(v, (list, tuple)) and v}
    if not cols:
        return pd.DataFrame()
    n = max(len(v) for v in cols.values())
    df = pd.DataFrame({k: v for k, v in cols.items() if len(v) == n})
    df["code"] = code
    return df

def tick_supported(tq, code, d):
    tq.refresh(code, "tick", refresh_lock=_refresh_lock)
    try:
        return not tick_frame(tq.kline(code, "tick", -1, fmtdate(d), fmtdate(d)), code).empty
    except Exception:
        return False

def update_tick(tq, cal, code, d, log):
    path = TICK_DIR / code / f"{fmtdate(d)}.parquet"
    if path.exists():
        close_dt = datetime(d.year, d.month, d.day, *TICK_CLOSE_TIME)
        if datetime.fromtimestamp(path.stat().st_mtime) >= close_dt:
            log(f"    tick {fmtdate(d)}: 已收盘且文件完整，跳过")
            return
    df = tick_frame(tq.kline(code, "tick", -1, fmtdate(d), fmtdate(d)), code)
    if df.empty:
        log(f"    tick {fmtdate(d)}: 空数据")
        return
    if "Time" in df.columns:
        res = [cal.fix_tick_time(d, t) for t in df["Time"]]
        df["datetime"] = [r[0] for r in res]
        df["is_night"] = [r[1] for r in res]
    df["trade_date"] = fmtdate(d)
    df = df.drop_duplicates().reset_index(drop=True)
    atomic_write(df, path)
    log(f"    tick {fmtdate(d)}: 写入 {len(df)} 笔")

def update_ticks_range(tq, cal, code, count, start, end, log):
    current = cal.current_trade_day(datetime.now())
    end_d = date8(end) if end else current
    if start:
        start_d = date8(start)
    else:
        existing = []
        folder = TICK_DIR / code
        if folder.exists():
            for p in folder.glob("*.parquet"):
                try:
                    existing.append(date8(p.stem))
                except Exception:
                    pass
        valid = sorted(x for x in cal.days if x <= end_d)
        if existing:
            start_d = cal.prev(max(existing))
        else:
            start_d = valid[-count] if len(valid) >= count else (valid[0] if valid else end_d)
    d = start_d
    while d <= end_d:
        if cal.is_trade_day(d):
            update_tick(tq, cal, code, d, log)
        d += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser(description="通达信期货通 TQ 本地 HTTP 数据增量下载器")
    ap.add_argument("--count", type=int, default=DEFAULT_COUNT, help="首次抓取的 K 线数量（默认 60）")
    ap.add_argument("--start-date", default="", help="起始日期 YYYYMMDD")
    ap.add_argument("--end-date", default="", help="结束日期 YYYYMMDD")
    ap.add_argument("--only", default="", help="指定抓取周期（如 5m,15m,1d）")
    ap.add_argument("--no-tick", action="store_true", help="禁用分笔数据下载")
    ap.add_argument("--main-only", action="store_true", help="只下载主力合约")
    ap.add_argument("--secondary-only", action="store_true", help="只下载次主力合约")
    ap.add_argument("--no-secondary", action="store_true", help="禁用次主力探测")
    ap.add_argument("--no-financial", action="store_true", help="排除金融期货(股指/国债)")
    ap.add_argument("--secondary-by", choices=["oi", "volume"], default="oi", help="次主力排序依据")
    ap.add_argument("--secondary-min-ratio", type=float, default=0.0, help="次主力持仓量占比过滤阈值")
    ap.add_argument("--refresh-secondary", action="store_true", help="忽略今日缓存重新探测次主力")
    ap.add_argument("--no-refresh", action="store_true", help="不强制刷新客户端缓存")
    ap.add_argument("--rebuild", action="store_true", help="忽略本地 Parquet 重新生成")
    ap.add_argument("--workers", type=int, default=1, help="多合约并发数")
    ap.add_argument("--codes", default="", help="手动指定的合约代码（逗号分隔）")
    ap.add_argument("--url", default=TQ_URL, help="TQ HTTP 接口地址")
    args = ap.parse_args()

    if args.count < 1:
        ap.error("--count 必须大于 0")
    now = datetime.now()
    today = now.date()
    start = date8(args.start_date) if args.start_date else today - timedelta(days=365)
    end = date8(args.end_date) if args.end_date else today
    if start > end:
        ap.error("--start-date 不能晚于 --end-date")
    refresh = not args.no_refresh

    print("=" * 78)
    print("通达信期货通 TQ 本地数据下载器")
    print(f"接口={args.url} count={args.count}")
    tq = TQ(args.url)
    if not tq.ping():
        print("[ERROR] 通达信期货通未启动或 TQ 接口不可用，请启动并登录后重试。")
        return 2

    try:
        if args.codes:
            codes = [clean_code(x) for x in args.codes.split(",") if x.strip()]
            ref_codes = codes[:4]
            main_codes, secondary = codes, []
        else:
            main_codes, secondary, ref_codes, info = discover_contracts(
                tq, args.refresh_secondary, args.no_secondary or args.main_only,
                args.no_financial, args.secondary_by, args.secondary_min_ratio)
            atomic_write(pd.DataFrame(
                [{"code": c, "role": "main", "oi": info[c]["oi"], "vol": info[c]["vol"]} for c in main_codes] +
                [{"code": c, "role": "secondary", "oi": None, "vol": None} for c in secondary]),
                META_DIR / "contracts.parquet")
            codes = (main_codes if args.main_only else secondary if args.secondary_only
                     else sorted(set(main_codes + secondary)))
    except Exception as e:
        print(f"[ERROR] 合约发现失败：{e}")
        return 3
    codes = [c for c in codes if is_future(c)]
    if not codes:
        print("[ERROR] 最终有效合约数量为 0。")
        return 4

    lo = min(start, today) - timedelta(days=200)
    cal = build_calendar(tq, ref_codes, lo, today + timedelta(days=5), refresh)

    if args.only.strip().lower() == "tick":
        periods, want_tick = [], True
    elif args.only:
        periods = [x.strip().lower() for x in args.only.split(",") if x.strip()]
        bad = [x for x in periods if x not in PERIODS]
        if bad:
            ap.error(f"不支持的周期：{bad}，可选 {list(PERIODS)}")
        want_tick = False
    else:
        periods, want_tick = list(PERIODS), True
    if args.no_tick:
        want_tick = False

    do_tick = False
    if want_tick:
        do_tick = tick_supported(tq, codes[0], cal.current_trade_day(now))

    errors = []
    def work(item):
        i, code = item
        lines, errs = [], []
        log = lines.append
        for p in periods:
            try:
                update_kline(tq, cal, code, p, args.count, args.start_date, args.end_date,
                             refresh, args.rebuild, log)
            except Exception as e:
                log(f"    {p}: 失败 {e}")
                errs.append((code, p, str(e)))
        if do_tick:
            try:
                update_ticks_range(tq, cal, code, args.count, args.start_date, args.end_date, log)
            except Exception as e:
                log(f"    tick: 失败 {e}")
                errs.append((code, "tick", str(e)))
        with _print_lock:
            print(f"\n[{i}/{len(codes)}] {code}\n" + "\n".join(lines), flush=True)
        return errs

    try:
        with ThreadPoolExecutor(max(1, args.workers)) as ex:
            for errs in ex.map(work, list(enumerate(codes, 1))):
                errors.extend(errs)
    except KeyboardInterrupt:
        print("用户中断。")
        return 130

    print("\n" + "=" * 78)
    print(f"完成：合约={len(codes)} 失败项={len(errors)}")
    return 0 if not errors else 5

if __name__ == "__main__":
    sys.exit(main())