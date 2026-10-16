# -*- coding: utf-8 -*-
"""期货合约计算与发现工具。"""
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import pandas as pd
from config import (
    PRODUCT_RE, VALID_SUFFIX, PRODUCT_BLACKLIST, CONTRACT_BLACKLIST,
    FINANCIAL_PRODUCTS, SECONDARY_MONTHS_AHEAD, PROBE_WORKERS, META_DIR, STALE_DAYS
)
from tdx.api import clean_code
from tdx.calendar import kline_frame

def split_code(code):
    body, _, ex = clean_code(code).partition(".")
    m = PRODUCT_RE.match(body)
    return (m.group(1), m.group(2), "." + ex) if m else ("", "", "." + ex)

def product_of(code):
    return split_code(code)[0]

def is_future(code):
    code = clean_code(code)
    if "." not in code or code in CONTRACT_BLACKLIST:
        return False
    prod, _, suffix = split_code(code)
    return bool(prod and suffix in VALID_SUFFIX and prod not in PRODUCT_BLACKLIST)

def candidate_codes(main_code, months=SECONDARY_MONTHS_AHEAD):
    prod, _, suffix = split_code(main_code)
    y, m = date.today().year, date.today().month
    out = []
    for _ in range(months + 1):
        out.append(f"{prod}{y % 100:02d}{m:02d}{suffix}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return [c for c in out if c != clean_code(main_code)]

def probe(tq, code):
    try:
        df = kline_frame(tq.kline(code, "1d", 3), code, daily=True)
    except Exception:
        return None
    if df.empty:
        return None
    r = df.iloc[-1]
    
    def num0(x):
        try:
            v = float(str(x).replace(",", ""))
            return 0.0 if v != v else v
        except (ValueError, TypeError):
            return 0.0

    return {"date": r["datetime"].date(), "oi": num0(r["open_interest"]), "vol": num0(r["volume"])}

def probe_many(tq, codes):
    codes = sorted(set(codes))
    with ThreadPoolExecutor(PROBE_WORKERS) as ex:
        return dict(zip(codes, ex.map(lambda c: probe(tq, c), codes)))

def discover_secondary(tq, main, ref_date, refresh, by, main_oi, min_ratio):
    cache = META_DIR / "secondary_cache.json"
    key = {"date": date.today().strftime("%Y%m%d"), "main": main, "by": by,
           "ref": ref_date.strftime("%Y%m%d"), "ratio": min_ratio, "v": 5}
    if not refresh and cache.exists():
        try:
            c = json.loads(cache.read_text(encoding="utf-8"))
            if c.get("key") == key:
                print(f"[次主力] 使用今日缓存，共 {len(c['secondary'])} 个")
                return c["secondary"]
        except Exception:
            pass
    jobs = [(m, c) for m in main for c in candidate_codes(m)]
    print(f"[次主力] 探测候选合约 {len(jobs)} 个（并发 {PROBE_WORKERS}）...")
    res = probe_many(tq, [c for _, c in jobs])
    rows = []
    for m, c in jobs:
        r = res.get(c)
        if r and r["date"] >= ref_date and (r["oi"] > 0 or r["vol"] > 0):
            mo = main_oi.get(m, 0)
            rows.append({"product": product_of(m), "main": m, "code": c, "oi": r["oi"], "vol": r["vol"],
                         "oi_ratio": (r["oi"] / mo) if mo > 0 else None})
    secondary = []
    if rows:
        df = pd.DataFrame(rows)
        sk = ["oi", "vol"] if by == "oi" else ["vol", "oi"]
        df = df.sort_values(["product"] + sk, ascending=[True, False, False])
        df["rank"] = df.groupby("product").cumcount() + 1
        df.to_csv(META_DIR / "secondary_debug.csv", index=False, encoding="utf-8-sig")
        top = df[df["rank"] == 1]
        if min_ratio > 0:
            weak = top[top["oi_ratio"].fillna(0) < min_ratio]
            if len(weak):
                print(f"[次主力] 持仓量不足主力 {min_ratio:.0%} 而剔除 {len(weak)} 个：{sorted(weak['code'])}")
            top = top[top["oi_ratio"].fillna(0) >= min_ratio]
        secondary = sorted(top["code"])
    cache.write_text(json.dumps({"key": key, "secondary": secondary}, ensure_ascii=False, indent=1),
                     encoding="utf-8")
    print(f"[次主力] 识别 {len(secondary)} 个，示例={secondary[:8]}")
    return secondary

def discover_contracts(tq, refresh_secondary, no_secondary, no_financial, by, min_ratio):
    mains = tq.main_list()
    if no_financial:
        mains = [c for c in mains if product_of(c) not in FINANCIAL_PRODUCTS]
    if not mains:
        raise RuntimeError("主力合约列表为空（或已全部被黑名单剔除）。")
    print(f"[主力] 候选 {len(mains)} 个，探测最新日K...")
    pm = probe_many(tq, mains)
    ok = {c: r for c, r in pm.items() if r}
    if not ok:
        raise RuntimeError("所有主力合约都读不到日K，请确认客户端行情已连接。")
    ref_date = max(r["date"] for r in ok.values())
    stale = [c for c in mains if c not in ok or ok[c]["date"] < ref_date - timedelta(days=STALE_DAYS)]
    if stale:
        print(f"[主力] 剔除不活跃/无数据合约 {len(stale)} 个：{stale}")
    main = sorted(c for c in mains if c not in stale)
    main_oi = {c: ok[c]["oi"] for c in main}
    secondary = ([] if no_secondary else
                 discover_secondary(tq, main, ref_date, refresh_secondary, by, main_oi, min_ratio))
    secondary = [c for c in secondary if is_future(c) and c not in main]
    ref_codes = sorted(main, key=lambda c: -ok[c]["oi"])[:5]
    print(f"[合约结果] 主力 {len(main)}，次主力 {len(secondary)}，最新交易日(日K)={ref_date}")
    return main, secondary, ref_codes, {c: ok[c] for c in main}