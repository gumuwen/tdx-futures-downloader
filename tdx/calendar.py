# -*- coding: utf-8 -*-
"""交易日历与伪时间戳转化处理。"""
import bisect
import re
from datetime import datetime, timedelta
import pandas as pd
from tdx.api import clean_code

def kline_frame(raw, code, daily=False):
    """解析 TQ 返回的 K 线数据为 pandas DataFrame。"""
    if not isinstance(raw, dict):
        return pd.DataFrame()
    val = raw.get("Value", raw)
    if isinstance(val, dict) and code in val:
        val = val[code]
    if not isinstance(val, dict):
        return pd.DataFrame()
    datev = val.get("Date")
    if not isinstance(datev, (list, tuple)) or not datev:
        return pd.DataFrame()
    n = len(datev)
    ds = [re.sub(r"\D", "", str(x))[:8] for x in datev]
    
    def hms(values):
        nums = [int(re.sub(r"\D", "", str(v)) or 0) for v in values]
        hhmm = max(nums) < 10000 if nums else True
        return [f"{x:04d}00" if hhmm else f"{x:06d}" for x in nums]

    timev = val.get("Time")
    ts = ["000000"] * n if (daily or not isinstance(timev, (list, tuple)) or len(timev) != n) else hms(timev)
    df = pd.DataFrame({
        "datetime": pd.to_datetime([a + b for a, b in zip(ds, ts)], format="%Y%m%d%H%M%S", errors="coerce"),
        "code": code})
    for col, key in (("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close"),
                     ("volume", "Volume"), ("amount", "Amount"), ("open_interest", "VolInStock")):
        v = val.get(key)
        df[col] = (pd.to_numeric(pd.Series(v), errors="coerce")
                   if isinstance(v, (list, tuple)) and len(v) == n else float("nan"))
    return (df.dropna(subset=["datetime"]).sort_values("datetime")
            .drop_duplicates("datetime", keep="last").reset_index(drop=True))

class Calendar:
    """
    TDX 伪时间戳规则处理：
    夜盘(21:00-23:59)与凌晨(00:00-02:59)标为“下一个交易日”。
    本类负责将其还原为真实自然时间。
    """
    def __init__(self, days, from_kline):
        self.list = sorted(set(days))
        self.days = set(self.list)
        self.from_kline = from_kline

    def prev(self, d):
        if self.list and self.list[0] < d <= self.list[-1]:
            return self.list[bisect.bisect_left(self.list, d) - 1]
        x = d - timedelta(days=1)
        while x.weekday() >= 5:
            x -= timedelta(days=1)
        return x

    def next(self, d):
        if self.list and self.list[0] <= d < self.list[-1]:
            return self.list[bisect.bisect_right(self.list, d)]
        x = d + timedelta(days=1)
        while x.weekday() >= 5:
            x += timedelta(days=1)
        return x

    def is_trade_day(self, d):
        if d in self.days:
            return True
        return bool(self.list) and d > self.list[-1] and d.weekday() < 5

    def current_trade_day(self, now):
        d = now.date()
        if now.hour >= 21:
            return self.next(d)
        if now.hour < 3:
            return self.next(d - timedelta(days=1))
        return d if self.is_trade_day(d) else self.prev(d)

    def to_real(self, pseudo_d, hh):
        if hh >= 21:
            return self.prev(pseudo_d), True
        if hh < 3:
            return self.prev(pseudo_d) + timedelta(days=1), True
        return pseudo_d, False

    def fix_kline(self, df):
        out = df.copy()
        ts = out["datetime"]
        out["tdx_time"] = ts
        out["trade_date"] = ts.dt.strftime("%Y%m%d")
        hh = ts.dt.hour
        night = (hh >= 21) | (hh < 3)
        out["is_night"] = night
        if night.any():
            day0 = ts.dt.normalize()
            cache = {}
            def base(d):
                if d not in cache:
                    cache[d] = pd.Timestamp(self.prev(d.date()))
                return cache[d]
            b = day0[night].map(base) + pd.to_timedelta((hh[night] < 3).astype(int), unit="D")
            out.loc[night, "datetime"] = b + (ts[night] - day0[night])
        return out

    def fix_tick_time(self, pseudo_d, t):
        s = re.sub(r"\D", "", str(t))
        s = (s[:-3] if len(s) > 6 else s).zfill(6)[:6]
        hh, mm, ss = int(s[:2]), int(s[2:4]), int(s[4:6])
        nat, night = self.to_real(pseudo_d, hh)
        return datetime(nat.year, nat.month, nat.day, hh, mm, ss), night

def build_calendar(tq, refs, lo, hi, refresh):
    """基于代表性主力合约生成交易日历。"""
    days = set()
    for c in refs:
        if refresh:
            tq.refresh(c, "1d")
        try:
            df = kline_frame(tq.kline(c, "1d", -1, lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d")), c, daily=True)
            days.update(df["datetime"].dt.date)
        except Exception as e:
            print(f"[WARN] 交易日历参考合约 {c} 读取失败：{e}")
    if len(days) >= 20:
        return Calendar(days, True)
    d, fallback = lo, []
    while d <= hi:
        if d.weekday() < 5:
            fallback.append(d)
        d += timedelta(days=1)
    return Calendar(fallback, False)