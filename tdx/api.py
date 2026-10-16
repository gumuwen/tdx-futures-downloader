# -*- coding: utf-8 -*-
"""TQ HTTP 客户端实现。"""
import threading
import time
import requests
from config import TQ_URL, TIMEOUT, RETRIES

class TQError(RuntimeError):
    pass

def clean_code(x):
    return str(x or "").strip().upper()

def unwrap(x):
    for _ in range(5):
        if not isinstance(x, dict):
            break
        if str(x.get("ErrorId", "0")) not in ("0", "", "None"):
            raise TQError(f"ErrorId={x.get('ErrorId')}: {x.get('Error') or x.get('ErrorMsg') or x}")
        if "result" in x:
            x = x["result"]
        elif "Value" in x and len(x) <= 5:
            x = x["Value"]
        else:
            break
    return x

def extract_codes(obj):
    out = []
    def walk(x):
        if isinstance(x, str):
            s = clean_code(x)
            import re
            if re.match(r"^[A-Z]{1,5}\d{3,4}\.[A-Z]+$", s):
                out.append(s)
        elif isinstance(x, list):
            for y in x:
                walk(y)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)
    walk(obj)
    return sorted(set(out))

class TQ:
    def __init__(self, url=TQ_URL):
        self.url = url
        self._local = threading.local()
        self._rid = 0
        self._lock = threading.Lock()

    def _session(self):
        if not hasattr(self._local, "s"):
            self._local.s = requests.Session()
        return self._local.s

    def call(self, method, params=None, retries=RETRIES, timeout=TIMEOUT):
        with self._lock:
            self._rid += 1
            rid = self._rid
        payload = {"id": rid, "method": method, "params": params or {}}
        err = None
        for i in range(retries):
            try:
                r = self._session().post(self.url, json=payload, timeout=timeout)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    raise TQError(str(data["error"]))
                return unwrap(data)
            except TQError as e:
                raise TQError(f"{method}: {e}") from None
            except Exception as e:
                err = e
                if i + 1 < retries:
                    time.sleep(i + 1)
        raise RuntimeError(f"{method} 调用失败：{err}")

    def ping(self):
        """判断通达信期货通 TQ 是否已启动。"""
        try:
            r = self._session().post(self.url, json={
                "id": 0, "method": "get_stock_list",
                "params": {"market": 92, "list_type": 0}}, timeout=3)
            if r.status_code != 200:
                print(f"[TQ诊断] HTTP状态={r.status_code}")
                return False
            res = r.json().get("result") or {}
            ok = str(res.get("ErrorId", "0")) in ("0", "", "None")
            print(f"[TQ诊断] 已连接 HTTP=200 ErrorId={res.get('ErrorId', '0')}")
            return ok
        except requests.exceptions.ConnectionError:
            print("[TQ诊断] 无法连接本地接口：期货通未启动，或端口不是 17709。")
            return False
        except Exception as e:
            print(f"[TQ诊断] 连接失败：{e}")
            return False

    def main_list(self):
        from tdx.contract import is_future
        raw = self.call("get_stock_list", {"market": 92, "list_type": 0})
        return [c for c in extract_codes(raw) if is_future(c)]

    def refresh(self, code, period, refresh_lock=None, refresh_timeout=180):
        def _exec():
            try:
                r = self.call("refresh_kline", {"stock_list": [code], "period": period},
                              retries=2, timeout=refresh_timeout)
                return True, (r.get("Msg", "") if isinstance(r, dict) else str(r))
            except Exception as e:
                return False, str(e)

        if refresh_lock:
            with refresh_lock:
                return _exec()
        return _exec()

    def kline(self, code, period, count, start="", end=""):
        return self.call("get_market_data", {
            "field_list": [], "stock_list": [code], "period": period,
            "start_time": start, "end_time": end, "count": count,
            "dividend_type": "none", "fill_data": True})