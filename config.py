# -*- coding: utf-8 -*-
"""全局配置文件。"""
import re
from pathlib import Path

# TQ HTTP 接口地址与默认保存目录
TQ_URL = "http://127.0.0.1:17709/"
BASE_DIR = Path("./data")

KLINE_DIR = BASE_DIR / "kline"
TICK_DIR = BASE_DIR / "tick"
META_DIR = BASE_DIR / "meta"

# 基础抓取参数
DEFAULT_COUNT = 60
TIMEOUT = 20
RETRIES = 3
REFRESH_TIMEOUT = 180
SECONDARY_MONTHS_AHEAD = 15
PROBE_WORKERS = 6
STALE_DAYS = 7
TICK_CLOSE_TIME = (15, 30)
MAX_ATTEMPTS = 2
RETRY_WAIT = 1.0

# 过滤黑名单与金融期货列表
PRODUCT_BLACKLIST = set()
CONTRACT_BLACKLIST = set()
FINANCIAL_PRODUCTS = {"IF", "IH", "IC", "IM", "T", "TF", "TS", "TL"}

# 周期映射: 名称 -> (TQ周期名, 目录名)
PERIODS = {
    "1d": ("1d", "DAY"),
    "60m": ("1h", "60MIN"),
    "30m": ("30m", "30MIN"),
    "15m": ("15m", "15MIN"),
    "5m": ("5m", "5MIN"),
    "1m": ("1m", "1MIN"),
}

# 刷新缓存时用到的基础周期
REFRESH_BASE = {
    "1d": "1d",
    "1m": "1m",
    "5m": "5m",
    "15m": "5m",
    "30m": "5m",
    "60m": "5m",
}

PRODUCT_RE = re.compile(r"^([A-Z]{1,4})(\d{3,4})$")
VALID_SUFFIX = {".SHF", ".DCE", ".CZC", ".CFF", ".INE", ".GFEX"}

# 自动创建必要的数据保存目录
for _path in (KLINE_DIR, TICK_DIR, META_DIR):
    _path.mkdir(parents=True, exist_ok=True)