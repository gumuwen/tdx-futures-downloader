# TDX Futures Data Downloader (通达信期货本地 HTTP 数据下载器)

这是一个基于 Python 编写的高性能通达信（TQ）本地 HTTP 增量数据下载器。它能自动识别国内商品与金融期货的主力与次主力合约，自动修正通达信夜盘伪时间戳，并将 K 线及分笔数据增量存储为高效的 **Apache Parquet** 格式。

## ✨ 核心特性

- 🔄 **自动合约识别**：自动探测各品种的主力合约与次主力合约，支持按持仓量/成交量智能排序。
- ⏰ **夜盘时间戳修复**：自动识别通达信夜盘与凌晨的伪交易日时间戳，精准还原为真实自然时间。
- 📦 **高性能 Parquet 存储**：使用 ZSTD 压缩的 Parquet 文件格式存储，大幅节省存储空间并提升读取速度。
- ⚡ **增量更新与断点续传**：基于本地数据末尾时间自动计算抓取范围，防止重复覆盖。
- 🚀 **多线程并发支持**：支持多合约并发下载调度。

## 🛠️ 环境要求

- Python 3.9+
- 本地正在运行并处于登录状态的**通达信期货通**客户端（开启 HTTP 监听端口 `17709`）

## 📦 安装与使用

1. **克隆本仓库**：
   ```bash
   git clone [https://github.com/gumuwen/tdx-futures-downloader.git](https://github.com/gumuwen/tdx-futures-downloader.git)
   cd tdx-futures-downloader