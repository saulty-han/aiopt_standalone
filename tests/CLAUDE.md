# 端到端测试

## 依赖环境

- **元数据 MySQL**（普通 MySQL 即可）— 存储任务记录、规则、workload 等，连接信息在 `etc/aiopt_conf.toml` 的 `[meta_server]`
- **训练环境 MySQL**（TXSQL 内核）— SQL 优化的目标数据库，需支持 SPM 或 Statement Outline，且 AWR 功能已开启（`txsql_awr_enabled_level != 0`），AWR 表中需有 TPC-H 查询统计数据
- **Python 3.10+**
- **Docker** — 数据准备阶段需要，用于构建和运行 BenchBase

两个 MySQL 实例的连接信息分别配置在 `etc/aiopt_conf.toml` 和 `tests/profiles.json` 中。

## 运行

```bash
python tests/test_e2e.py --profile cdb20260331-spm
```

## 环境搭建

### 1. 依赖安装

```bash
pip install -r requirements.txt -r mcts/requirements.txt
```

### 2. 配置文件

```bash
cp etc/aiopt_conf.toml.tpl etc/aiopt_conf.toml      # 填写 [meta_server]
cp tests/profiles.json.example tests/profiles.json    # 填写连接信息
```

### 3. 初始化元数据库

```bash
for f in schema/aiopt.sql schema/perf_metrics.sql schema/perf_result.sql schema/mcts_result.sql; do
  mysql -h <ip> -P <port> -u <user> -p'<pwd>' <database> < "$f"
done
```

### 4. 检查训练环境 AWR 功能

用 tencentroot 用户连接训练环境（普通用户看不到这些特权变量）：

```sql
-- AWR 功能必须开启（不为 0）
SHOW GLOBAL VARIABLES LIKE 'txsql_awr_enabled_level';

-- 检查是否已有 workload 数据
SELECT COUNT(*) FROM information_schema.txsql_awr_sql_by_digest_planid WHERE db = 'tpch';
```

### 5. 灌入 TPC-H 数据并产生 AWR 统计

如果上一步数据为空，需要先灌数据再跑负载。

先修改 `tests/benchbase/tpch_config.xml` 中的 `<url>` 连接串，将地址、端口改为训练环境的实际值。BenchBase 不会自动建库，需要手动创建：

```bash
# 构建 BenchBase 镜像（一次性）
docker build -t benchbase-mysql tests/benchbase/

# 手动建库
mysql -h <ip> -P <port> -u <user> -e "CREATE DATABASE IF NOT EXISTS tpch"

# 建表 + 灌数据
docker run --rm --network host \
  -v $(pwd)/tests/benchbase/tpch_config.xml:/benchbase/config/tpch_config.xml \
  benchbase-mysql -b tpch -c config/tpch_config.xml --create=true --load=true --execute=false

# 跑查询负载
docker run --rm --network host \
  -v $(pwd)/tests/benchbase/tpch_config.xml:/benchbase/config/tpch_config.xml \
  benchbase-mysql -b tpch -c config/tpch_config.xml --create=false --load=false --execute=true

# 刷新 AWR 日志（使负载统计立即可见）
mysql -h <ip> -P <port> -u tencentroot -e "FLUSH TXSQL_AWR LOGS"

# 验证
mysql -h <ip> -P <port> -u tencentroot -e \
  "SELECT COUNT(*) FROM information_schema.txsql_awr_sql_by_digest_planid WHERE db='tpch'"
```

## 排查

- **配置文件缺失**（`FileNotFoundError`）→ 回到搭建步骤 2
- **数据库连不上**（`Can't connect` / `Access denied`）→ 检查 `profiles.json` 和 `aiopt_conf.toml` 中的连接信息
- **元数据表不存在**（`Table doesn't exist`）→ 回到搭建步骤 3
- **规则数为 0**（`未产生任何规则`）→ 回到搭建步骤 4、5，确认 AWR 有数据
- **AWR 有数据但某些查询统计不到** → 检查 `SHOW GLOBAL VARIABLES LIKE 'txsql_awr_lightweight_query_skip_enabled'`，如果为 ON，轻量查询会被 AWR 跳过
