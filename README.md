# create-table-project

根据 Excel 配置自动生成 Hive / ClickHouse 建表 SQL 的 Python 工具。

**版本**: 0.1.0
**Python 要求**: >= 3.13

## 快速开始

### 安装依赖（使用 uv，推荐）

```bash
cd create-table-project
uv sync
```

说明：

- 这会根据 `pyproject.toml` 和 `uv.lock` 创建/复用本地虚拟环境（默认在 `.venv/`），并安装所需依赖。
- 如果尚未安装 `uv`，可参考官方安装文档（`https://docs.astral.sh/uv/`），或使用系统包管理器安装。

### 运行程序

```bash
cd create-table-project
uv run python app/main.py
```

### 调试模式

```bash
uv run python app/main.py --debug
```

### 安装为命令行工具使用

项目提供命令行入口：`create-table=app.main:main`（定义在 `pyproject.toml` 的 `[project.scripts]` 中）。

```bash
cd create-table-project

# 确保依赖已安装
uv sync

# 全局安装工具
uv tool install . --editable

# 若有报错：error: Querying Python at `/Users/sunpeng9/.local/share/uv/tools/create-table-project/bin/python3` failed with exit status signal: 9 (SIGKILL)  
# 则执行： sudo codesign -s - -f /Users/sunpeng9/.local/share/uv/tools/create-table-project/bin/python3

# 使用脚本入口运行（通过 uv 调用虚拟环境中的命令）
uv run create-table

# 调试模式
uv run create-table --debug
```

### 常用 uv 命令速查

```bash
# 安装依赖
uv add <package>

# 移除依赖
uv remove <package>

# 运行程序
uv run <script.py>

# 同步环境
uv sync

# 全局安装工具
uv tool install <tool>
```

说明：

- Excel 输入路径 `EXCEL_PATH` 与输出根目录 `OUTPUT_BASE_DIR` 在 `app/config/settings.py` 中配置
- 输出的 SQL 文件按运行日期自动归档到 `create-table-output/YYYYMMDD/` 目录下
- 若提示 `create-table: command not found`，请优先使用 `uv run create-table`；或者确认你已经在当前环境中安装了该项目（如使用传统 conda/venv，可在对应环境下执行 `pip install -e .`）

## Excel 文件格式

### tables sheet（必需列）

- `表名`
- `产品线`
- `入仓方式`（天表/小时表，自动映射为 day/hour）
- `表注释信息`
- `hive表名`（可选）：
  - 有值：直接使用该值作为 Hive 表名
  - 为空：使用拼接逻辑 `ods_ad_产品线_表名_入仓方式`
- `数仓分层`（可选）：
  - 用于生成 LOCATION：`viewfs://c9/dw/{数仓分层}/{hive表名}`
  - 例如填 `sds` 则路径为 `viewfs://c9/dw/sds/xxx`
  - 为空时默认使用 `ods`
- `建表格式`（可选）：
  - 取值：`orc` / `rcfile` / `text`
  - 为空或非法值：默认使用 `rcfile`
  - 为 `rcfile`：使用 RCFILE 格式（无 ORC 属性）
  - 为 `orc`：使用 ORC 格式，带 `TBLPROPERTIES('orc.compress'='SNAPPY')`
  - 为 `text`：使用 TEXTFILE 格式（无 ORC 属性）
- `目标表类型`（可选）：
  - 取值：`hive`（默认）/ `clickhouse`
- `操作类型`（可选）：
  - 取值：`新建表`（默认）/ `修改表`
  - 当目标表类型为 `clickhouse` 且操作类型为 `修改表` 时，生成 ALTER ADD COLUMN 语句

### fields sheet（统一格式，5 列 + 可选列）

- `表名`（必需）
- `字段名`
- `字段数据类型`
- `字段注释`（自动规范化：换行符等特殊字符会统一转换为单个空格）
- `建表语句`
- `操作类型`（可选）：`新建表` / `修改表`，用于 ClickHouse 修改表场景

**处理规则（逐行检测）**：
- 有"建表语句"且不为空 → 解析建表语句提取字段信息
- 有"字段名"、"字段数据类型"且不为空 → 直接使用这些字段信息（字段注释允许为空；为空时生成 SQL 不输出 `COMMENT` 子句）
- 同一 sheet 中可以混合使用两种方式
- 字段注释会进行规范化：换行符、制表符等空白字符会被合并为单个空格

## 类型转换规则

### MySQL → Hive

| MySQL 类型 | Hive 类型 |
|-----------|----------|
| BIGINT, INT, INTEGER, TINYINT | BIGINT |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | STRING |
| 其他类型 | STRING |

### MySQL → ClickHouse

| MySQL 类型 | ClickHouse 类型 |
|-----------|----------------|
| BIGINT, INT, INTEGER, TINYINT | Int64 |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | String |
| 其他类型 | String |

## 输出说明

- **输出根目录**：`app/config/settings.py` 中的 `OUTPUT_BASE_DIR`（默认为项目同级的 `create-table-output/` 目录）
- **日期归档**：每次运行时自动在输出根目录下创建当日日期目录（格式 `YYYYMMDD`），SQL 文件写入该目录
- **Hive 表名规则**：`ods_ad_产品线_表名_入仓方式`（入仓方式标准化为 day/hour）
- **文件命名**：
  - Hive：`{hive表名}.sql`
  - ClickHouse 新建表：`{hive表名}_ck.sql`
  - ClickHouse 修改表：`{hive表名}_ck_alter.sql`

输出示例目录结构：

```
create-table-output/
└── 20260318/
    ├── ods_ad_pl_t1_day.sql
    ├── ods_ad_pl_t2_day_ck.sql
    └── ods_ad_pl_t3_day_ck_alter.sql
```

生成的 Hive SQL 文件包含：
- 字段定义（字段名、Hive 数据类型、字段注释）
- 表注释
- 固定配置：分区、存储格式、压缩方式、存储位置等

生成的 ClickHouse SQL 文件包含：
- 本地表（ReplicatedMergeTree）和分布式表
- 自动追加 `dt` 分区字段
- 自增主键自动识别，用于 ORDER BY

## 调试工具

### 单元测试（unittest）

```bash
cd create-table-project

# 运行全部测试
uv run python -m unittest discover -s tests -p "test_*.py" -v

# 运行单个测试文件（示例）
uv run python -m unittest tests.test_main_flow -v
uv run python -m unittest tests.test_mysql_parser -v
```

## 测试覆盖概览

| 测试文件 | 覆盖内容 |
|----------|----------|
| `test_main_flow.py` | 主流程关键分支（hive/clickhouse 分流、输出路径/后缀、输出目录创建、存在旧文件先删除、fields 匹配失败跳过） |
| `test_mysql_parser.py` | MySQL 建表解析状态机（括号/引号/逗号场景、跳过约束行） |
| `test_hive_create_sql.py` | Hive 建表 SQL 拼装分支（分区、存储格式、LOCATION、表注释后缀） |
| `test_clickhouse_create_sql.py` | ClickHouse 建表 SQL 结构（本地表/分布式表、ALTER、ORDER BY、自增主键识别） |
| `test_excel_reader_load_excel.py` | `load_excel()` 入口校验与异常（文件不存在、fields 缺列、最小成功路径） |
| `test_empty_field_comment.py` | 字段注释为空时仍保留字段，且 SQL 不输出 `COMMENT` |
| `test_fields_sheet_processing.py` | fields sheet 混合格式（建表语句解析 + 直填字段）处理逻辑 |
| `test_table_builder.py` | 表名构建、入仓方式标准化 |
| `test_table_matching.py` | 表名匹配逻辑（优先使用 hive 表名匹配 fields） |
| `test_type_converter.py` | MySQL → Hive/ClickHouse 类型转换（整数、浮点、小数、字符串） |

## 常见问题

### Excel 文件不存在

检查 `app/config/settings.py` 中的 `EXCEL_PATH` 配置是否正确。默认路径为项目同级目录下的 `create-table-output/create_table_info.xlsx`。

### 表名匹配失败

- 检查 `tables` sheet 中的表名是否与 `fields` sheet 中的表名完全一致
- 如果 tables 页有 `hive表名`，fields 页应使用 `hive表名` 作为表名
- 确保表名没有多余的空格

### 字段解析失败

- 检查 MySQL 建表语句格式是否正确
- 使用 `--debug` 模式查看详细错误信息

## 项目特点

- 统一的 Excel 格式（5 列混合格式）
- 自动解析 MySQL 建表语句（状态机实现，正确处理括号嵌套和引号）
- 自动类型转换（MySQL → Hive / ClickHouse）
- 支持 Hive 和 ClickHouse 两种目标表类型
- 支持 ClickHouse 新建表和修改表（ALTER ADD COLUMN）
- 支持直接指定 Hive 表名
- 灵活的字段定义方式（建表语句解析 + 直接字段信息）
- 字段注释自动规范化（换行符等特殊字符统一转换为单个空格）
- 输出按运行日期自动归档
- 完善的日志系统

## 技术栈

- Python >= 3.13（见 `pyproject.toml` 中的 `requires-python`）
- pandas
- openpyxl
- tabulate

## 项目结构

```
create-table-project/
├── app/
│   ├── __init__.py
│   ├── main.py              # 主程序入口
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py      # 配置（路径、日志格式）
│   └── utils/
│       ├── __init__.py
│       ├── excel_reader.py   # Excel 读取与解析
│       ├── mysql_parser.py   # MySQL 建表语句解析（状态机）
│       ├── table_builder.py  # SQL 构建（Hive/ClickHouse）
│       ├── type_converter.py # 类型映射（MySQL → Hive/ClickHouse）
│       └── logger.py         # 日志配置
├── tests/                    # 单元测试（10 个测试文件，34 个用例）
├── logs/                     # 日志目录
├── pyproject.toml            # 项目与依赖配置（uv / pip 共用）
└── uv.lock                   # uv 生成的锁文件（精确记录依赖版本）
```

## 配置说明

配置文件：`app/config/settings.py`

| 配置项 | 说明 |
|--------|------|
| `PROJECT_ROOT` | 项目根目录（自动计算） |
| `EXCEL_PATH` | Excel 输入文件路径（默认：项目同级 `create-table-output/create_table_info.xlsx`） |
| `OUTPUT_BASE_DIR` | 输出根目录（默认：项目同级 `create-table-output/`，运行时自动追加日期子目录） |
| `LOG_DIR` | 日志目录（默认：项目下 `logs/`） |

## 许可证

MIT License
