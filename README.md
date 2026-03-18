# create-table-project

根据 Excel 配置自动生成 Hive 建表语句的 Python 工具。

**版本**: 1.0.0  
**Python 要求**: >= 3.7

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

### 常用uv命令速查

```bash
# 初始化项目
uv init

# 安装依赖
uv add \<package\>

# 移除依赖
uv remove \<package\>

# 运行程序
uv run \<script.py\>

# 同步环境
uv sync

# 全局安装工具
uv tool install \<tool\>

```

说明：

- Excel 输入路径 `EXCEL_PATH` 与输出目录 `OUTPUT_DIR` 仍在 `app/config/settings.py` 中配置
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

### fields sheet（统一格式，5列）

- `表名`（必需）
- `字段名`
- `字段数据类型`
- `字段注释`（自动规范化：换行符等特殊字符会统一转换为单个空格）
- `建表语句`

**处理规则（逐行检测）**：
- 有"建表语句"且不为空 → 解析建表语句提取字段信息
- 有"字段名"、"字段数据类型"且不为空 → 直接使用这些字段信息（字段注释允许为空；为空时生成 SQL 不输出 `COMMENT` 子句）
- 同一 sheet 中可以混合使用两种方式
- 字段注释会进行规范化：换行符、制表符等空白字符会被合并为单个空格

## 类型转换规则

| MySQL 类型 | Hive 类型 |
|-----------|----------|
| BIGINT, INT, INTEGER, TINYINT | BIGINT |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | STRING |
| 其他类型 | STRING |

## 输出说明

- **输出目录**：在 `app/config/settings.py` 中配置 `OUTPUT_DIR`
- **文件格式**：每个表生成一个 `.sql` 文件，文件名为 `{hive表名}.sql`
- **Hive 表名规则**：`ods_ad_产品线_表名_入仓方式`（入仓方式标准化为 day/hour）

生成的 SQL 文件包含：
- 字段定义（字段名、Hive 数据类型、字段注释）
- 表注释
- 固定配置：分区、存储格式、压缩方式、存储位置等

## 调试工具

### 单元测试（unittest）

```bash
cd create-table-project

# 运行全部测试
python -m unittest discover -s tests -p "test_*.py" -v
# 或（使用 uv）：
# uv run python -m unittest discover -s tests -p "test_*.py" -v

# 运行单个测试文件（示例）
python -m unittest tests.test_main_flow -v
python -m unittest tests.test_mysql_parser -v
# 或（使用 uv）：
# uv run python -m unittest tests.test_main_flow -v
# uv run python -m unittest tests.test_mysql_parser -v
```

## 测试覆盖概览

- `tests/test_main_flow.py`：覆盖 `app/main.py` 主流程关键分支（hive/clickhouse 分流、输出路径/后缀、输出目录创建、存在旧文件先删除、fields 匹配失败跳过）
- `tests/test_mysql_parser.py`：覆盖 MySQL 建表解析状态机（括号/引号/逗号场景、跳过约束行）
- `tests/test_hive_create_sql.py`：覆盖 Hive 建表 SQL 拼装分支（分区、存储格式、LOCATION、表注释后缀）
- `tests/test_clickhouse_create_sql.py`：覆盖 ClickHouse 建表 SQL 结构（本地表/分布式表、追加 `dt` 字段、类型片段）
- `tests/test_excel_reader_load_excel.py`：覆盖 `load_excel()` 入口校验与异常（文件不存在、fields 缺列、最小成功路径，均用 mock）
- `tests/test_empty_field_comment.py`：覆盖字段注释为空时仍保留字段，且 SQL 不输出 `COMMENT`
- `tests/test_fields_sheet_processing.py`：覆盖 fields sheet 混合格式（建表语句解析 + 直填字段）处理逻辑

## 常见问题

### Excel 文件不存在

检查 `app/config/settings.py` 中的 `EXCEL_PATH` 配置是否正确。

### 表名匹配失败

- 检查 `tables` sheet 中的表名是否与 `fields` sheet 中的表名完全一致
- 如果 tables 页有 `hive表名`，fields 页应使用 `hive表名` 作为表名
- 确保表名没有多余的空格

### 字段解析失败

- 检查 MySQL 建表语句格式是否正确
- 使用 `--debug` 模式查看详细错误信息

## 项目特点

- ✅ 统一的 Excel 格式（5列混合格式）
- ✅ 自动解析 MySQL 建表语句
- ✅ 自动类型转换（MySQL → Hive）
- ✅ 支持直接指定 Hive 表名
- ✅ 灵活的字段定义方式（建表语句解析 + 直接字段信息）
- ✅ 字段注释自动规范化（换行符等特殊字符统一转换为单个空格）
- ✅ 完善的日志系统

## 技术栈

- Python >= 3.13（见 `pyproject.toml` 中的 `requires-python`）
- pandas
- openpyxl
- tabulate

## 项目结构

```
create-table-project/
├── app/
│   ├── config/          # 配置模块
│   ├── utils/           # 工具函数模块
│   └── main.py          # 主程序入口
├── tests/               # 测试目录
├── logs/                # 日志目录
├── pyproject.toml       # 项目与依赖配置（uv / pip 共用）
└── uv.lock              # uv 生成的锁文件（精确记录依赖版本）
```

## 配置说明

配置文件：`app/config/settings.py`

- `EXCEL_PATH`: Excel 文件路径
- `OUTPUT_DIR`: 输出目录
- `LOG_DIR`: 日志目录

## 许可证

MIT License
