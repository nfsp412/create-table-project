# create-table-project

根据 **Excel** 或 **JSON** 配置自动生成 Hive / ClickHouse 建表 / 改表 SQL，并写入 `rpa` 汇总页的 Python 工具。

**版本**: 1.1.0  
**Python 要求**: >= 3.12

## 版本说明（v1.1.0）

- **必须显式指定输入方式**：`--input-excel`、`--json-file` 或 `--json-string` 三选一，**不再支持无参数运行**。
- JSON 输入的字段约定、示例与校验规则见下文 **「JSON 输入（完整说明）」**，全部在本文档中给出。

## 快速开始

### 安装依赖（使用 uv，推荐）

```bash
cd create-table-project
uv sync
```

### 两种入口（互斥）


| 方式        | 参数                                         | 说明                                                                                                 |
| --------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| **Excel** | `--input-excel PATH`                       | 读取含 `tables` / `fields` 的 xlsx（与旧版一致）                                                              |
| **JSON**  | `--json-file PATH` 或 `--json-string '...'` | 按下文 JSON 约定解析，内存中构造与 Excel 等价的表数据，**不**先写入中间 Excel；输出 SQL 与 `create_table_info.xlsx` 的 `rpa` sheet |


输出目录：默认 `create-table-output/YYYYMMDD/`（与 `app/config/settings.py` 中 `OUTPUT_BASE_DIR` 一致），可用 `--output-dir` 覆盖。

### CLI 参数一览

| 参数 | 必填 | 说明 |
|------|------|------|
| `--input-excel PATH` | 三选一 | 输入 xlsx，须含 `tables`、`fields` sheet |
| `--json-file PATH` | 三选一 | 输入 JSON 文件，编码建议 UTF-8 |
| `--json-string JSON` | 三选一 | 直接传入 JSON 字符串（注意 shell 引号转义） |
| `--output-dir PATH` | 否 | 输出根目录；默认 `OUTPUT_BASE_DIR/YYYYMMDD`（当天日期） |
| `--debug` | 否 | 开启 DEBUG 日志 |

上述三种输入方式**互斥**，必须且只能指定其一。

### 示例：Excel

```bash
uv run create-table --input-excel /path/to/create_table_info.xlsx
```

### 示例：JSON 文件

```bash
uv run create-table --json-file /path/to/input.json
```

### 示例：JSON 字符串

```bash
uv run create-table --json-string '{"mysql_sql":"CREATE TABLE `t` (id int);",...}'
```

### 调试

```bash
uv run create-table --input-excel ./create_table_info.xlsx --debug
# JSON 文件或 JSON 字符串同样可附加 --debug
uv run create-table --json-file ./input.json --debug
```

### 安装为命令行工具

`create-table=app.main:main`（见 `pyproject.toml`）。

```bash
uv tool install . --editable
uv run create-table --json-file ../create-table-output/20260327/input.json
```

说明：

- 若提示 `create-table: command not found`，请使用 `uv run create-table`。
- 生成的 SQL 与 `create_table_info.xlsx`（含 `rpa` sheet）写在 `--output-dir` 或默认日期目录下；**JSON 模式**下若该路径尚无文件，会**新建**仅含 `rpa` sheet 的 xlsx（或覆盖已有文件的 `rpa` sheet）。

### JSON 输入（完整说明）

以下约定与实现一致，对应代码为 `app/utils/json_sql_parser.py`、`app/utils/input_from_json.py`、`app/input_models.py`。

#### 功能说明

支持两类 JSON 输入（**顶层可为单对象或数组**）：

1. **新建表（含 `mysql_sql`）**  
   以下 **8 个字段均须存在且非空**，否则记录 WARNING 并跳过该项：  
   `mysql_sql`、`product_line`、`day_or_hour`、`dw_layer`、`table_format`、`target_table_format`、`operate_type`（须为 `新建表`）、`is_sharding`。  
   另可选：`table_comment`（不提供则从 `mysql_sql` 表级 COMMENT 解析）。  
   从 `mysql_sql` 解析 MySQL 表名；**新建表**在内存中的 `fields` 等价于 Excel 中一行，建表语句列为完整 DDL。

2. **修改表（含 `new_fields` 数组）**  
   须包含：`table_name`、`operate_type`（须为 `修改表`）、`target_table_format`、`new_fields`（非空数组）。  
   每项为 `{ "field_name": "...", "field_type": "..." }`，`field_type` 可省略（默认 `string`）。  
   在 `tables` 等价行中 `hive表名` 与 `表名` 均填 `table_name`；在 `fields` 中每个新字段一行，无建表语句。

其他行为说明：

- 输出目录仍为 `create-table-output/YYYYMMDD/`（可用 `--output-dir` 覆盖）；JSON 模式下会生成/覆盖该目录下的 `create_table_info.xlsx`（通常仅含 `rpa` sheet，见上文「说明」）。
- 缺少必需字段或 SQL 解析失败时，记录日志并跳过该项。
- 若数组中**全部**项均被跳过（或解析后无任何有效对象），程序会抛出错误并**以退出码 1** 结束（与 `load_json_input` / `parse_json_to_items` 行为一致）。
- SQL 中包含未转义的双引号时（如 `DEFAULT ""` 或 `COMMENT "xxx"`），解析侧会将双引号替换为单引号后继续（MySQL 中两者等价）。

#### JSON 路由规则

以 **`operate_type`** 为准（须为 `新建表` 或 `修改表`）：

- **`operate_type` 为 `修改表`**：必须提供**非空** `new_fields` 数组；否则记录 WARNING 并跳过该项。不要求 `mysql_sql`。
- **`operate_type` 为 `新建表`**：必须提供**非空** `mysql_sql`；否则记录 WARNING 并跳过该项。
- 若同一对象中同时出现 `mysql_sql` 与 `new_fields`，仍只根据 `operate_type` 走对应分支（例如 `新建表` 时只消费 `mysql_sql`，`修改表` 时只消费 `new_fields` 等修改表字段）。
- 缺少 `operate_type` 或取值不在上述二者之内：记录 WARNING 并跳过该项。

#### 字段映射（JSON → 与 Excel 一致的列）


| Sheet  | 列名           | 来源                                            | 可选值               |
| ------ | ------------ | --------------------------------------------- | ----------------- |
| tables | 表名           | 从 `mysql_sql` 解析                              | -                 |
| tables | 产品线          | `product_line`                                | -                 |
| tables | 入仓方式         | `day_or_hour`                                 | 天表, 小时表           |
| tables | 表注释信息        | `table_comment`（可选，回退从 SQL COMMENT 解析）        | -                 |
| tables | 数仓分层         | 新建表必填 `dw_layer`；修改表为空                        | ods, mds, sds     |
| tables | 建表格式         | 新建表必填 `table_format`；修改表为空                    | orc, rcfile, text |
| tables | 目标表类型        | `target_table_format`；新建表必填；修改表必填             | hive, clickhouse  |
| tables | 操作类型         | `operate_type`；新建表必填；修改表必填                                | 新建表, 修改表          |
| tables | hive表名       | 新建表：为空；**修改表**：与 `table_name` 相同              | -                 |
| tables | 是否分库分表       | 仅新建表：`is_sharding`（必填枚举）；修改表行为空               | 是, 否              |
| fields | 表名           | **新建表**：从 `mysql_sql` 解析；**修改表**：`table_name` | -                 |
| fields | 字段名 / 字段数据类型 | **新建表**：空；**修改表**：`new_fields` 每项一行           | -                 |
| fields | 操作类型         | 与 tables 一致                                   | 新建表, 修改表          |
| fields | 建表语句         | **新建表**：`mysql_sql` 原文；**修改表**：空              | -                 |
| fields | 字段注释         | **新建表**：空；**修改表**：可空                          | -                 |


#### `input.json` 示例（新建表，含可选字段）

```json
{
  "mysql_sql": "CREATE TABLE `ai_media_task` (\n  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键ID',\n  `task_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '任务ID'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='AI媒体任务表'",
  "day_or_hour": "天表",
  "product_line": "sfst",
  "dw_layer": "ods",
  "table_format": "orc",
  "target_table_format": "hive",
  "operate_type": "新建表",
  "table_comment": "AI媒体任务表",
  "is_sharding": "否"
}
```

**新建表**：上述 8 项缺一不可（枚举须在允许值内）。`table_comment` 可选；未提供则从 `mysql_sql` 表级 `COMMENT` 解析；若 JSON 中显式写空字符串则仍回退解析 SQL。

#### JSON 数组（批量）

输入可为数组 `[{...}, {...}]`，每项为一条「新建表」或「修改表」配置。单对象 `{...}` 仍兼容。

```json
[
  {
    "mysql_sql": "CREATE TABLE `table_a` (id int);",
    "day_or_hour": "天表",
    "product_line": "sfst",
    "dw_layer": "ods",
    "table_format": "orc",
    "target_table_format": "hive",
    "operate_type": "新建表",
    "is_sharding": "否"
  },
  {
    "table_name": "ods_ad_wax_table_b_day",
    "operate_type": "修改表",
    "target_table_format": "hive",
    "new_fields": [
      {"field_name": "new_col", "field_type": "bigint"}
    ]
  }
]
```

#### 命令行一行 JSON 示例

```bash
uv run create-table --json-string '{
  "mysql_sql": "CREATE TABLE `my_table` (id int) COMMENT='\''示例表'\''",
  "day_or_hour": "天表",
  "product_line": "sfst",
  "dw_layer": "ods",
  "table_format": "orc",
  "target_table_format": "hive",
  "operate_type": "新建表",
  "is_sharding": "否"
}'
```

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
  - 当目标表类型为 `clickhouse` 且操作类型为 `修改表` 时，生成 `ALTER TABLE ... ADD COLUMN ...`（本地表与 `_cluster` 各一条）；有字段注释时列带 `COMMENT`，无注释时不输出列 `COMMENT`
  - 当目标表类型为 `hive` 且操作类型为 `修改表` 时，生成 `alter table default.<表名> add columns (... ) cascade;`；有字段注释时列带 `COMMENT`，无注释时不输出列 `COMMENT`

### fields sheet（统一格式，5 列 + 可选列）

- `表名`（必需）
- `字段名`
- `字段数据类型`
- `字段注释`（自动规范化：换行符等特殊字符会统一转换为单个空格）
- `建表语句`
- `操作类型`（可选）：`新建表` / `修改表`，用于 ClickHouse 修改表场景

**处理规则（逐行检测）**：

- 有"建表语句"且不为空 → 解析建表语句提取字段信息
- 有"字段名"、"字段数据类型"且不为空 → 直接使用这些字段信息（字段注释允许为空；**Hive / ClickHouse 的新建表与修改表**在注释为空时生成 SQL 均不输出列 `COMMENT`）。
- 同一 sheet 中可以混合使用两种方式
- 字段注释会进行规范化：换行符、制表符等空白字符会被合并为单个空格

### rpa sheet（运行后自动写入）

每次运行结束会在**同一文件** `create-table-output/YYYYMMDD/create_table_info.xlsx` 中**新建或覆盖** `rpa` 工作表，不修改 `tables`、`fields` 中已有内容。


| 列名     | Hive 新建表                                         | ClickHouse 新建表      |
| ------ | ------------------------------------------------ | ------------------- |
| 数据描述信息 | 从生成的 Hive DDL 中解析表级 `COMMENT`                    | 空                   |
| 数仓分层   | 与主流程一致；`tables` 中空单元格时写入默认 `ods`                 | 空                   |
| 建表语句   | 与落盘的 `.sql` 一致但**去掉** `LOCATION '...'` 整行        | 完整 DDL（与 `.sql` 一致） |
| 存储路径值  | `LOCATION` 中单引号内的路径（如 `viewfs://c9/dw/ods/xxx`）  | 空                   |
| 表类型    | `hive` / `clickhouse`，与主流程一致；`目标表类型` 空时默认 `hive` | `clickhouse`        |


**说明**：`tables` 页「操作类型」为**修改表**时，仍会生成 `_hive_alter.sql` / `_ck_alter.sql`，但**当前版本不向 `rpa` sheet 追加行**。若本次运行没有任何「新建表」成功落地，仍会写入仅含表头的 `rpa` sheet。

## 类型转换规则

### MySQL → Hive


| MySQL 类型                        | Hive 类型 |
| ------------------------------- | ------- |
| BIGINT, INT, INTEGER, TINYINT   | BIGINT  |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | STRING  |
| 其他类型                            | STRING  |


### MySQL → ClickHouse


| MySQL 类型                        | ClickHouse 类型 |
| ------------------------------- | ------------- |
| BIGINT, INT, INTEGER, TINYINT   | Int64         |
| FLOAT, DOUBLE, DECIMAL, NUMERIC | String        |
| 其他类型                            | String        |


## 输出说明

- **输出根目录**：`app/config/settings.py` 中的 `OUTPUT_BASE_DIR`（默认为项目父目录下的 `create-table-output/` 目录）
- **日期归档**：每次运行时自动在输出根目录下创建当日日期目录（格式 `YYYYMMDD`），SQL 文件写入该目录；同目录下的 `create_table_info.xlsx` 会在运行结束时回写 **`rpa` sheet**（见上文「rpa sheet」）
- **Hive 表名规则**：`ods_ad_产品线_表名_入仓方式`（入仓方式标准化为 day/hour）
- **文件命名**：
  - Hive 新建表：`{hive表名}_hive.sql`
  - Hive 修改表：`{hive表名}_hive_alter.sql`
  - ClickHouse 新建表：`{hive表名}_ck.sql`
  - ClickHouse 修改表：`{hive表名}_ck_alter.sql`

输出示例目录结构：

```
create-table-output/
└── 20260318/
    ├── ods_ad_pl_t1_day_hive.sql
    ├── ods_ad_pl_t2_day_hive_alter.sql
    ├── ods_ad_pl_t3_day_ck.sql
    └── ods_ad_pl_t4_day_ck_alter.sql
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
uv run python -m unittest discover -v tests

# 运行单个测试文件（示例）
uv run python -m unittest tests.test_main_flow -v
uv run python -m unittest tests.test_mysql_parser -v
```

## 测试覆盖概览


| 测试文件                              | 覆盖内容                                                                               |
| --------------------------------- | ---------------------------------------------------------------------------------- |
| `test_main_flow.py`               | 主流程关键分支（hive/clickhouse 分流、输出路径/后缀、输出目录创建、存在旧文件先删除、fields 匹配失败跳过、rpa 汇总与修改表跳过 rpa） |
| `test_main_entry.py`              | `main()`：JSON 无有效项 / JSON 文件不存在时退出码 1 |
| `test_json_input.py`              | JSON → `json_items_to_dataframes` / `load_json_input` 与 Excel 路径字段列对齐              |
| `test_rpa_sheet.py`               | rpa 列解析（去 LOCATION、LOCATION 路径、表级 COMMENT）、`write_rpa_sheet` 保留其他 sheet、目标文件不存在时新建 |
| `test_mysql_parser.py`            | MySQL 建表解析状态机（括号/引号/逗号场景、跳过约束行）                                                    |
| `test_hive_create_sql.py`         | Hive 建表 SQL 拼装分支（分区、存储格式、LOCATION、表注释后缀）                                           |
| `test_hive_alter_sql.py`          | Hive 修改表 ALTER 分支                                                                  |
| `test_clickhouse_create_sql.py`   | ClickHouse 建表 SQL（本地表/分布式表、ORDER BY、自增主键）与 ALTER（无列注释时不输出 `COMMENT`）                    |
| `test_excel_reader_load_excel.py` | `load_excel()` 入口校验与异常（文件不存在、fields 缺列、最小成功路径）                                     |
| `test_empty_field_comment.py`     | 字段注释为空时仍保留字段，且 Hive 建表 SQL 不输出列 `COMMENT`                                         |
| `test_fields_sheet_processing.py` | fields sheet 混合格式（建表语句解析 + 直填字段）处理逻辑                                               |
| `test_table_builder.py`           | 表名构建、入仓方式标准化                                                                       |
| `test_table_matching.py`          | 表名匹配逻辑（优先使用 hive 表名匹配 fields）                                                      |
| `test_type_converter.py`          | MySQL → Hive/ClickHouse 类型转换（整数、浮点、小数、字符串）                                         |
| `test_logger.py`                  | 日志配置（log_dir 写入指定目录）                                                               |


## 常见问题

### 使用 `--input-excel` 时提示 Excel 不存在

请传入**真实存在的** xlsx 路径（含 `tables` / `fields`）。若你习惯把模板放在 `create-table-output/YYYYMMDD/create_table_info.xlsx`，请确认该日期目录与文件名一致。

### JSON 文件不存在、无法解析或全部项被跳过

- 使用 `--json-file` 时：路径须存在且可读，编码建议 UTF-8。
- 使用 `--json-string` 时：须为合法 JSON；注意在终端中对引号、反斜杠转义。
- 顶层为数组时，若每一项都因缺字段、`operate_type` 不合法等原因被跳过，会报错退出（退出码 1），请检查日志中的 WARNING。

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
- 支持 Hive、ClickHouse 新建表和修改表（ALTER ADD COLUMN）
- 支持直接指定 Hive 表名
- 灵活的字段定义方式（建表语句解析 + 直接字段信息）
- 字段注释自动规范化（换行符等特殊字符统一转换为单个空格）
- 输出按运行日期自动归档
- 完善的日志系统

## 技术栈

- Python >= 3.12（见 `pyproject.toml` 中的 `requires-python`）
- pandas
- openpyxl
- tabulate

## 项目结构

```
create-table-project/
├── app/
│   ├── __init__.py
│   ├── main.py                 # 主程序入口（Excel / JSON 互斥入口，run_generation）
│   ├── input_models.py         # JSON 输入数据结构（InputData / ModifyTableInput）
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py         # 配置（路径、日志格式）
│   └── utils/
│       ├── __init__.py
│       ├── excel_reader.py     # Excel 读取；process_fields_dataframe
│       ├── json_sql_parser.py  # JSON 解析为 InputData / ModifyTableInput
│       ├── input_from_json.py  # JSON → tables/fields DataFrame
│       ├── rpa_sheet.py        # 写入/新建 rpa sheet
│       ├── mysql_parser.py     # MySQL 建表语句解析（状态机）
│       ├── table_builder.py    # SQL 构建（Hive/ClickHouse）
│       ├── type_converter.py   # 类型映射（MySQL → Hive/ClickHouse）
│       └── logger.py           # 日志配置
├── tests/                      # 单元测试（15 个测试模块）
├── logs/                       # 日志目录（fallback，主流程使用输出目录）
├── pyproject.toml              # 项目与依赖配置（uv / pip 共用）
└── uv.lock                     # uv 锁文件（运行 uv sync 后生成）
```

## 配置说明

配置文件：`app/config/settings.py`


| 配置项               | 说明                                                                                                     |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| `PROJECT_ROOT`    | 项目根目录（自动计算）                                                                                            |
| `EXCEL_FILENAME`  | Excel 文件名（默认：`create_table_info.xlsx`，运行时按日期拼接为 `create-table-output/YYYYMMDD/create_table_info.xlsx`） |
| `OUTPUT_BASE_DIR` | 输出根目录（默认：项目父目录下 `create-table-output/`，运行时自动追加日期子目录）                                                   |
| `LOG_DIR`         | 日志目录。实际运行时日志写入 `create-table-output/YYYYMMDD/create_table.log`（与输出目录一致）；仅当未传入 `log_dir` 时使用项目下 `logs/` |


## 许可证

MIT License