# pcrdb - 公主连结数据采集系统

从公主连结游戏 API 采集公会、成员、竞技场等数据的 ETL 工具。

## 快速开始

### 方式一：Docker 部署 (推荐)

最简单的方式是使用 Docker Compose 一键启动数据库和应用。

1. **准备配置**:
   复制 `.env.example` 为 `.env` 并填入数据库密码：

   ```bash
   cp .env.example .env
   ```
2. **启动服务**:

   ```bash
   docker-compose up -d
   ```

### 方式二：本地运行

1. **安装依赖**:

   ```bash
   pip install -r requirements.txt
   ```
2. **配置数据库**:
   确保本地安装了 PostgreSQL，并在 `.env` 中配置连接信息。
3. **运行任务**:

   ```bash
   python cli.py task clan_sync
   ```

## 目录结构

```
pcrdb/
├── cli.py              # 命令行入口
├── scheduler.py        # 任务调度器
├── docker-compose.yml  # Docker 编排配置
├── src/pcrdb/          # 源代码
│   ├── api/            # 游戏 API 客户端
│   ├── models/         # 数据库模型
│   ├── tasks/          # 采集任务逻辑
│   └── analysis/       # 数据分析模块
├── config/             # 配置文件 (需自行创建/配置)
│   ├── accounts.json   # 游戏账号配置 (敏感信息，不上传)
│   ├── schedule.yaml   # 任务调度配置
│   └── unit_id.json    # 角色 ID 映射
└── docs/               # 文档和示例文件
```

## 配置说明

本项目依赖 `config/` 目录下的配置文件运行。

1. **账号配置** (`config/accounts.json`):
   包含游戏账号的认证信息。**请勿提交此文件到版本控制。**
2. **调度配置** (`config/schedule.yaml`):
   定义定时任务的执行规则。可参考 `docs/schedule.yaml` (如果存在) 或创建新文件。
3. **环境变量** (`.env`):
   定义数据库连接信息和访问密钥。参考 `.env.example`。

## 双渠道采集 (渠道服 + B服)

pcrdb 支持分别从**渠道服** (qsdk, 默认) 和 **B服** (bsdk, bilibili 官服) 采集数据，
两服数据写入**同一 PostgreSQL 实例下的两个独立数据库**（默认 `pcrdb` / `pcrdb_bsdk`），互不影响。

### 渠道选择方式（三选一，优先级从高到低）

```bash
# 1. CLI 全局参数
python cli.py --channel bsdk task clan_sync

# 2. daily_sync 交互选择 (开头第一个选项, 默认渠道服)
daily_sync.bat            # 交互选择
daily_sync.bat bsdk       # 直接指定, 跳过渠道选择

# 3. .env 默认值
PCRDB_CHANNEL=qsdk
```

### B服首次配置步骤

```bash
# 1. 以 postgres 超级用户建库 (zwm 需有所有权)
CREATE DATABASE pcrdb_bsdk OWNER zwm;

# 2. 初始化表结构
python scripts/apply_schema.py --channel bsdk

# 3. 填写 B服账号 (B站账号密码, 非 uid/access_key)
#    编辑 config/accounts.bsdk.json 后导入:
python scripts/init_accounts.py --channel bsdk

# 4. 确认账号可用性 (见下方"账号练度要求")
python scripts/check_accounts.py --channel bsdk

# 5. (可选) 实测公会 ID 上限, 校准首轮全量扫描范围
python scripts/probe_clan_max.py --channel bsdk

# 6. 试跑公会同步 (空库默认全量扫描 1~400000, 可用 PCRDB_BSDK_FULL_SCAN_MAX 调整)
python cli.py --channel bsdk task clan_sync
```

### B服账号练度要求（重要）

B服（官服）对低等级账号有**功能解锁门槛**，渠道服没有：

| 功能 | 解锁条件 | 影响的采集任务 |
|---|---|---|
| 行会 | 通关主线 NORMAL 3-1 | `clan_sync`、无公会复查 |
| 竞技场 | 通关主线 NORMAL 4-6 | `arena_deck_sync` |
| 公主竞技场 | 通关主线 NORMAL 8-15 | `grand_sync` |

未解锁的账号调用对应 API 会返回通用错误"发生了错误。回到标题界面。"。
**TaskQueue 并发采集时所有活跃账号都会发起查询，因此所有 B服采集号都需要解锁**
（只需解锁功能，不需要实际加入公会）。`profile/get_profile` 在等级过低时同样会被拒，
练号后务必用 `check_accounts.py --channel bsdk` 全部验证一遍再跑采集。

### 渠道差异速查

| 项 | 渠道服 (qsdk) | B服 (bsdk) |
|---|---|---|
| 数据库配置 | `PCRDB_*` (兼容) / `PCRDB_QSDK_*` | `PCRDB_BSDK_*` |
| 账号文件 | `config/accounts.json` | `config/accounts.bsdk.json` |
| 账号凭据 | 自抓 uid + 共享 access_key | B站账号密码 (自动经 bsgamesdk 换凭据) |
| 无公会名单 | `config/clanless_players.json` | `config/clanless_players.bsdk.json` |
| 版本文件 | `version.txt` | `version.bsdk.txt` |
| 极验过码 | 不需要 | `PCRDB_GEETEST_API` (默认公共服务) |

> **注意**: B服账号密码存于 B服库的 `accounts` 表 (uid/access_key 列)，属敏感信息；
> 两服 viewer_id / clan_id 体系独立，数据不可跨库混用。

## CLI 命令

使用 `cli.py` 手动运行采集任务。

```bash
# 查看帮助
python cli.py --help

# 运行特定任务
python cli.py task <task_name> [args]
```

### 可用任务

| 任务名称                | 描述                     | 参数示例                         |
| :---------------------- | :----------------------- | :------------------------------- |
| `clan_sync`           | 同步公会及成员信息       | (无)                             |
| `grand_sync`          | 同步公主竞技场(PJJC)排名 | (无)                             |
| `arena_deck_sync`     | 同步竞技场防守阵容       | (无)                             |
| `player_profile_sync` | 同步玩家详细档案         | `mode=top_clans rank_limit=30` |

### 示例

```bash
# 采集前30名公会的成员档案
python cli.py task player_profile_sync --args mode=top_clans rank_limit=30

# 如果配置了月度全量模式
python cli.py task player_profile_sync --args mode=active_all
```

## 任务调度

本项目包含一个基于 Python 的调度器 `scheduler.py`，用于按计划自动执行上述任务。

```bash
python scheduler.py
```

调度规则在 `config/schedule.yaml` 中配置。

## 文档列表

- [数据库管理](docs/DATABASE.md): Schema 执行、验证与修复
- [开发指南](docs/DEVELOPMENT.md): 如何添加新功能
- [API 规范](docs/ANALYSIS_API.md): 查询接口定义
- [功能特性](docs/FEATURES.md): 功能清单与优先级
