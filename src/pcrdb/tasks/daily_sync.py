"""
每日同步组合任务
依次执行 clan_sync 和 player_profile_sync (mode=active_all, clear_before=True)
完成后可选择性地导出表到 CSV 文件

开头先选择采集渠道: 渠道服(qsdk, 默认) / B服(bsdk), 后续所有阶段
(账号导入 / 无公会导入 / 采集 / 导出) 均作用于所选渠道的独立数据库。
"""
import os
from datetime import datetime
from pathlib import Path

import yaml

from pcrdb.channel import get_channel, set_channel, current as _channel_current
from pcrdb.tasks import clan_sync, player_profile_sync
from pcrdb.db.connection import get_connection


def _get_csv_output_dir() -> str:
    """从 config/paths.yaml 读取 CSV 导出目录，配置文件缺失时使用默认值"""
    # 配置文件路径: 项目根目录 / config / paths.yaml
    config_path = Path(__file__).resolve().parent.parent.parent.parent / 'config' / 'paths.yaml'

    # 默认路径: %LOCALAPPDATA%/pcrdb/csv (C 盘公共缓存，不依赖项目目录)
    default_dir = os.path.join(os.path.expandvars('%LOCALAPPDATA'), 'pcrdb', 'csv')

    if not config_path.exists():
        return default_dir

    try:
        with open(config_path, encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get('csv_output_dir', default_dir)
    except Exception:
        return default_dir


def ask_yes_no(prompt, default=True):
    """交互式询问，返回布尔值，回车采用默认值"""
    default_str = 'Y/n' if default else 'y/N'
    while True:
        response = input(f"{prompt} [{default_str}]: ").strip().lower()
        if response == '':
            return default
        if response in ('y', 'yes'):
            return True
        if response in ('n', 'no'):
            return False
        print("请输入 y 或 n，或直接按回车")


def ask_channel(default: str = 'qsdk') -> str:
    """交互式选择采集渠道，回车采用默认值"""
    default_num = '1' if default == 'qsdk' else '2'
    while True:
        print("  1 = 渠道服 (默认)")
        print("  2 = B服 (bilibili官服)")
        response = input(f"请选择采集渠道 [1/2, 回车={default_num}]: ").strip().lower()
        if response == '':
            return default
        if response in ('1', 'qsdk', 'qudao'):
            return 'qsdk'
        if response in ('2', 'bsdk', 'b', 'b服'):
            return 'bsdk'
        print("请输入 1 或 2，或直接按回车")


def _load_script_module(script_name: str):
    """动态加载项目根目录 scripts/ 下的脚本为模块（脚本目录不在包内，无法直接 import）"""
    import importlib.util
    script_path = Path(__file__).resolve().parent.parent.parent.parent / 'scripts' / script_name
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_account_import():
    """阶段0.1: 调用 scripts/init_accounts.py 从 config/accounts.json 导入采集账号

    仅执行增量导入（ON CONFLICT DO NOTHING）与账号展示，表不存在时才建表；
    不调用其 DROP 重建逻辑，避免在每日流程中清掉现有账号的
    viewer_id / 分组 / 启用状态（init_accounts.py 单独运行时才是全量重建语义）。
    """
    mod = _load_script_module('init_accounts.py')

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT to_regclass('public.accounts')")
    table_exists = cursor.fetchone()[0] is not None
    cursor.close()

    if not table_exists:
        mod.create_accounts_table()
    mod.import_from_json()
    mod.show_accounts()


def run_clanless_import():
    """阶段0.2: 调用 scripts/import_clanless_players.py
    从 config/clanless_players.json 导入无公会玩家种子记录（名单为空时脚本自行提示并跳过）"""
    mod = _load_script_module('import_clanless_players.py')
    mod.main()


def export_single_table(table_name, output_dir):
    """导出单个表到 CSV 文件 (bsdk 渠道文件名加 _bsdk 后缀防混淆)"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    suffix = '_bsdk' if get_channel() == 'bsdk' else ''
    filename = f'{table_name}{suffix}_{timestamp}.csv'
    filepath = os.path.join(output_dir, filename)

    print(f"正在导出 {table_name} 到 {filepath} ...")

    conn = get_connection()
    cursor = conn.cursor()

    query = f"COPY (SELECT * FROM {table_name} ORDER BY collected_at DESC) TO STDOUT WITH CSV HEADER DELIMITER ','"

    with open(filepath, 'w', encoding='utf-8-sig') as f:
        cursor.copy_expert(query, f)

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    file_size_mb = os.path.getsize(filepath) / 1024 / 1024
    print(f"{table_name} 导出完成，共 {row_count} 行，文件大小: {file_size_mb:.2f} MB")
    return filepath


def export_tables_to_csv(table_flags, output_dir):
    """根据标志字典导出多个表"""
    for table, do_export in table_flags.items():
        if do_export:
            try:
                export_single_table(table, output_dir)
            except Exception as e:
                print(f"导出 {table} 失败: {e}")


def run(channel: str = None):
    """执行每日组合任务（交互式）

    Args:
        channel: 采集渠道 'qsdk' / 'bsdk'。为 None 时交互式选择
                 (默认值取当前渠道: cli --channel 或 PCRDB_CHANNEL 可预设)
    """
    # 阶段0.0: 确定采集渠道 (必须在任何 DB / 采集操作之前)
    if channel is None:
        print("=" * 60)
        print("每日同步组合任务 - 渠道选择")
        print("=" * 60)
        channel = ask_channel(default=get_channel())
    channel = set_channel(channel)

    ch_name = _channel_current()['name']
    # 显示目标数据库, 防止选错渠道写错库
    from pcrdb.db.connection import get_config as _get_config
    _db_cfg = _get_config()
    print("=" * 60)
    print(f"开始执行每日同步组合任务 [渠道: {ch_name}]")
    print(f"目标数据库: {_db_cfg['database']} @ {_db_cfg['host']}:{_db_cfg['port']}")
    print("=" * 60)

    # 交互式询问
    print("\n--- 任务选项 ---")
    do_account_import = ask_yes_no("是否执行阶段0.1: 导入采集账号 (accounts.json)？", default=False)
    do_clanless_import = ask_yes_no("是否执行阶段0.2: 导入无公会玩家id (clanless_players.json)？", default=False)
    do_clan = ask_yes_no("是否执行阶段1: 公会信息同步？", default=True)

    force_full_scan = False
    if do_clan:
        force_full_scan = ask_yes_no("阶段1是否开启全量扫描？(默认 N，全量将忽略活跃判断，扫描全部可能公会ID)", default=False)

    do_recheck = ask_yes_no("是否执行阶段1.5: 无公会玩家全量复查？(默认 N，复查超期无公会玩家与手动登记名单)", default=False)

    do_profile = ask_yes_no("是否执行阶段2: 玩家档案同步（全量刷新）？", default=True)
    do_export = ask_yes_no("是否执行阶段3: 导出 CSV 文件？", default=True)


    if do_export:
        print("\n--- 导出选项 ---")
        export_clan = ask_yes_no("是否导出 clan_snapshots？", default=False)
        export_player_clan = ask_yes_no("是否导出 player_clan_snapshots？", default=False)
        export_player_profile = ask_yes_no("是否导出 player_profile_snapshots？", default=True)
        table_flags = {
            'clan_snapshots': export_clan,
            'player_clan_snapshots': export_player_clan,
            'player_profile_snapshots': export_player_profile
        }
    else:
        table_flags = {}

    # 设置输出目录（从 config/paths.yaml 读取，缺失则使用默认值）
    output_dir = _get_csv_output_dir()
    os.makedirs(output_dir, exist_ok=True)

    # 执行阶段0.1（默认关闭）：导入采集账号，需在阶段1之前（采集依赖账号）
    if do_account_import:
        print("\n>>> 阶段 0.1: 导入采集账号 (accounts.json)\n")
        try:
            run_account_import()
        except Exception as e:
            print(f"导入采集账号失败: {e}")
    else:
        print("已跳过阶段0.1 (导入采集账号)")

    # 执行阶段0.2（默认关闭）：导入无公会玩家种子记录，当天阶段2即可采集
    if do_clanless_import:
        print("\n>>> 阶段 0.2: 导入无公会玩家id (clanless_players.json)\n")
        try:
            run_clanless_import()
        except Exception as e:
            print(f"导入无公会玩家失败: {e}")
    else:
        print("已跳过阶段0.2 (导入无公会玩家)")

    # 执行阶段1
    if do_clan:
        print("\n>>> 阶段 1/3: 公会信息同步" + (" [全量]" if force_full_scan else "") + "\n")
        clan_sync.run(force_full_scan=force_full_scan)
    else:
        print("已跳过阶段1")

    # 执行阶段1.5（默认关闭）：放在阶段2之前，复查刷新的回归玩家当天即可进入档案采集
    if do_recheck:
        print("\n>>> 阶段 1.5: 无公会玩家全量复查\n")
        player_profile_sync.run_clanless_recheck()
    else:
        print("已跳过阶段1.5 (无公会玩家复查)")

    # 执行阶段2
    if do_profile:
        print("\n>>> 阶段 2/3: 玩家档案同步（全量刷新）\n")
        player_profile_sync.run(mode='active_all', clear_before=True)
    else:
        print("已跳过阶段2")

    # 执行阶段3
    if do_export and any(table_flags.values()):
        print("\n>>> 阶段 3/3: 导出 CSV 文件\n")
        export_tables_to_csv(table_flags, output_dir)
    elif do_export:
        print("没有选择任何表导出，跳过阶段3")
    else:
        print("已跳过阶段3")

    print("=" * 60)
    print(f"每日同步组合任务完成 [渠道: {ch_name}]")
    print("=" * 60)