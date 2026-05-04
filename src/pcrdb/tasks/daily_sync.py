"""
每日同步组合任务
依次执行 clan_sync 和 player_profile_sync (mode=active_all, clear_before=True)
完成后可选择性地导出表到 CSV 文件
"""
import os
from datetime import datetime
from pcrdb.tasks import clan_sync, player_profile_sync
from pcrdb.db.connection import get_connection


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


def export_single_table(table_name, output_dir):
    """导出单个表到 CSV 文件"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'{table_name}_{timestamp}.csv'
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


def run():
    """执行每日组合任务（交互式）"""
    print("=" * 60)
    print("开始执行每日同步组合任务")
    print("=" * 60)

    # 交互式询问
    print("\n--- 任务选项 ---")
    do_clan = ask_yes_no("是否执行阶段1: 公会信息同步？", default=True)
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

    # 设置输出目录
    output_dir = r'E:\思い出の扉\mzq\pcrdb\pcrdb-main\留档'
    os.makedirs(output_dir, exist_ok=True)

    # 执行阶段1
    if do_clan:
        print("\n>>> 阶段 1/3: 公会信息同步")
        print()
        clan_sync.run()
    else:
        print("已跳过阶段1")

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
    print("每日同步组合任务完成")
    print("=" * 60)