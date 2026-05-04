"""
公会信息同步任务
每月同步所有公会和成员信息
"""
import time
import threading
from typing import Dict, Any, List
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.base import TaskQueue, format_duration
from db.connection import get_connection, insert_snapshots_batch, get_config

_insert_lock = threading.Lock()


def build_query_list(new_clan_add: int = 100, full_rescan: bool = False) -> List[int]:
    """
    构建待查询的公会 ID 列表
    Parameters:
        new_clan_add: 探测新增公会的步长（常规模式）
        full_rescan: 若为 True，则全量扫描 1~52000，并排除最近已解散的公会
    """
    start_time = time.time()
    print("正在构建待查询公会列表...")

    conn = get_connection()
    print(f"Connected to Database: {get_config()['database']}")

    cursor = conn.cursor()

    # ================= 全量重扫模式（排除已解散公会）=================
    if full_rescan:
        print("模式：全量重新扫描（排除最近已解散公会）")
        # 1. 获取最近30天内已确认解散的公会 ID
        dissolved_sql = """
            SELECT DISTINCT clan_id
            FROM clan_snapshots
            WHERE exist = FALSE
              AND collected_at > NOW() - INTERVAL '30 days'
        """
        cursor.execute(dissolved_sql)
        dissolved_ids = {r[0] for r in cursor.fetchall()}
        if dissolved_ids:
            print(f"发现 {len(dissolved_ids)} 个已解散公会，将被排除")
        else:
            print("未发现已解散公会")

        # 2. 构建 1~52000 的列表并剔除已解散 ID
        full_range = set(range(1, 52001))
        scan_ids = sorted(list(full_range - dissolved_ids))
        print(f"待扫描公会数量（排除解散后）: {len(scan_ids)}")

        cursor.close()
        conn.close()
        query_cost = time.time() - start_time
        print(f"构建列表耗时: {format_duration(query_cost)}")
        return scan_ids

    # ================= 常规模式 =================
    # 1. 来自玩家快照的公会（成员尚在的公会）
    known_sql = """
        SELECT DISTINCT join_clan_id
        FROM player_clan_snapshots
        WHERE join_clan_id IS NOT NULL
    """
    cursor.execute(known_sql)
    known_clans = [r[0] for r in cursor.fetchall()]

    # 2. 来自公会快照、最近30天标记为存活的公会（补全可能已无成员的公会）
    survive_sql = """
        SELECT DISTINCT clan_id
        FROM clan_snapshots
        WHERE exist = TRUE
          AND collected_at > NOW() - INTERVAL '30 days'
    """
    cursor.execute(survive_sql)
    survive_clans = [r[0] for r in cursor.fetchall()]

    # 合并去重
    known_set = set(known_clans) | set(survive_clans)

    # 3. 如果当前库无数据，尝试从生产库获取
    if not known_set:
        current_db = get_config()['database']
        if current_db != 'pcrdb':
            print(f"当前库 {current_db} 无已知公会，尝试从生产库 pcrdb 获取...")
            try:
                import psycopg2
                prod_cfg = get_config().copy()
                prod_cfg['database'] = 'pcrdb'
                with psycopg2.connect(
                    host=prod_cfg['host'], port=prod_cfg['port'],
                    user=prod_cfg['user'], password=prod_cfg['password'],
                    database='pcrdb'
                ) as prod_conn:
                    with prod_conn.cursor() as prod_cur:
                        prod_cur.execute(known_sql)
                        known_clans = [r[0] for r in prod_cur.fetchall()]
                if known_clans:
                    known_set = set(known_clans)
                    print(f"从生产库获取到 {len(known_set)} 个已知公会")
            except Exception as e:
                print(f"从生产库获取失败: {e}")

    # 4. 兜底：仍然为空时，使用默认全量范围 1-52000
    if not known_set:
        print("无任何已知公会数据，使用默认全量范围 1-52001")
        cursor.close()
        conn.close()
        query_cost = time.time() - start_time
        print(f"构建列表耗时: {format_duration(query_cost)}")
        return list(range(1, 52001))

    # 5. 常规模式：基于已知公会计算扫描范围
    max_known = max(known_set)
    safe_add = max(new_clan_add, 2000)
    probe_start = max_known + 1
    probe_end = max_known + safe_add
    print(f"新增探测范围: {probe_start} ~ {probe_end}")

    now = datetime.now()
    is_full_scan_month = (now.month == 1 or now.month == 7)

    if is_full_scan_month:
        print(f"全量月 ({now.month}月)，扫描范围 1 ~ {probe_end}")
        final_list = list(range(1, probe_end + 1))
    else:
        extra_clans = list(range(probe_start, probe_end + 1))
        final_list = sorted(list(known_set | set(extra_clans)))
        print(f"普通月，综合后待扫描公会: {len(final_list)} 个（已知 {len(known_set)} + 新增探测 {len(extra_clans)}）")

    query_cost = time.time() - start_time
    print(f"构建列表耗时: {format_duration(query_cost)}")
    cursor.close()
    conn.close()
    return final_list


def process_clan_data(clan_data: Dict[str, Any], clan_id: int) -> Dict[str, Any] | None:
    """
    处理公会 API 返回数据，返回统一格式的字典或 None
    新增：识别解散公会并返回 disband 类型
    """
    if 'clan' in clan_data:
        return {
            "type": "data",
            "content": clan_data,
            "clan_id": clan_data['clan']['detail']['clan_id']
        }
    elif 'server_error' in clan_data:
        msg = clan_data.get('server_error', {}).get('message', '')
        if '此行会已解散' in msg or '连接中断' in msg:
            return {
                "type": "disband",
                "clan_id": clan_id,
                "reason": msg
            }
    return None


def insert_clan_batch(data_batch: List[Dict]):
    """批量插入公会快照和成员快照（支持解散记录）"""
    with _insert_lock:
        clan_records = []
        member_records = []
        now = datetime.now()

        for item in data_batch:
            typ = item.get('type')
            if typ == 'data':
                content = item['content']
                clan = content['clan']
                detail = clan['detail']
                members = clan['members']

                clan_records.append({
                    'clan_id': detail['clan_id'],
                    'clan_name': detail['clan_name'],
                    'leader_viewer_id': detail['leader_viewer_id'],
                    'leader_name': detail['leader_name'],
                    'join_condition': detail['join_condition'],
                    'activity': detail['activity'],
                    'clan_battle_mode': detail['clan_battle_mode'],
                    'member_num': detail['member_num'],
                    'current_period_ranking': detail['current_period_ranking'],
                    'grade_rank': detail['grade_rank'],
                    'description': detail['description'],
                    'exist': True
                })

                for m in members:
                    login_ts = m['last_login_time']
                    login_time = datetime.fromtimestamp(login_ts) if login_ts else None
                    member_records.append({
                        'viewer_id': m['viewer_id'],
                        'name': m['name'],
                        'level': m['level'],
                        'role': m['role'],
                        'total_power': m['total_power'],
                        'join_clan_id': detail['clan_id'],
                        'join_clan_name': detail['clan_name'],
                        'last_login_time': login_time
                    })
            elif typ == 'disband':
                # 解散公会：仅记录 clan_id 且 exist=False，其他字段留空
                clan_records.append({
                    'clan_id': item['clan_id'],
                    'clan_name': None,
                    'leader_viewer_id': None,
                    'leader_name': None,
                    'join_condition': None,
                    'activity': None,
                    'clan_battle_mode': None,
                    'member_num': None,
                    'current_period_ranking': None,
                    'grade_rank': None,
                    'description': None,
                    'exist': False
                })

        if clan_records:
            insert_snapshots_batch('clan_snapshots', clan_records, collected_at=now)
        if member_records:
            insert_snapshots_batch('player_clan_snapshots', member_records, collected_at=now)


def deduplicate_player_clan_snapshots():
    """玩家公会快照去重"""
    print("正在进行玩家公会快照去重...")
    start_time = time.time()
    conn = get_connection()
    cursor = conn.cursor()
    delete_sql = """
        DELETE FROM player_clan_snapshots
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY viewer_id, join_clan_name
                           ORDER BY last_login_time DESC NULLS LAST, collected_at DESC
                       ) AS rn
                FROM player_clan_snapshots
            ) t
            WHERE rn > 1
        )
    """
    cursor.execute(delete_sql)
    deleted_count = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    elapsed = time.time() - start_time
    print(f"去重完成，删除了 {deleted_count} 条重复记录，耗时 {format_duration(elapsed)}")
    return deleted_count


def deduplicate_clan_snapshots():
    """公会快照去重"""
    print("正在进行公会快照去重...")
    start_time = time.time()
    conn = get_connection()
    cursor = conn.cursor()
    delete_sql = """
        DELETE FROM clan_snapshots
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY clan_id, clan_name, leader_viewer_id
                           ORDER BY collected_at DESC
                       ) AS rn
                FROM clan_snapshots
            ) t
            WHERE rn > 1
        )
    """
    cursor.execute(delete_sql)
    deleted_count = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    elapsed = time.time() - start_time
    print(f"公会去重完成，删除了 {deleted_count} 条重复记录，耗时 {format_duration(elapsed)}")
    return deleted_count


def run(new_clan_add: int = 100, full_rescan: bool = False):
    """
    运行公会信息同步任务
    Parameters:
        new_clan_add: 新增公会探测步长（常规模式）
        full_rescan: 若为 True，则全量重扫 [1, 52000]
    """
    from db.task_logger import TaskLogger

    print("=" * 60)
    print(f"公会信息同步任务 (PostgreSQL)")
    if full_rescan:
        print(">>> 全量重扫模式已开启，将扫描所有 1~52000 <<<")
    print("=" * 60)

    config = get_config()

    query_list = build_query_list(new_clan_add, full_rescan=full_rescan)
    query_count = len(query_list)
    print(f"待查询公会: {query_count} 个")

    records_expected = query_count * 31  # 粗略估计

    fetch_counter = {'count': 0}

    def insert_with_count(data_batch):
        fetch_counter['count'] += len(data_batch)
        insert_clan_batch(data_batch)

    task_logger = TaskLogger('clan_sync')
    task_logger.start(
        records_expected=records_expected,
        details={
            'new_clan_add': new_clan_add,
            'query_count': query_count,
            'full_rescan': full_rescan
        }
    )

    try:
        queue = TaskQueue(
            query_list=query_list,
            data_processor=process_clan_data,   # 直接使用，base 会传入 query_id
            pg_inserter=insert_with_count,
            sync_num=config['sync_num'],
            batch_size=config['batch_size']
        )
        queue.run()

        deduplicate_player_clan_snapshots()
        deduplicate_clan_snapshots()

        task_logger.finish_success(records_fetched=fetch_counter['count'])
    except Exception as e:
        task_logger.finish_failed(str(e), records_fetched=fetch_counter['count'])
        raise


if __name__ == '__main__':
    full = False
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ('--full-rescan', '--y'):
            full = True
    run(full_rescan=full)