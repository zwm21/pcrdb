"""
手动导入无公会玩家 viewer_id 进行持续跟踪

用法:
    1. 在 config/clanless_players.json 的 viewer_ids 数组中填入玩家 viewer_id
    2. 运行 python scripts/import_clanless_players.py

原理:
    为每个 viewer_id 在 player_clan_snapshots 中写入一条无公会(clan 0)种子记录：
    - total_power 使用占位值 1000001 以满足 active_all 的战力门槛(>1000000)
    - last_login_time 使用当前时间以满足 30 天活跃门槛
    下一次 daily_sync 阶段2(active_all) 即会查询其真实档案并回写真实数据，
    之后 clan_sync 的去重逻辑会自动清理占位的种子记录。

    对"之前有公会、后来退会"的玩家同样适用：种子记录会成为其最新归属(无公会)，
    立即转入无公会跟踪。若误导入了实际仍在公会中的玩家，下次该公会被采集时
    会自动恢复正确归属，误差不超过一个采集周期。

    名单内玩家即使超过30天未登录脱离每日跟踪，也会被"无公会玩家全量复查"任务
    (daily_sync 阶段1.5，默认关闭；或 python cli.py task clanless_recheck)
    无条件复查，回归后自动恢复每日跟踪。
"""
import json
from datetime import datetime
from pathlib import Path
import sys

# Add src to path (与 scripts/init_accounts.py 相同方式)
sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from db.connection import get_connection

REGISTRY_PATH = Path(__file__).parent.parent / 'config' / 'clanless_players.json'

# 占位战力：满足 active_all 的 total_power > 1000000 门槛，首次采集后被真实值取代
PLACEHOLDER_POWER = 1000001


def load_registry() -> list:
    """读取登记名单"""
    if not REGISTRY_PATH.exists():
        print(f"登记文件不存在: {REGISTRY_PATH}")
        print('请创建该文件，格式: {"viewer_ids": [123456789012345]}')
        return []

    with open(REGISTRY_PATH, encoding='utf-8') as f:
        data = json.load(f)

    ids = []
    for v in data.get('viewer_ids', []):
        try:
            vid = int(v)
        except (TypeError, ValueError):
            print(f"跳过无效 viewer_id: {v!r}")
            continue
        # viewer_id 必然 > 1万亿（TaskQueue 依据该阈值区分玩家/公会查询，
        # 非法小数字一旦入库会误导 active_all 的查询类型判断）
        if vid <= 1000000000000:
            print(f"跳过非法 viewer_id (应为13位以上数字): {vid}")
            continue
        ids.append(vid)
    return ids


def get_latest_status(cursor, viewer_id: int):
    """查询玩家当前最新记录，返回 (join_clan_id, join_clan_name, last_login_time) 或 None"""
    cursor.execute("""
        SELECT join_clan_id, join_clan_name, last_login_time
        FROM player_clan_snapshots
        WHERE viewer_id = %s
        ORDER BY collected_at DESC
        LIMIT 1
    """, (viewer_id,))
    return cursor.fetchone()


def import_players(viewer_ids: list):
    """为名单内玩家写入无公会种子记录"""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now()

    inserted = 0
    for vid in viewer_ids:
        status = get_latest_status(cursor, vid)
        if status is None:
            prev = "新玩家(库中无记录)"
        elif status[0] == 0:
            prev = f"已在无公会跟踪中(最后登录 {status[2]})"
        elif status[0] is None:
            prev = "此前归属未知"
        else:
            prev = f"此前在公会 [{status[1]}]({status[0]})，转为无公会跟踪"

        cursor.execute("""
            INSERT INTO player_clan_snapshots
                (viewer_id, collected_at, name, total_power, last_login_time,
                 join_clan_id, join_clan_name)
            VALUES (%s, %s, %s, %s, %s, 0, '0')
            ON CONFLICT (viewer_id, collected_at) DO NOTHING
        """, (vid, now, '手动导入', PLACEHOLDER_POWER, now))
        inserted += cursor.rowcount
        print(f"  {vid}: {prev}")

    conn.commit()
    cursor.close()
    print(f"\n导入完成: 名单 {len(viewer_ids)} 人，写入种子记录 {inserted} 条")
    print("下一次 daily_sync 阶段2(active_all) 将开始采集这些玩家的真实档案")


def main():
    viewer_ids = load_registry()
    if not viewer_ids:
        print("名单为空，未执行导入")
        return
    print(f"从 {REGISTRY_PATH.name} 读取到 {len(viewer_ids)} 个 viewer_id\n")
    import_players(viewer_ids)


if __name__ == '__main__':
    main()
