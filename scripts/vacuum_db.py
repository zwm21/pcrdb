"""
对快照表执行 VACUUM FULL 回收磁盘空间

用法:
    python scripts/vacuum_db.py [--channel bsdk]
"""
import sys
from pathlib import Path

import psycopg2

# 添加项目根目录到路径
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root / 'src'))


def vacuum_db():
    from pcrdb.channel import apply_channel_arg, current
    from pcrdb.db.connection import get_config

    apply_channel_arg()
    cfg_ch = current()
    cfg = get_config()

    print(f"目标渠道: {cfg_ch['name']}, 数据库: {cfg['database']}")
    print("Starting VACUUM FULL to reclaim disk space...")
    try:
        conn = psycopg2.connect(
            host=cfg['host'], port=cfg['port'],
            database=cfg['database'], user=cfg['user'], password=cfg['password']
        )
        # Connect with autocommit=True because VACUUM cannot run inside a transaction block
        conn.autocommit = True

        cursor = conn.cursor()

        tables = [
            'clan_snapshots',
            'player_clan_snapshots',
            'player_profile_snapshots',
            'grand_arena_snapshots',
            'arena_deck_snapshots'
        ]

        for table in tables:
            print(f"Vacuuming {table}...")
            cursor.execute(f"VACUUM FULL {table};")
            print(f"✓ {table} compacted.")

        cursor.close()
        conn.close()
        print("\n✅ Database optimization completed.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    vacuum_db()
