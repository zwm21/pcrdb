"""
Initialize accounts table in PostgreSQL
Run once to create accounts table and optionally import from accounts.json

用法:
    python scripts/init_accounts.py                  # 渠道服 (默认)
    python scripts/init_accounts.py --channel bsdk   # B服

渠道差异:
    qsdk: config/accounts.qsdk.json (回退 accounts.json), 格式 {"access_key": "...", "accounts": [{"uid", "vid"}]}
    bsdk: config/accounts.bsdk.json, 格式 {"accounts": [{"username", "password", "vid"}]}
          B站账号/密码分别写入 accounts 表的 uid / access_key 列, 采集时经 bsgamesdk 换真实凭据
"""
import json
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from db.connection import get_connection
from channel import apply_channel_arg, current, config_file


def create_accounts_table():
    """Create accounts table if not exists"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        DROP TABLE IF EXISTS accounts;
        CREATE TABLE accounts (
            id SERIAL PRIMARY KEY,
            uid TEXT NOT NULL UNIQUE,
            access_key TEXT NOT NULL,

            viewer_id BIGINT,
            name TEXT,

            arena_group SMALLINT DEFAULT 0,
            grand_arena_group SMALLINT DEFAULT 0,

            is_active BOOLEAN DEFAULT TRUE,
            note TEXT,

            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    conn.commit()
    print("accounts table created")


def _load_channel_accounts(data: dict, sdk_mode: str):
    """按渠道格式解析 JSON, 统一返回 [(uid, access_key, vid), ...]"""
    result = []
    if sdk_mode == 'bsgamesdk':
        # bsdk: 每账号独立 B站账号密码
        for acc in data.get('accounts', []):
            username = str(acc.get('username', '')).strip()
            password = str(acc.get('password', '')).strip()
            vid = acc.get('vid')
            if not username or not password or '在此填入' in username:
                continue
            result.append((username, password, vid))
    else:
        # qsdk: 共享 access_key + 每账号 uid
        access_key = data.get('access_key', '')
        for acc in data.get('accounts', []):
            uid = str(acc.get('uid', ''))  # uid is a string
            vid = acc.get('vid')
            if not uid:
                continue
            result.append((uid, access_key, vid))
    return result


def import_from_json():
    """Import accounts from channel-specific json"""
    cfg = current()
    config_path = config_file('accounts_json')

    if not config_path.exists():
        print(f"账号文件不存在: {config_path}")
        return

    print(f"从 {config_path.name} 导入 [{cfg['name']}] 账号...")

    with open(config_path, encoding='utf-8') as f:
        data = json.load(f)

    rows = _load_channel_accounts(data, cfg['sdk_mode'])
    if not rows:
        print("未解析到有效账号 (bsdk 请检查 username/password 是否已填写)")
        return

    conn = get_connection()
    cursor = conn.cursor()

    imported = 0
    for uid, access_key, vid in rows:
        try:
            cursor.execute("""
                INSERT INTO accounts (uid, access_key, viewer_id, is_active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (uid) DO NOTHING
            """, (uid, access_key, vid))
            imported += 1
        except Exception as e:
            print(f"Error importing {uid}: {e}")
            conn.rollback()

    conn.commit()
    print(f"Imported {imported} accounts from JSON")


def show_accounts():
    """Display current accounts"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, uid, viewer_id, name, arena_group, grand_arena_group, is_active FROM accounts ORDER BY id")
    rows = cursor.fetchall()

    print(f"\nTotal accounts: {len(rows)}")
    print("-" * 90)
    print(f"{'ID':>3} | {'UID':>25} | {'Viewer ID':>15} | {'Name':>10} | {'JJC':>3} | {'PJJC':>4} | Active")
    print("-" * 90)

    for row in rows:
        uid_short = str(row[1])[-10:] if row[1] else '-'  # Show last 10 chars
        print(f"{row[0]:>3} | ...{uid_short:>22} | {row[2] or '-':>15} | {row[3] or '-':>10} | {row[4]:>3} | {row[5]:>4} | {'Yes' if row[6] else 'No'}")


def main():
    ch = apply_channel_arg()
    print(f"目标渠道: {current()['name']} ({ch})")
    create_accounts_table()
    import_from_json()
    show_accounts()


if __name__ == '__main__':
    main()
