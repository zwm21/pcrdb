"""
PostgreSQL Connection Management
Provides connection pooling and helper functions for pcrdb
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# 渠道模块: 兼容 pcrdb.db.connection 与顶层 db.connection 两种导入方式
try:
    from ..channel import get_channel, current as _channel_cfg
except ImportError:
    from channel import get_channel, current as _channel_cfg


# Module-level connection cache (记录连接所属渠道, 渠道切换后自愈重建)
_connection = None
_connection_channel = None
# 每渠道配置缓存: {channel: config_dict}
_configs = {}


@dataclass
class Account:
    """Account data class"""
    id: int
    uid: str
    access_key: str
    viewer_id: Optional[int] = None
    name: Optional[str] = None
    arena_group: int = 0
    grand_arena_group: int = 0
    is_active: bool = True
    note: Optional[str] = None


def get_config(channel: str = None) -> Dict[str, Any]:
    """
    Load database configuration from .env file in project root.
    Priority: OS Environment > .env > defaults

    渠道感知:
      - qsdk: 优先 PCRDB_QSDK_*, 回退历史无前缀变量 PCRDB_* (兼容旧 .env / docker-compose)
      - bsdk: 读 PCRDB_BSDK_*
      - sync_num / batch_size / access_key: 允许 PCRDB_{渠道}_XXX 覆盖, 缺省读全局共享值
    """
    ch = channel or get_channel()
    if ch in _configs:
        return _configs[ch]

    cfg_ch = _channel_cfg(ch)
    prefix = cfg_ch['db_prefix']
    legacy = cfg_ch['db_legacy_fallback']

    # Load .env from project root
    project_root = Path(__file__).parent.parent.parent.parent
    env_file = project_root / '.env'
    load_dotenv(env_file)

    def pick(key: str, default=None):
        v = os.getenv(f'{prefix}_{key}')
        if v is None and legacy:
            v = os.getenv(f'PCRDB_{key}')
        return v if v is not None else default

    _configs[ch] = {
        'host': pick('HOST', 'localhost'),
        'port': int(pick('PORT', '5432')),
        'database': pick('DATABASE', 'pcrdb'),
        'user': pick('USER', 'postgres'),
        'password': pick('PASSWORD', ''),
        'sync_num': int(pick('SYNC_NUM', os.getenv('PCRDB_SYNC_NUM', '10'))),
        'batch_size': int(pick('BATCH_SIZE', os.getenv('PCRDB_BATCH_SIZE', '30'))),
        'access_key': pick('ACCESS_KEY', os.getenv('PCRDB_ACCESS_KEY', '')),
        'channel': ch,
        'channel_name': cfg_ch['name'],
    }
    return _configs[ch]



def create_connection(channel: str = None, **kwargs):
    """
    Create a new PostgreSQL connection

    Args:
        channel: 目标渠道 (缺省为当前渠道)
        **kwargs: Additional arguments passed to psycopg2.connect
    """
    config = get_config(channel)
    # Merge default config with kwargs
    conn_args = {
        'host': config['host'],
        'port': config['port'],
        'database': config['database'],
        'user': config['user'],
        'password': config['password']
    }
    conn_args.update(kwargs)

    return psycopg2.connect(**conn_args)


def get_connection():
    """
    Get PostgreSQL connection (cached)

    渠道感知: 缓存连接记录所属渠道, 若当前渠道已切换 (set_channel 写环境变量),
    自动关闭旧连接并重连新渠道的库 —— 对调用方透明。
    """
    global _connection, _connection_channel
    ch = get_channel()
    if _connection is not None and not _connection.closed:
        if _connection_channel == ch:
            return _connection
        close_connection()

    _connection = create_connection(channel=ch)
    _connection_channel = ch
    return _connection


def get_cursor():
    """Get a cursor from the cached connection"""
    conn = get_connection()
    return conn.cursor()


def close_connection():
    """Close the cached connection"""
    global _connection, _connection_channel
    if _connection is not None:
        _connection.close()
        _connection = None
    _connection_channel = None


def get_accounts(active_only: bool = True) -> List[Account]:
    """
    Get all accounts from database
    
    Args:
        active_only: Only return active accounts
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if active_only:
        cursor.execute("SELECT * FROM accounts WHERE is_active = TRUE ORDER BY id")
    else:
        cursor.execute("SELECT * FROM accounts ORDER BY id")
    
    accounts = []
    for row in cursor.fetchall():
        accounts.append(Account(
            id=row[0],
            uid=row[1],
            access_key=row[2],
            viewer_id=row[3],
            name=row[4],
            arena_group=row[5] or 0,
            grand_arena_group=row[6] or 0,
            is_active=row[7],
            note=row[8]
        ))
    
    return accounts


def get_accounts_by_group(group_type: str = 'grand_arena') -> Dict[int, Account]:
    """
    Get one account per arena group
    
    Args:
        group_type: 'arena' or 'grand_arena'
        
    Returns:
        {group_id: account} - one account per group
    """
    accounts = get_accounts(active_only=True)
    result = {}
    
    for acc in accounts:
        if group_type == 'grand_arena':
            group_id = acc.grand_arena_group
        else:
            group_id = acc.arena_group
        
        if group_id > 0 and group_id not in result:
            result[group_id] = acc
    
    return result


def update_account(uid: int, **kwargs):
    """
    Update account fields
    
    Args:
        uid: Account UID
        **kwargs: Fields to update (viewer_id, name, arena_group, etc.)
    """
    if not kwargs:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    set_clauses = []
    values = []
    for key, value in kwargs.items():
        set_clauses.append(f"{key} = %s")
        values.append(value)
    
    set_clauses.append("updated_at = NOW()")
    values.append(uid)
    
    query = f"UPDATE accounts SET {', '.join(set_clauses)} WHERE uid = %s"
    cursor.execute(query, values)
    conn.commit()


def insert_snapshot(table: str, data: Dict[str, Any], collected_at: datetime = None):
    """
    Insert a snapshot record
    
    Args:
        table: Target table name
        data: Column values
        collected_at: Timestamp (default: NOW())
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if collected_at is None:
        collected_at = datetime.now()
    
    data['collected_at'] = collected_at
    
    columns = list(data.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_str = ', '.join(columns)
    
    # Get unique constraint columns for ON CONFLICT
    if table == 'clan_snapshots':
        conflict_cols = 'clan_id, collected_at'
    else:
        conflict_cols = 'viewer_id, collected_at'
    
    query = f"""
        INSERT INTO {table} ({column_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    
    cursor.execute(query, [data[col] for col in columns])
    conn.commit()


def insert_snapshots_batch(table: str, records: List[Dict[str, Any]], collected_at: datetime = None):
    """
    Batch insert snapshot records
    
    Args:
        table: Target table name
        records: List of column value dicts
        collected_at: Timestamp for all records (default: NOW())
    """
    if not records:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    if collected_at is None:
        collected_at = datetime.now()
    
    # Add collected_at to all records
    for record in records:
        record['collected_at'] = collected_at
    
    columns = list(records[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_str = ', '.join(columns)
    
    # Get unique constraint columns
    if table == 'clan_snapshots':
        conflict_cols = 'clan_id, collected_at'
    else:
        conflict_cols = 'viewer_id, collected_at'
    
    query = f"""
        INSERT INTO {table} ({column_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    
    values = [[record[col] for col in columns] for record in records]
    cursor.executemany(query, values)
    conn.commit()
