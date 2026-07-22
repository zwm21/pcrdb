"""
执行 schema.sql 更新数据库结构

用法：
    python scripts/apply_schema.py                  # 渠道服 (默认)
    python scripts/apply_schema.py --channel bsdk   # B服 (初始化 pcrdb_bsdk 库)
"""
import sys
from pathlib import Path

# 添加项目根目录到路径，以便导入 src.pcrdb 模块
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root / 'src'))

import psycopg2


def get_db_config():
    """按当前渠道加载数据库配置 (复用 db.connection 的渠道感知逻辑)"""
    from pcrdb.db.connection import get_config
    cfg = get_config()
    return {
        'host': cfg['host'],
        'port': cfg['port'],
        'database': cfg['database'],
        'user': cfg['user'],
        'password': cfg['password']
    }


def apply_schema():
    """执行 schema.sql，已存在的对象会跳过"""
    schema_path = project_root / 'src' / 'pcrdb' / 'db' / 'schema.sql'
    
    if not schema_path.exists():
        print(f"❌ 找不到 schema 文件: {schema_path}")
        return False
    
    print(f"正在执行: {schema_path.relative_to(project_root)}")
    
    try:
        db_config = get_db_config()
        print(f"目标数据库: {db_config['database']} @ {db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True  # 每条语句独立提交
        
        sql_content = schema_path.read_text(encoding='utf-8')
        
        # 按分号分割语句，过滤空语句和纯注释
        statements = []
        for stmt in sql_content.split(';'):
            # 移除注释和空白
            lines = [l for l in stmt.strip().split('\n') 
                     if l.strip() and not l.strip().startswith('--')]
            if lines:
                statements.append(stmt.strip() + ';')
        
        success_count = 0
        skip_count = 0
        
        with conn.cursor() as cur:
            for stmt in statements:
                # 提取语句类型用于显示
                first_line = stmt.split('\n')[0].strip()
                try:
                    cur.execute(stmt)
                    success_count += 1
                    print(f"  ✓ {first_line[:60]}...")
                except psycopg2.errors.DuplicateTable:
                    skip_count += 1
                    print(f"  ⏭ 已存在，跳过: {first_line[:50]}...")
                    conn.rollback()  # autocommit 模式下实际无需 rollback
                except psycopg2.errors.DuplicateObject:
                    skip_count += 1
                    print(f"  ⏭ 已存在，跳过: {first_line[:50]}...")
                except psycopg2.errors.DuplicateSchema:
                    skip_count += 1
                    print(f"  ⏭ 已存在，跳过: {first_line[:50]}...")
                except Exception as e:
                    print(f"  ❌ 失败: {first_line[:50]}...")
                    print(f"     错误: {e}")
        
        conn.close()
        
        print(f"\n✅ 执行完成！成功: {success_count}, 跳过: {skip_count}")
        return True
        
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False


if __name__ == "__main__":
    from pcrdb.channel import apply_channel_arg, current
    apply_channel_arg()
    cfg = current()
    print(f"目标渠道: {cfg['name']}")
    success = apply_schema()
    sys.exit(0 if success else 1)
