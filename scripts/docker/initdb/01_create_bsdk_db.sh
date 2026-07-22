#!/bin/sh
# pcrdb 双渠道: 首次初始化时创建 B服数据库
# 仅当配置了 PCRDB_BSDK_DATABASE 且与主库不同名时执行
set -e

if [ -z "$PCRDB_BSDK_DATABASE" ] || [ "$PCRDB_BSDK_DATABASE" = "$POSTGRES_DB" ]; then
  echo "[initdb] skip: PCRDB_BSDK_DATABASE 未设置或与主库同名"
  exit 0
fi

echo "[initdb] Creating B服 database: $PCRDB_BSDK_DATABASE"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE "$PCRDB_BSDK_DATABASE"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$PCRDB_BSDK_DATABASE')\gexec
EOSQL
echo "[initdb] done"
