"""
渠道管理模块

pcrdb 支持双渠道采集:
  - qsdk: 渠道服 (默认)
  - bsdk: B服 (bilibili 官服)

设计说明:
  一个进程一次运行只服务一个渠道。当前渠道记录在环境变量 PCRDB_CHANNEL 中,
  set_channel() 本质是写入 os.environ —— 项目里同时存在多套 import 路径
  (src.pcrdb.channel / pcrdb.channel / 顶层 channel) 时会产生多个模块副本,
  模块级全局变量会双状态不一致, 而环境变量进程级唯一, 天然规避该问题。

  db.connection 的连接缓存按"连接所属渠道"自愈式重建, 不依赖事件回调。
"""
import os
import sys
from pathlib import Path

QSDK = 'qsdk'
BSDK = 'bsdk'

ENV_KEY = 'PCRDB_CHANNEL'

CHANNELS = {
    QSDK: {
        'key': QSDK,
        'name': '渠道服',
        # 游戏网关与协议常量 (pcrdb 历史在用 l3 前缀, 与 autopcr qsdkclient 的 l1 等价)
        'apiroot': 'https://l3-prod-uo-gs-gzlj.bilibiligame.net/',
        'reskey': 'd145b29050641dac2f8b19df0afe0e59',
        'platform_id': '4',
        'channel_id': '4',
        # 数据库环境变量前缀; qsdk 兼容历史无前缀变量 (PCRDB_HOST 等, 供 docker-compose)
        'db_prefix': 'PCRDB_QSDK',
        'db_legacy_fallback': True,
        # 登录方式: 凭据 (自抓 uid/access_key) 直接用于 tool/sdk_login
        'sdk_mode': 'direct',
        # 配置文件候选 (按顺序取第一个存在的; 均不存在时返回第一个作为新建目标)
        'accounts_json': ['accounts.qsdk.json', 'accounts.json'],
        'clanless_json': ['clanless_players.qsdk.json', 'clanless_players.json'],
        # 客户端版本号文件: 读=第一个存在的, 写=固定写第一个
        'version_files': ['version.txt'],
        # 空库首次全量扫描的公会 ID 上限 (可 env: PCRDB_QSDK_FULL_SCAN_MAX 覆盖)
        'full_scan_max': 52000,
    },
    BSDK: {
        'key': BSDK,
        'name': 'B服',
        # 与 autopcr bsdkclient 对齐
        'apiroot': 'https://l3-prod-all-gs-gzlj.bilibiligame.net/',
        'reskey': 'ab00a0a6dd915a052a2ef7fd649083e5',
        'platform_id': '2',
        'channel_id': '1',
        'db_prefix': 'PCRDB_BSDK',
        'db_legacy_fallback': False,
        # 登录方式: B站账号密码经 bsgamesdk 换 uid/access_key 再 tool/sdk_login
        'sdk_mode': 'bsgamesdk',
        'accounts_json': ['accounts.bsdk.json'],
        'clanless_json': ['clanless_players.bsdk.json'],
        # B服读不到自有版本文件时借渠道服 version.txt 做种子 (同一客户端, 版本同源)
        'version_files': ['version.bsdk.txt', 'version.txt'],
        # B服公会量级远大于渠道服, 初版给 40 万 (可 env: PCRDB_BSDK_FULL_SCAN_MAX 覆盖)
        'full_scan_max': 400000,
    },
}


def project_root() -> Path:
    """项目根目录 (src/pcrdb/channel.py -> 上三级)"""
    return Path(__file__).resolve().parent.parent.parent


def valid(name: str) -> bool:
    return isinstance(name, str) and name.strip().lower() in CHANNELS


def get_channel() -> str:
    """当前渠道, 缺省 qsdk"""
    ch = os.getenv(ENV_KEY, QSDK).strip().lower()
    return ch if ch in CHANNELS else QSDK


def set_channel(name: str) -> str:
    """设置当前渠道 (写环境变量, 进程级生效)"""
    name = (name or '').strip().lower()
    if name not in CHANNELS:
        raise ValueError(f"未知渠道: {name!r}, 可选: {sorted(CHANNELS)}")
    os.environ[ENV_KEY] = name
    return name


def current(channel: str = None) -> dict:
    """渠道配置字典"""
    return CHANNELS[channel or get_channel()]


def channel_name(channel: str = None) -> str:
    return current(channel)['name']


def config_file(kind: str, channel: str = None) -> Path:
    """
    解析渠道配置文件路径

    Args:
        kind: 'accounts_json' | 'clanless_json'
    """
    cfg = current(channel)
    root = project_root() / 'config'
    candidates = cfg[kind]
    for name in candidates:
        p = root / name
        if p.exists():
            return p
    return root / candidates[0]


def version_file_read(channel: str = None) -> Path:
    """版本文件读取路径: 第一个存在的候选, 均不存在返回 None"""
    cfg = current(channel)
    root = project_root()
    for name in cfg['version_files']:
        p = root / name
        if p.exists():
            return p
    return None


def version_file_write(channel: str = None) -> Path:
    """版本文件写入路径: 固定为第一个候选"""
    cfg = current(channel)
    return project_root() / cfg['version_files'][0]


def full_scan_max(channel: str = None) -> int:
    """空库首次全量扫描的公会 ID 上限 (env 可覆盖)"""
    cfg = current(channel)
    return int(os.getenv(f"{cfg['db_prefix']}_FULL_SCAN_MAX", cfg['full_scan_max']))


def apply_channel_arg(argv=None) -> str:
    """
    解析命令行 --channel X / --channel=X 并应用 (脚本入口用)

    未传参时保持 env / 默认。返回当前渠道。
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    for i, a in enumerate(argv):
        if a == '--channel' and i + 1 < len(argv):
            return set_channel(argv[i + 1])
        if a.startswith('--channel='):
            return set_channel(a.split('=', 1)[1])
    return get_channel()
