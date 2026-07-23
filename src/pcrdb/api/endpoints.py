"""
游戏 API 端点封装
提供高层次的游戏数据查询接口
"""
from typing import Optional, Dict, Any
from .client import PCRClient

# 渠道模块: 兼容 pcrdb.api.endpoints 与顶层 api.endpoints 两种导入方式
try:
    from ..channel import CHANNELS as _CHANNELS, current as _channel_current
except ImportError:
    from channel import CHANNELS as _CHANNELS, current as _channel_current


class PCRApi:
    """公主连结游戏 API 封装 (渠道感知: qsdk=渠道服 / bsdk=B服)"""

    def __init__(self, viewer_id: int, uid: str, access_key: str, channel: str = None):
        """
        初始化 API 客户端

        Args:
            viewer_id: 玩家 viewer_id
            uid: 账号 UID (qsdk: 自抓 uid; bsdk: B站账号)
            access_key: 访问密钥 (qsdk: 自抓 access_key; bsdk: B站密码)
            channel: 渠道 (缺省取当前全局渠道)
        """
        self.viewer_id = viewer_id
        self.uid = uid
        self.access_key = access_key
        self.channel_cfg = _CHANNELS[channel] if channel else _channel_current()
        self.client = PCRClient(viewer_id, channel=self.channel_cfg['key'])
        self.load = None
        self.home = None
        # bsdk 登录换得的 uid/access_key 缓存 (避免每次重登都走 B站 SDK)
        self._sdk_uid: Optional[str] = None
        self._sdk_access_key: Optional[str] = None

    async def _ensure_sdk_credentials(self, force_refresh: bool = False):
        """
        获取 tool/sdk_login 所需的 (uid, access_key)

        qsdk: 凭据直接可用 (自抓 uid/access_key)
        bsdk: 以 B站账号密码经 bsgamesdk 登录换取, 实例内缓存;
              force_refresh=True 时强制重新换取
        """
        if self.channel_cfg['sdk_mode'] != 'bsgamesdk':
            return self.uid, self.access_key

        if force_refresh or not self._sdk_uid:
            from . import bsgamesdk
            self._sdk_uid, self._sdk_access_key = await bsgamesdk.login_bili(
                self.uid, self.access_key
            )
        return self._sdk_uid, self._sdk_access_key

    async def login(self, force_refresh: bool = False):
        """登录游戏 (bsdk 会先走 B站 SDK 换凭据)"""
        uid, access_key = await self._ensure_sdk_credentials(force_refresh)
        # print(f'登录账号 {self.viewer_id}')
        self.load, self.home = await self.client.login(uid, access_key)

    async def _safe_call(self, endpoint: str, request: dict) -> dict:
        """安全调用 API，失败时自动重试

        异常路径强制刷新凭据: bsdk 的 access_key 长时间后会失效, 走缓存重登
        仍旧凭据仍旧失败, 需重新过码; qsdk 的 _ensure_sdk_credentials 直接返回,
        force_refresh 无实际影响。
        """
        try:
            return await self.client.call_api(endpoint, request)
        except Exception:
            await self.login(force_refresh=True)
            return await self.client.call_api(endpoint, request)

    async def query_profile(self, target_viewer_id: int) -> dict:
        """
        查询玩家档案

        Args:
            target_viewer_id: 目标玩家的 viewer_id

        Returns:
            玩家档案信息
        """
        return await self._safe_call('/profile/get_profile', {
            'target_viewer_id': target_viewer_id
        })

    async def query_clan(self, clan_id: int) -> dict:
        """
        查询公会信息

        Args:
            clan_id: 公会 ID

        Returns:
            公会详细信息，包含成员列表
        """
        return await self._safe_call('/clan/others_info', {
            'clan_id': clan_id
        })

    async def query_arena_ranking(self, page: int) -> dict:
        """
        查询 JJC 排名

        Args:
            page: 页码（每页 20 人）

        Returns:
            排名列表
        """
        return await self._safe_call('/arena/ranking', {
            'limit': 20,
            'page': page
        })

    async def query_grand_arena_ranking(self, page: int) -> dict:
        """
        查询 PJJC 排名

        Args:
            page: 页码（每页 20 人）

        Returns:
            排名列表
        """
        return await self._safe_call('/grand_arena/ranking', {
            'limit': 20,
            'page': page
        })

    async def query_arena_info(self) -> dict:
        """
        查询 JJC 信息 (用于激活/刷新)
        """
        return await self._safe_call('/arena/info', {})

    async def query_grand_arena_info(self) -> dict:
        """
        查询 PJJC 信息 (用于激活/刷新)
        """
        return await self._safe_call('/grand_arena/info', {})

    async def query_clan_battle_ranking(self, page: int, clan_id: int = 0) -> dict:
        """
        查询会战排名

        Args:
            page: 页码
            clan_id: 公会 ID（可选）

        Returns:
            会战排名列表
        """
        result = await self._safe_call('clan_battle/period_ranking', {
            'clan_id': clan_id,
            'clan_battle_id': -1,
            'period': -1,
            'month': 0,
            'page': page,
            'is_my_clan': 0,
            'is_first': 1
        })
        return result.get('period_ranking', [])


async def create_client(account: dict, channel: str = None) -> PCRApi:
    """
    创建并登录客户端

    Args:
        account: 账号信息字典，包含 vid, uid, access_key
                 (bsdk 渠道: uid=B站账号, access_key=B站密码)
        channel: 渠道 (缺省取当前全局渠道)

    Returns:
        已登录的 PCRApi 实例
    """
    client = PCRApi(
        account['vid'],
        account['uid'],
        account['access_key'],
        channel=channel
    )
    await client.login()
    return client
