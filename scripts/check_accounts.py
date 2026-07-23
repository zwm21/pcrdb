"""
采集账号健康检查

逐个登录当前渠道的活跃账号, 验证其是否具备采集能力:
  - 登录是否成功
  - team_level / 是否已解锁所需功能 (以自查 profile 为探针)

背景: B服 (官服) 对低等级账号有功能门槛 —— 行会功能需通关主线 3-1 解锁,
未解锁的账号调用 clan/others_info 与 profile/get_profile 均返回通用错误
"发生了错误。回到标题界面。", 而渠道服无此限制。
TaskQueue 并发采集时所有活跃账号都会发起查询, 因此所有 B服采集号都需要解锁。

用法:
    python scripts/check_accounts.py [--channel bsdk]
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from channel import apply_channel_arg, current
from db.connection import get_accounts
from api.endpoints import create_client


async def check_one(acc) -> dict:
    """检查单个账号, 返回结果字典"""
    result = {'uid_tail': str(acc.uid)[-8:], 'ok': False, 'detail': ''}
    try:
        client = await create_client({
            'vid': acc.viewer_id,
            'uid': str(acc.uid),
            'access_key': acc.access_key
        })
    except Exception as e:
        result['detail'] = f'登录失败: {str(e)[:60]}'
        return result

    ui = (client.load or {}).get('user_info', {})
    level = ui.get('team_level')
    vid = ui.get('viewer_id') or client.client.viewer_id
    result['level'] = level
    result['vid'] = vid

    # 探针: 自查 profile (未解锁行会/功能时返回通用错误)
    try:
        res = await client.query_profile(int(vid))
        if 'user_info' in res:
            result['ok'] = True
            result['detail'] = f"OK ({res['user_info'].get('user_name', '?')})"
        else:
            msg = res.get('server_error', {}).get('message', str(res)[:50])
            result['detail'] = f'profile 被拒: {msg[:40]} (疑似功能未解锁, 需推图)'
    except Exception as e:
        result['detail'] = f'profile 异常: {str(e)[:60]}'
    return result


async def main():
    cfg = current()
    accounts = get_accounts(active_only=True)
    print(f"目标渠道: {cfg['name']}, 活跃账号: {len(accounts)} 个")
    if not accounts:
        return

    ok_count = 0
    for i, acc in enumerate(accounts, 1):
        r = await check_one(acc)
        mark = '✓' if r['ok'] else '✗'
        ok_count += 1 if r['ok'] else 0
        print(f"  [{i:>2}/{len(accounts)}] {mark} ...{r['uid_tail']} "
              f"Lv{r.get('level', '?'):>3} vid={r.get('vid', '-')} | {r['detail']}")
        await asyncio.sleep(0.5)  # 错峰, 避免并发登录拥堵

    print(f"\n结果: {ok_count}/{len(accounts)} 个账号可用于采集")
    if ok_count < len(accounts) and cfg['key'] == 'bsdk':
        print("提示: B服账号需通关主线 3-1 解锁行会功能后才可用于公会/档案采集")
        print("      (JJC 4-6 / PJJC 8-15 解锁分别对应 arena/grand 类任务)")


if __name__ == '__main__':
    apply_channel_arg()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
