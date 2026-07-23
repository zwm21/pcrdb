"""
采集账号健康检查 (四级判定管线)

逐个登录当前渠道的活跃账号, 验证其具备完整采集能力 —— 四级全部通过才算 OK:

  1. 登录      bsgamesdk (bsdk) + 游戏登录成功
  2. 解锁      query_profile(自己) 正常返回 (功能解锁探针)
  3. 采集查询  query_clan(探针公会) 拿到真实公会数据
               (探针公会 = [1, 100, 1000, 5000, 20000] 中首个有效)
  4. 写入数据库 走生产代码路径: tasks.clan_sync.process_clan_data ->
               insert_clan_batch 真实落库, 再 SELECT 回读验证
               clan_snapshots / player_clan_snapshots 行数。
               写入的是合法采集快照 (幂等、每日去重自收敛), 保留不清理。

背景: B服 (官服) 对低等级账号有功能门槛 —— 行会需通关主线 3-1 解锁,
未解锁账号 clan/profile 查询均返回通用错误 "发生了错误。回到标题界面。",
渠道服无此限制。TaskQueue 并发采集时所有活跃账号都会发起查询,
任何一个账号不过四级, 它领到的任务份额就会静默漏采。

用法:
    python scripts/check_accounts.py [--channel bsdk]
退出码: 0 = 全部账号通过; 1 = 存在未通过账号
"""
import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from channel import apply_channel_arg, current
from db.connection import get_accounts, get_connection
from api.endpoints import create_client
from tasks.clan_sync import process_clan_data, insert_clan_batch

# 探针公会候选 (按序取首个有效)
ANCHOR_IDS = (1, 100, 1000, 5000, 20000)

# 每级失败时的指引
FAIL_HINTS = {
    '登录': '检查账号密码是否正确; bsdk 检查极验服务 PCRDB_GEETEST_API 是否可用',
    '解锁': '账号功能未解锁 (B服需通关主线 3-1), 请推图后再验证',
    '采集查询': '行会功能未解锁 (B服 3-1); 若账号等级足够仍失败, 可能是限流, 稍后再试',
    '写入数据库': '检查 schema 是否已刷 (python scripts/apply_schema.py --channel {ch}) 及 DB 权限',
}


async def _stage1_login(acc) -> dict:
    """级别1: 登录。成功返回 {'client': ...}, 失败返回 {'fail': msg}"""
    try:
        client = await create_client({
            'vid': acc.viewer_id,
            'uid': str(acc.uid),
            'access_key': acc.access_key
        })
        return {'client': client}
    except Exception as e:
        return {'fail': f'登录失败: {str(e)[:60]}'}


async def _stage2_unlock(client, r: dict) -> bool:
    """级别2: 功能解锁探针 (自查 profile)"""
    ui = (client.load or {}).get('user_info', {})
    r['level'] = ui.get('team_level')
    r['vid'] = ui.get('viewer_id') or client.client.viewer_id
    try:
        res = await client.query_profile(int(r['vid']))
    except Exception as e:
        r['fail'] = f'profile 异常: {str(e)[:50]}'
        return False
    if 'user_info' not in res:
        msg = res.get('server_error', {}).get('message', str(res)[:40])
        r['fail'] = f'profile 被拒: {msg[:40]}'
        return False
    r['name'] = res['user_info'].get('user_name', '?')
    return True


async def _stage3_query(client, r: dict):
    """级别3: 采集查询探针。成功返回 (clan_id, 响应), 失败返回 (None, None)"""
    for cid in ANCHOR_IDS:
        try:
            res = await client.query_clan(cid)
        except Exception as e:
            r['fail'] = f'clan 查询异常: {str(e)[:50]}'
            return None, None
        if 'clan' in res:
            return cid, res
        msg = res.get('server_error', {}).get('message', '')
        if '发生了错误' in msg:
            r['fail'] = f'clan 查询被拒: {msg[:40]}'
            return None, None
        # "此行会已解散" 是健康响应, 换下一个候选
    r['fail'] = f'探针公会 {ANCHOR_IDS} 全部无效, 无法取到真实数据'
    return None, None


def _cleanup_probe_records(probe_id: int, t0: datetime):
    """删除本次写入的孤立探针记录 (成功时应保留, 仅失败路径调用)

    按 (clan_id/join_clan_id = probe_id AND collected_at >= t0) 精准匹配, 只删本次;
    这两个约束都在 UNIQUE 索引里, 定位准且不会误伤同公会的历史合法快照。
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM player_clan_snapshots WHERE join_clan_id = %s AND collected_at >= %s",
            (probe_id, t0))
        m = cursor.rowcount
        cursor.execute(
            "DELETE FROM clan_snapshots WHERE clan_id = %s AND collected_at >= %s",
            (probe_id, t0))
        c = cursor.rowcount
        conn.commit()
        cursor.close()
        if c or m:
            print(f"    [cleanup] 已清理孤立探针记录: clan {c} 行 + 成员 {m} 行")
    except Exception as e:
        print(f"    [cleanup] 清理孤立探针失败(可忽略): {str(e)[:80]}")


def _stage4_write(probe_id: int, res: dict, r: dict) -> bool:
    """级别4: 生产路径真实落库 + 回读验证

    落库后任何环节失败, 都会清理刚写入的探针记录, 避免残留污染。
    """
    item = process_clan_data(res, probe_id)
    if not item or item.get('type') != 'data':
        r['fail'] = 'process_clan_data 未能加工出有效数据'
        return False

    expected_members = len(item['content']['clan']['members'])
    t0 = datetime.now()
    try:
        insert_clan_batch([item])
    except Exception as e:
        r['fail'] = f'insert_clan_batch 失败: {str(e)[:80]}'
        # 落库本身抛异常, 一般不会有部分写入, 兜底清理一下
        _cleanup_probe_records(probe_id, t0)
        return False

    # 回读验证 (collected_at 由 insert_clan_batch 内部取 now, 必 >= t0)
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM clan_snapshots WHERE clan_id = %s AND collected_at >= %s",
            (probe_id, t0))
        clan_rows = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM player_clan_snapshots WHERE join_clan_id = %s AND collected_at >= %s",
            (probe_id, t0))
        member_rows = cursor.fetchone()[0]
        cursor.close()
    except Exception as e:
        r['fail'] = f'回读验证失败: {str(e)[:80]}'
        _cleanup_probe_records(probe_id, t0)
        return False

    if clan_rows < 1:
        r['fail'] = '落库后回读不到 clan_snapshots 记录'
        _cleanup_probe_records(probe_id, t0)
        return False
    if member_rows < expected_members:
        r['fail'] = f'成员行数不足: 期望 {expected_members}, 实读 {member_rows}'
        _cleanup_probe_records(probe_id, t0)
        return False

    r['write_info'] = f'写入 clan_snapshots 1 行 + 成员 {member_rows} 行 (探针公会 {probe_id})'
    return True


async def check_one(acc) -> dict:
    """检查单个账号, 返回 {'uid_tail', 'level', 'vid', 'ok', 'fail_stage', 'fail', ...}"""
    r = {'uid_tail': str(acc.uid)[-8:], 'ok': False}

    # 级别1: 登录
    s1 = await _stage1_login(acc)
    if 'fail' in s1:
        r.update(fail_stage='登录', fail=s1['fail'])
        return r
    client = s1['client']

    # 级别2: 解锁
    if not await _stage2_unlock(client, r):
        r['fail_stage'] = '解锁'
        return r

    # 级别3: 采集查询
    probe_id, res = await _stage3_query(client, r)
    if probe_id is None:
        r['fail_stage'] = '采集查询'
        return r

    # 级别4: 写入数据库
    if not _stage4_write(probe_id, res, r):
        r['fail_stage'] = '写入数据库'
        return r

    r['ok'] = True
    return r


async def run_check() -> tuple:
    """执行账号健康检查, 返回 (ok_count, total)

    供外部集成调用 (如 daily_sync 阶段0.15), 不做 sys.exit, 让调用方决定后续动作。
    """
    cfg = current()
    accounts = get_accounts(active_only=True)
    print(f"目标渠道: {cfg['name']}, 活跃账号: {len(accounts)} 个")
    if not accounts:
        return 0, 0

    fail_stages = set()
    ok_count = 0
    for i, acc in enumerate(accounts, 1):
        r = await check_one(acc)
        if r['ok']:
            ok_count += 1
            print(f"  [{i:>2}/{len(accounts)}] ✓ ...{r['uid_tail']} "
                  f"Lv{r.get('level', '?'):>3} {r.get('name', '?')} | 四级全过: {r['write_info']}")
        else:
            fail_stages.add(r['fail_stage'])
            print(f"  [{i:>2}/{len(accounts)}] ✗ ...{r['uid_tail']} "
                  f"Lv{r.get('level', '?'):>3} vid={r.get('vid', '-')} | "
                  f"卡在[{r['fail_stage']}]: {r['fail']}")
        await asyncio.sleep(0.5)  # 错峰, 避免并发登录拥堵

    print(f"\n结果: {ok_count}/{len(accounts)} 个账号具备完整采集能力 (登录/解锁/采集查询/写入数据库)")
    for stage in ('登录', '解锁', '采集查询', '写入数据库'):
        if stage in fail_stages:
            hint = FAIL_HINTS[stage].format(ch=cfg['key'])
            print(f"提示 [{stage}]: {hint}")
    return ok_count, len(accounts)


async def main():
    """命令行入口: 全通=0, 有失败=1"""
    ok, total = await run_check()
    return 0 if total > 0 and ok == total else 1


if __name__ == '__main__':
    apply_channel_arg()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(main()))
