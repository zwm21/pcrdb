"""
探测当前渠道公会 ID 实际上限 (用于校准 PCRDB_{渠道}_FULL_SCAN_MAX)

原理:
    clan/others_info 对"已解散"和"从未创建"的 ID 返回相同错误, 无法逐 ID 区分;
    但前沿之上全部无效, 前沿之下活公会有一定密度 (B服为稀疏空间, 采样实测约 1/3)。
    按指数分段 [1w,2w,4w,...] 逐段采样定位首个"空段", 再在末个活段与首个空段间
    二分细化。空段判定经两轮错开复采, 稀疏空间误判率可忽略。

注意 (实测教训):
    - 窗口采样必须保证进度: 命中点 <= lo 时不能推进 lo, 需按"无命中"处理, 否则死循环
    - B服对单账号短时高频查询会返回通用错误 ("发生了错误"), 疑似软限流;
      本脚本对通用错误做有限重试, 连续出现则中止 (ProbeBlockedError)
    - 低等级/未解锁行会功能的账号所有查询均返回通用错误 (FeatureLockedError),
      脚本会自动跳过该账号换下一个

成本: 约 100~150 次 API 请求, 单账号数分钟。

用法:
    python scripts/probe_clan_max.py [--channel bsdk]
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'pcrdb'))

from channel import apply_channel_arg, current
from db.connection import get_accounts
from api.endpoints import create_client

# 分段扫描参数: 从 SEG_START 起按 SEG_WIDTH 逐段上探
SEG_START = 10_000
SEG_WIDTH = 40_000
# 每段最多采样点数 (活段命中即停, 平均仅数次; 空段双倍复采确认)
SEG_SAMPLES = 8
# 判定越过前沿需连续 EMPTY_TO_STOP 个确认空段 (稀疏空间存在中段死区, 单空段不足为据)
EMPTY_TO_STOP = 2
# 探测保护上限
HARD_CAP = 3_000_000
# 每次查询间隔 (B服有软限流迹象, 放缓)
QUERY_INTERVAL = 0.3
# 通用错误连续出现次数上限 (超过判定为限流/会话失效, 中止)
GENERIC_ERROR_LIMIT = 5


class FeatureLockedError(Exception):
    """账号功能未解锁 (低等级号所有查询均返回通用错误)"""
    pass


class ProbeBlockedError(Exception):
    """探测中途连续通用错误 (疑似软限流/会话失效)"""
    pass


class ClanProber:
    """带通用错误统计的公会存在性探测器"""

    def __init__(self, client, total_queries: int):
        self.client = client
        self.generic_errors = 0
        self.total_queries = total_queries  # 本账号累计查询数 (含历史)
        self.started_at = total_queries

    async def exists(self, clan_id: int) -> bool:
        """True=有效公会; False=已解散/未创建/连续失败(保守按无效)"""
        for _ in range(3):
            try:
                res = await self.client.query_clan(clan_id)
                self.total_queries += 1
                if 'clan' in res:
                    self.generic_errors = 0
                    return True
                msg = res.get('server_error', {}).get('message', '')
                if '此行会已解散' in msg:
                    self.generic_errors = 0
                    return False
                if '发生了错误' in msg:
                    self.generic_errors += 1
                    if self.total_queries <= 10:
                        # 账号刚登录就通用错误 -> 功能未解锁
                        raise FeatureLockedError('账号未解锁行会功能')
                    if self.generic_errors >= GENERIC_ERROR_LIMIT:
                        raise ProbeBlockedError(
                            f'连续 {self.generic_errors} 次通用错误, 疑似软限流/会话失效 '
                            f'(本账号已查询约 {self.total_queries} 次)'
                        )
                    await asyncio.sleep(2)
                    continue
                # 其他错误按无效处理
                return False
            except (FeatureLockedError, ProbeBlockedError):
                raise
            except Exception:
                await asyncio.sleep(1)
            finally:
                await asyncio.sleep(QUERY_INTERVAL)
        return False

    async def first_exists(self, ids):
        """按给定顺序查询, 返回第一个有效公会 ID, 全无效返回 None"""
        for i in ids:
            if i > 0 and await self.exists(i):
                return i
        return None


async def find_anchor(prober: ClanProber) -> int:
    """在低 ID 段找一个确认有效的锚点"""
    for cid in (1, 100, 1000, 5000, 20000, 50000):
        if await prober.exists(cid):
            print(f"  锚点: ID {cid} 有效")
            return cid
    raise RuntimeError('低 ID 段无有效公会, 账号或服务器异常, 请先跑 check_accounts.py')


async def segment_has_live(prober: ClanProber, start: int, width: int):
    """[start, start+width) 随机采样 SEG_SAMPLES 点, 任一有效即 (True, hit);
    首轮全空则换一批复采确认 (稀疏空间降低误判)"""
    import random
    for round_no in range(2):
        ids = random.sample(range(start, start + width), min(SEG_SAMPLES, width))
        hit = await prober.first_exists(ids)
        if hit is not None:
            return True, hit
    return False, None


async def probe(prober: ClanProber) -> int:
    """返回 (估计上限, 末个活段结束位置)

    B服空间稀疏且密度不均 (老段大面积解散形成"死区", 高段密度回升),
    不能以单个空段判定前沿, 需连续 EMPTY_TO_STOP 个确认空段。
    """
    await find_anchor(prober)  # 仅验证空间可用

    empty_streak = 0
    last_live_end = 0
    max_hit = 0
    start = SEG_START

    while start < HARD_CAP:
        live, hit = await segment_has_live(prober, start, SEG_WIDTH)
        if live:
            empty_streak = 0
            max_hit = max(max_hit, hit)
            last_live_end = start + SEG_WIDTH
            print(f"  段 [{start}, {start + SEG_WIDTH}): 活 (段内命中 {hit})")
        else:
            empty_streak += 1
            print(f"  段 [{start}, {start + SEG_WIDTH}): 空 (连续 {empty_streak}/{EMPTY_TO_STOP})")
            if empty_streak >= EMPTY_TO_STOP:
                break
        start += SEG_WIDTH

    if last_live_end == 0:
        raise RuntimeError(f"从 {SEG_START} 起所有分段均为空, 请检查账号/服务器")
    if start >= HARD_CAP:
        print(f"  已达探测保护上限 {HARD_CAP}, 前沿可能更高")

    return max_hit, last_live_end


async def main():
    cfg = current()
    print(f"目标渠道: {cfg['name']}")

    accounts = get_accounts(active_only=True)
    if not accounts:
        print("错误: 当前渠道库中没有活跃采集账号")
        return

    # 依次尝试账号: 未解锁行会功能的账号自动跳过
    max_id = None
    for acc in accounts:
        tail = str(acc.uid)[-8:]
        try:
            client = await create_client({
                'vid': acc.viewer_id,
                'uid': str(acc.uid),
                'access_key': acc.access_key
            })
        except Exception as e:
            print(f"账号 ...{tail} 登录失败, 跳过: {str(e)[:60]}")
            continue
        print(f"使用账号 ...{tail}, 开始探测...")
        prober = ClanProber(client, total_queries=0)
        try:
            max_id = await probe(prober)
            break
        except FeatureLockedError:
            print(f"账号 ...{tail} 未解锁行会功能, 尝试下一个账号...")
            continue
        except ProbeBlockedError as e:
            print(f"\n探测中止: {e}")
            print("建议: 稍后重试, 或在脚本中调大 QUERY_INTERVAL 降低查询频率")
            return

    if max_id is None:
        print("\n探测失败: 所有活跃账号均未解锁行会功能 (B服需通关主线 3-1)")
        print("请先练号并验证: python scripts/check_accounts.py --channel bsdk")
        return

    max_hit, last_live_end = max_id
    # 建议值: 末个活段结束位置 +25% 余量, 向上取整到万位
    # (稀疏空间无法精确定位前沿, 余量用于覆盖末段之后的零散新高; 偏低比偏高危害大)
    suggest = ((int(last_live_end * 1.25) // 10000) + 1) * 10000
    env_name = f"{cfg['db_prefix']}_FULL_SCAN_MAX"

    print()
    print(f"最大确认有效公会 ID = {max_hit}, 末个活段结束于 {last_live_end}")
    print(f"建议在 .env 中设置: {env_name}={suggest}")
    print("(首轮全量扫描完成后, 该值不再被使用 —— 之后为活跃+探测滚动模式)")


if __name__ == '__main__':
    apply_channel_arg()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
