#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pcrdb 命令行入口
公主连结渠道服数据采集系统
"""
import argparse
import sys
from pathlib import Path

# 添加 src 到路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))


def cmd_task(args):
    """运行采集任务（日志记录已集成在各task模块内部）"""
    from pcrdb.channel import current as _channel_current
    from pcrdb.tasks import clan_sync, grand_sync, arena_deck_sync, player_profile_sync, daily_sync

    task_map = {
        'clan_sync': clan_sync.run,
        'grand_sync': grand_sync.run,
        'arena_deck_sync': arena_deck_sync.run,
        'player_profile_sync': player_profile_sync.run,
        'clanless_recheck': player_profile_sync.run_clanless_recheck,
        'daily_sync': daily_sync.run,  # 新增
    }

    if args.task_name not in task_map:
        print(f"未知任务: {args.task_name}")
        print(f"可用任务: {list(task_map.keys())}")
        return 1

    # 解析参数
    kwargs = {}
    if args.args:
        for arg in args.args:
            if '=' in arg:
                k, v = arg.split('=', 1)
                kwargs[k] = int(v) if v.isdigit() else v

    print(f"运行任务: {args.task_name} [渠道: {_channel_current()['name']}]")
    try:
        task_map[args.task_name](**kwargs)
        return 0
    except Exception as e:
        print(f"任务失败: {e}")
        return 1


def main():
    parser = argparse.ArgumentParser(
        description='pcrdb - 公主连结双渠道数据采集系统 (渠道服 + B服)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
可用任务:
  clan_sync           同步公会数据
  grand_sync          同步PJJC排名数据
  arena_deck_sync     同步JJC防守阵容
  player_profile_sync 同步玩家档案
  clanless_recheck    无公会玩家全量复查
  daily_sync          每日组合任务 (交互式)

示例:
  python cli.py task clan_sync
  python cli.py --channel bsdk task clan_sync
  python cli.py --channel bsdk task daily_sync
  python cli.py task player_profile_sync --args mode=top_clans rank_limit=30
"""
    )

    parser.add_argument('--channel', choices=['qsdk', 'bsdk'], default=None,
                        help='采集渠道: qsdk=渠道服(默认) / bsdk=B服。缺省读 PCRDB_CHANNEL')

    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # task 命令
    task_parser = subparsers.add_parser('task', help='运行采集任务')
    task_parser.add_argument('task_name', help='任务名称')
    task_parser.add_argument('--args', nargs='*', help='任务参数 (key=value)')
    task_parser.set_defaults(func=cmd_task)

    args = parser.parse_args()

    # 渠道设置必须在任务模块导入/执行之前 (环境变量进程级生效)
    if getattr(args, 'channel', None):
        from pcrdb.channel import set_channel
        set_channel(args.channel)

    if args.command is None:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
