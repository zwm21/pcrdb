"""
每日同步组合任务
依次执行 clan_sync 和 player_profile_sync (mode=active_all, clear_before=True)
"""
from pcrdb.tasks import clan_sync, player_profile_sync

def run():
    """执行每日组合任务"""
    print("=" * 60)
    print("开始执行每日同步组合任务")
    print("=" * 60)
    
    # 第一步：公会信息同步
    print("\n>>> 阶段 1/2: 公会信息同步\n")
    clan_sync.run()
    
    # 第二步：玩家档案同步（全量清空再同步）
    print("\n>>> 阶段 2/2: 玩家档案同步（全量刷新）\n")
    player_profile_sync.run(mode='active_all', clear_before=True)
    
    print("=" * 60)
    print("每日同步组合任务完成")
    print("=" * 60)