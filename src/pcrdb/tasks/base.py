"""
任务队列基类
提供并发数据采集的基础设施
"""
import os
import time
import asyncio
from typing import List, Dict, Any, Callable
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.endpoints import PCRApi, create_client
from db.connection import get_accounts, Account


def format_duration(seconds: float) -> str:
    """将秒数格式化为 HH:MM:SS"""
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


class DataProcessError(Exception):
    """数据处理返回 None 时抛出的异常"""
    pass


class TaskQueue:
    """
    并发任务队列
    支持多客户端并行采集，直接写入 PostgreSQL
    """

    def __init__(
        self,
        query_list: List[int],
        data_processor: Callable[[Dict, int], Any],  # 现在接收 (data, query_id)
        pg_inserter: Callable[[List[Dict]], None],
        sync_num: int = 20,
        batch_size: int = 30
    ):
        self.query_list = query_list
        if query_list:
            self.query_list = sorted(list(set(query_list)))

        self.data_processor = data_processor
        self.pg_inserter = pg_inserter
        self.sync_num = sync_num
        self.batch_size = batch_size

        self.query_type = 'profile' if self.query_list and self.query_list[0] > 1000000000000 else 'clan'

        # 用于保护同步插入函数的异步锁，确保同一时刻只有一个插入操作执行（线程安全）
        self._db_lock = asyncio.Lock()

    async def _monitor(self):
        """进度监控协程"""
        last_log_time = 0
        while True:
            if self.processed_count >= self.total_tasks:
                break

            now = time.time()
            if now - last_log_time >= 0.2:
                pct = self.processed_count / self.total_tasks if self.total_tasks > 0 else 0
                elapsed = now - self.start_time
                rate = self.processed_count / elapsed if elapsed > 0 else 0
                eta = (self.total_tasks - self.processed_count) / rate if rate > 0 else 0

                bar_len = 30
                filled_len = int(bar_len * pct)
                bar = '█' * filled_len + '-' * (bar_len - filled_len)

                eta_str = format_duration(eta)

                sys.stdout.write(
                    f"\r|{bar}| {pct:.1%} {self.processed_count}/{self.total_tasks} "
                    f"[{rate:.1f}it/s] ETA: {eta_str}"
                )
                sys.stdout.flush()
                last_log_time = now

            await asyncio.sleep(0.1)

        elapsed = time.time() - self.start_time
        sys.stdout.write(
            f"\r|{'█' * 30}| 100.0% {self.total_tasks}/{self.total_tasks} "
            f"[{self.total_tasks / elapsed:.1f}it/s] Time: {format_duration(elapsed)}\n"
        )
        sys.stdout.flush()

    async def _fetch_one(self, client, query_id: int):
        """
        对单个 query_id 发起请求，带重试逻辑。
        成功返回 processed 数据，失败返回 None。
        """
        for retry in range(4):
            try:
                if self.query_type == 'clan':
                    result = await client.query_clan(query_id)
                else:
                    result = await client.query_profile(query_id)

                # 关键修改：传入 query_id，供 processor 使用（如解散时记录 clan_id）
                processed = self.data_processor(result, query_id)
                if processed is None:
                    raise DataProcessError(f"Processed returned None for {query_id}")
                return processed
            except Exception as e:
                if retry >= 3:
                    return None

                err_msg = str(e).lower()
                if 'auth' in err_msg or 'login' in err_msg or 'unauthorized' in err_msg:
                    try:
                        await client.login()
                    except Exception:
                        pass
                    wait = 1
                else:
                    wait = 2 ** retry

                await asyncio.sleep(wait)

        return None

    async def _worker(self, account_dict: Dict, client_index: int):
        """单个客户端工作协程"""
        client = None
        try:
            client = await create_client(account_dict)
        except Exception:
            return

        while True:
            batch = []
            try:
                for _ in range(self.batch_size):
                    if self.queue.empty():
                        break
                    query_id = self.queue.get_nowait()
                    batch.append(query_id)
            except asyncio.QueueEmpty:
                pass

            if not batch:
                break

            tasks = [asyncio.create_task(self._fetch_one(client, qid)) for qid in batch]
            results = await asyncio.gather(*tasks)

            data_batch = [r for r in results if r is not None]
            self.processed_count += len(batch)

            if self.pg_inserter and data_batch:
                async with self._db_lock:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.pg_inserter, data_batch)

    async def _run_async(self):
        accounts = get_accounts(active_only=True)
        if not accounts:
            print("错误: 没有找到活跃的采集账号 (is_active=True)")
            return

        actual_sync_num = min(self.sync_num, len(accounts))
        print(f"启动 {actual_sync_num} 个采集客户端...")

        self.queue = asyncio.Queue()
        for qid in self.query_list:
            self.queue.put_nowait(qid)

        self.total_tasks = len(self.query_list)
        self.processed_count = 0
        self.start_time = time.time()

        monitor_task = asyncio.create_task(self._monitor())

        tasks = []
        for i in range(actual_sync_num):
            account_data = accounts[i]
            acc_dict = {
                'vid': account_data.viewer_id,
                'uid': str(account_data.uid),
                'access_key': account_data.access_key
            }
            task = asyncio.create_task(self._worker(acc_dict, i))
            tasks.append(task)
            await asyncio.sleep(0.25)

        if tasks:
            await asyncio.gather(*tasks)
            await monitor_task
        else:
            print("没有成功启动任何客户端任务")

    def run(self):
        start = time.time()
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._run_async())
        finally:
            loop.close()

        elapsed = time.time() - start
        print(f"任务完成，总耗时 {format_duration(elapsed)}")