from __future__ import annotations

import threading
import time


class SnowflakeGenerator:
    epoch = 1609459200000 # 2021-01-01 00:00:00


    def __init__(self, worker_id: int = 1, datacenter_id: int = 1) -> None:
        """
        初始化雪花 ID 生成器，并设置机器编号、数据中心编号和序列状态。
        """
        self.worker_id = worker_id & 0x1F
        self.datacenter_id = datacenter_id & 0x1F    # 0x1F = 31，二进制 11111
        self.sequence = 0  # 当前毫秒内的序列号(12bits，同一毫秒内最多生成 4096 个 ID)
        self.last_timestamp = -1  # 上次生成 ID 的时间戳
        self.lock = threading.Lock()  # 线程锁，保证多线程安全

    def _timestamp(self) -> int:
        """
        获取当前 Unix 时间对应的毫秒级时间戳。
        """
        return int(time.time() * 1000)

    def next_id(self) -> int:
        """
        在线程安全的前提下生成一个全局唯一的雪花算法 ID。
        """
        # 线程安全，同一时刻只有一个线程能执行
        with self.lock:
            # 同一毫秒内靠 sequence 防冲突，跨毫秒则重置序列号。
            timestamp = self._timestamp()

            # 时钟回拨保护：如果当前时间小于上次生成时间，使用上次时间
            if timestamp < self.last_timestamp:
                timestamp = self.last_timestamp
            # 同一毫秒内的处理
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 0xFFF
                # 同一毫秒内超过 4096 个 ID，等待下一毫秒
                if self.sequence == 0:
                    while timestamp <= self.last_timestamp:
                        timestamp = self._timestamp()
            else:
                self.sequence = 0
            self.last_timestamp = timestamp
            return (
                ((timestamp - self.epoch) << 22)
                | (self.datacenter_id << 17)
                | (self.worker_id << 12)
                | self.sequence
            )


id_worker = SnowflakeGenerator()
