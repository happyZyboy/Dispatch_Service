from __future__ import annotations

import os


class Settings:
    # 环境变量优先，保证本地、测试、生产都能复用同一套代码。
    app_name: str = os.getenv("APP_NAME", "rds")
    app_version: str = os.getenv("APP_VERSION", "0.1.0")
    debug: bool = os.getenv("DEBUG", "true").lower() == "true"
    # 当前项目统一使用 MySQL + asyncmy 异步驱动。
    database_url: str = os.getenv(
        "DATABASE_URL",
        "mysql+asyncmy://root:123456@127.0.0.1:3307/rds?charset=utf8mb4",
    )
    default_task_label: str = os.getenv("DEFAULT_TASK_LABEL", "MOVE_DEFAULT")
    redis_url: str = os.getenv("REDIS_URL", "redis://default:redis123456@127.0.0.1:6379/0")
    scheduler_poll_interval_seconds: float = float(os.getenv("SCHEDULER_POLL_INTERVAL_SECONDS", "1"))
    scheduler_lease_seconds: int = int(os.getenv("SCHEDULER_LEASE_SECONDS", "60"))
    scheduler_retry_base_seconds: int = int(os.getenv("SCHEDULER_RETRY_BASE_SECONDS", "2"))
    scheduler_retry_max_seconds: int = int(os.getenv("SCHEDULER_RETRY_MAX_SECONDS", "30"))
    scheduler_max_retries: int = int(os.getenv("SCHEDULER_MAX_RETRIES", "5"))
    scheduler_reconcile_interval_seconds: int = int(os.getenv("SCHEDULER_RECONCILE_INTERVAL_SECONDS", "10"))
    scheduler_reconcile_batch_size: int = int(os.getenv("SCHEDULER_RECONCILE_BATCH_SIZE", "100"))
    scheduler_requeue_batch_size: int = int(os.getenv("SCHEDULER_REQUEUE_BATCH_SIZE", "100"))
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://rds:rds123456@127.0.0.1:5672/")
    rabbitmq_exchange: str = os.getenv("RABBITMQ_EXCHANGE", "rds.dispatch")
    rabbitmq_queue: str = os.getenv("RABBITMQ_QUEUE", "rds.rmf.dispatch")
    rabbitmq_routing_key: str = os.getenv("RABBITMQ_ROUTING_KEY", "task.assigned")
    rabbitmq_dead_letter_queue: str = os.getenv("RABBITMQ_DEAD_LETTER_QUEUE", "rds.rmf.dispatch.dlq")
    rabbitmq_dead_letter_routing_key: str = os.getenv(
        "RABBITMQ_DEAD_LETTER_ROUTING_KEY", "task.assigned.dead"
    )
    rabbitmq_prefetch_count: int = int(os.getenv("RABBITMQ_PREFETCH_COUNT", "10"))
    rabbitmq_max_retries: int = int(os.getenv("RABBITMQ_MAX_RETRIES", "5"))
    rabbitmq_dispatch_lease_seconds: int = int(os.getenv("RABBITMQ_DISPATCH_LEASE_SECONDS", "60"))
    robot_heartbeat_timeout_seconds: int = int(os.getenv("ROBOT_HEARTBEAT_TIMEOUT_SECONDS", "30"))
    robot_heartbeat_scan_interval_seconds: int = int(os.getenv("ROBOT_HEARTBEAT_SCAN_INTERVAL_SECONDS", "5"))
    map_storage_dir: str = os.getenv("MAP_STORAGE_DIR", "data/maps")
    map_active_version_key: str = os.getenv("MAP_ACTIVE_VERSION_KEY", "rds:map:active_version")
    map_cache_channel: str = os.getenv("MAP_CACHE_CHANNEL", "rds:map:cache:invalidate")


settings = Settings()
