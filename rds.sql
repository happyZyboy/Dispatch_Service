-- 基于《初版调度系统核心表详解》整理的初版建表脚本
-- 说明：
-- 1. 面向 MySQL 8.x / InnoDB / utf8mb4
-- 2. 按文档字段生成，不包含外键约束
-- 3. 保留主键、唯一索引和常用查询索引，便于后续调度查询

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `t_windtasklog`;
DROP TABLE IF EXISTS `t_windblockrecord`;
DROP TABLE IF EXISTS `t_windtaskrecord`;
DROP TABLE IF EXISTS `t_mapedge`;
DROP TABLE IF EXISTS `t_mapnode`;
DROP TABLE IF EXISTS `t_mapversion`;
DROP TABLE IF EXISTS `t_windtaskdef`;
DROP TABLE IF EXISTS `t_alarmsrecord`;
DROP TABLE IF EXISTS `robot_current_state`;
DROP TABLE IF EXISTS `t_robotstatusrecord`;
DROP TABLE IF EXISTS `t_worksite`;
DROP TABLE IF EXISTS `t_robotitem`;

CREATE TABLE `t_robotitem` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `added_on` datetime DEFAULT NULL COMMENT '创建时间',
  `del` int NOT NULL DEFAULT 0 COMMENT '删除标记，0 正常，1 删除',
  `update_on` datetime DEFAULT NULL COMMENT '更新时间',
  `uuid` varchar(64) NOT NULL COMMENT '机器人业务唯一编码',
  `robot_code` varchar(64) DEFAULT NULL COMMENT '机器人编码',
  `robot_name` varchar(255) DEFAULT NULL COMMENT '机器人名称',
  `robot_type` varchar(64) DEFAULT NULL COMMENT '机器人类型',
  `enable_status` int NOT NULL DEFAULT 1 COMMENT '启用状态，0 禁用，1 启用',
  `battery_threshold` decimal(10,2) DEFAULT 20.00 COMMENT '参与自动调度的最低电量阈值',
  `current_map` varchar(255) DEFAULT NULL COMMENT '当前地图名称或业务标识',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_robotitem_uuid` (`uuid`),
  KEY `idx_t_robotitem_del` (`del`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='机器人档案表';

CREATE TABLE `t_robotstatusrecord` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `duration` bigint DEFAULT NULL COMMENT '状态持续时长',
  `ended_on` datetime DEFAULT NULL COMMENT '结束时间',
  `location` varchar(255) DEFAULT NULL COMMENT '机器人位置',
  `new_status` int DEFAULT NULL COMMENT '新状态',
  `odo` decimal(19,2) DEFAULT NULL COMMENT '累计里程',
  `old_status` int DEFAULT NULL COMMENT '旧状态',
  `started_on` datetime DEFAULT NULL COMMENT '开始时间',
  `today_odo` decimal(19,2) DEFAULT NULL COMMENT '当日里程',
  `uuid` varchar(64) NOT NULL COMMENT '机器人业务唯一编码',
  `vehicle_name` varchar(255) DEFAULT NULL COMMENT '车辆名称',
  PRIMARY KEY (`id`),
  KEY `idx_t_robotstatusrecord_uuid` (`uuid`),
  KEY `idx_t_robotstatusrecord_status` (`new_status`),
  KEY `idx_t_robotstatusrecord_started_on` (`started_on`),
  KEY `idx_t_robotstatusrecord_ended_on` (`ended_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='机器人状态记录表';

CREATE TABLE `robot_current_state` (
  `robot_id` bigint NOT NULL COMMENT '机器人主键 ID',
  `uuid` varchar(64) NOT NULL COMMENT '机器人唯一标识',
  `vehicle_name` varchar(255) DEFAULT NULL COMMENT '车辆名称',
  `current_status` int DEFAULT NULL COMMENT '当前机器人状态',
  `dispatch_status` int NOT NULL DEFAULT 0 COMMENT '当前调度状态，0 离线，1 空闲，2 忙碌，3 充电，4 故障，5 锁定',
  `current_task_id` bigint DEFAULT NULL COMMENT '当前任务记录 ID',
  `current_site_id` varchar(64) DEFAULT NULL COMMENT '当前地图节点/库位编码，必须使用 RobotShop .smap 的 instanceName',
  `current_location` varchar(255) DEFAULT NULL COMMENT '当前位置信息',
  `battery_level` decimal(10,2) DEFAULT NULL COMMENT '当前电量',
  `has_unresolved_alarm` int NOT NULL DEFAULT 0 COMMENT '是否有未恢复报警，0 无，1 有',
  `alarm_level` varchar(64) DEFAULT NULL COMMENT '当前最高报警级别',
  `last_status_record_id` bigint DEFAULT NULL COMMENT '最后状态流水 ID',
  `last_heartbeat_at` datetime DEFAULT NULL COMMENT '最后心跳时间',
  `version` int NOT NULL DEFAULT 1 COMMENT '乐观锁版本号',
  `updated_at` datetime DEFAULT NULL COMMENT '快照更新时间',
  PRIMARY KEY (`robot_id`),
  UNIQUE KEY `uk_robot_current_state_uuid` (`uuid`),
  KEY `idx_robot_current_state_dispatch_status` (`dispatch_status`),
  KEY `idx_robot_current_state_current_task_id` (`current_task_id`),
  KEY `idx_robot_current_state_current_site_id` (`current_site_id`),
  KEY `idx_robot_current_state_last_heartbeat_at` (`last_heartbeat_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='机器人当前状态快照表';

CREATE TABLE `t_worksite` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `agv_id` varchar(64) DEFAULT NULL COMMENT 'AGV ID',
  `area` varchar(255) DEFAULT NULL COMMENT '仓库',
  `disabled` int NOT NULL DEFAULT 0 COMMENT '是否禁用',
  `filled` int NOT NULL DEFAULT 0 COMMENT '是否已占用',
  `group_name` varchar(255) DEFAULT NULL COMMENT '库区',
  `holder` int NOT NULL DEFAULT 0 COMMENT '占位类型',
  `no` varchar(255) DEFAULT NULL COMMENT '编号',
  `preparing` int NOT NULL DEFAULT 0 COMMENT '是否预占',
  `row_num` varchar(255) DEFAULT NULL COMMENT '行号',
  `column_num` int DEFAULT NULL COMMENT '列号',
  `site_id` varchar(64) NOT NULL COMMENT '库位业务编码，必须使用 RobotShop .smap advancedPointList.instanceName',
  `site_name` varchar(255) DEFAULT NULL COMMENT '库位名称',
  `sync_failed` int NOT NULL DEFAULT 0 COMMENT '是否同步失败',
  `type` int DEFAULT NULL COMMENT '库位类型',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注',
  `added_on` datetime DEFAULT NULL COMMENT '创建时间',
  `update_on` datetime DEFAULT NULL COMMENT '更新时间',
  `del` int NOT NULL DEFAULT 0 COMMENT '删除标记，0 正常，1 删除',
  `working` int NOT NULL DEFAULT 0 COMMENT '是否作业中',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_worksite_site_id` (`site_id`),
  KEY `idx_t_worksite_agv_id` (`agv_id`),
  KEY `idx_t_worksite_group_name` (`group_name`),
  KEY `idx_t_worksite_type` (`type`),
  KEY `idx_t_worksite_status_flags` (`disabled`,`filled`,`preparing`,`working`),
  KEY `idx_t_worksite_added_on` (`added_on`),
  KEY `idx_t_worksite_del` (`del`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='库位表';

CREATE TABLE `t_mapversion` (
  `id` bigint NOT NULL COMMENT '地图版本内部主键，建议使用雪花算法生成',
  `map_name` varchar(255) NOT NULL COMMENT '地图名称，对应 .smap header.mapName',
  `source_version` varchar(64) DEFAULT NULL COMMENT '地图源文件版本，对应 .smap header.version',
  `map_type` varchar(64) DEFAULT NULL COMMENT '地图类型，例如 2D-Map',
  `resolution` decimal(10,4) DEFAULT NULL COMMENT '地图分辨率，例如 0.02 米',
  `min_x` decimal(12,4) DEFAULT NULL COMMENT '地图范围最小 X 坐标',
  `min_y` decimal(12,4) DEFAULT NULL COMMENT '地图范围最小 Y 坐标',
  `max_x` decimal(12,4) DEFAULT NULL COMMENT '地图范围最大 X 坐标',
  `max_y` decimal(12,4) DEFAULT NULL COMMENT '地图范围最大 Y 坐标',
  `source_file` varchar(512) NOT NULL COMMENT '原始 .smap 文件保存路径',
  `file_hash` varchar(64) NOT NULL COMMENT '原始地图文件 SHA-256 哈希',
  `node_count` int NOT NULL DEFAULT 0 COMMENT '解析出的地图节点数量',
  `edge_count` int NOT NULL DEFAULT 0 COMMENT '解析出的拓扑边数量',
  `status` varchar(16) NOT NULL DEFAULT 'DRAFT' COMMENT '地图状态，DRAFT 草稿，ACTIVE 当前使用，ARCHIVED 历史版本',
  `created_on` datetime NOT NULL COMMENT '地图导入时间',
  `activated_on` datetime DEFAULT NULL COMMENT '地图激活时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_mapversion_file_hash` (`file_hash`),
  KEY `idx_t_mapversion_map_name_status` (`map_name`,`status`),
  KEY `idx_t_mapversion_created_on` (`created_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='地图版本表';

CREATE TABLE `t_mapnode` (
  `id` bigint NOT NULL COMMENT '地图节点内部主键，建议使用雪花算法生成',
  `map_version_id` bigint NOT NULL COMMENT '所属地图版本 ID，对应 t_mapversion.id',
  `node_code` varchar(128) NOT NULL COMMENT '地图节点编码，对应 .smap advancedPointList.instanceName',
  `node_type` varchar(64) NOT NULL COMMENT '地图节点类型，对应 .smap advancedPointList.className',
  `x` decimal(12,4) DEFAULT NULL COMMENT '节点 X 坐标',
  `y` decimal(12,4) DEFAULT NULL COMMENT '节点 Y 坐标',
  `ignore_dir` int NOT NULL DEFAULT 0 COMMENT '是否忽略节点方向，0 否，1 是',
  `properties` longtext COMMENT '地图节点原始属性 JSON',
  `is_enabled` int NOT NULL DEFAULT 1 COMMENT '节点是否启用，0 禁用，1 启用',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_mapnode_version_code` (`map_version_id`,`node_code`),
  KEY `idx_t_mapnode_version_type` (`map_version_id`,`node_type`),
  KEY `idx_t_mapnode_enabled` (`is_enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='地图节点表';

CREATE TABLE `t_mapedge` (
  `id` bigint NOT NULL COMMENT '拓扑边内部主键，建议使用雪花算法生成',
  `map_version_id` bigint NOT NULL COMMENT '所属地图版本 ID，对应 t_mapversion.id',
  `edge_code` varchar(255) NOT NULL COMMENT '拓扑边编码，对应 .smap advancedCurveList.instanceName',
  `edge_type` varchar(64) DEFAULT NULL COMMENT '拓扑边类型，例如 DegenerateBezier',
  `from_node_code` varchar(128) NOT NULL COMMENT '有向边起点节点编码',
  `to_node_code` varchar(128) NOT NULL COMMENT '有向边终点节点编码',
  `direction` decimal(10,4) DEFAULT NULL COMMENT '地图原始方向属性',
  `move_style` int DEFAULT NULL COMMENT '地图原始移动方式属性',
  `cost` decimal(19,6) DEFAULT NULL COMMENT '路径代价，默认可使用边长度',
  `geometry` longtext COMMENT '曲线起终点和控制点等几何 JSON',
  `is_enabled` int NOT NULL DEFAULT 1 COMMENT '拓扑边是否启用，0 禁用，1 启用',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_mapedge_version_code` (`map_version_id`,`edge_code`),
  KEY `idx_t_mapedge_from_node` (`map_version_id`,`from_node_code`),
  KEY `idx_t_mapedge_to_node` (`map_version_id`,`to_node_code`),
  KEY `idx_t_mapedge_enabled` (`is_enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='地图拓扑边表';

CREATE TABLE `t_windtaskdef` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `create_date` datetime DEFAULT NULL COMMENT '创建日期',
  `delay` int NOT NULL DEFAULT 0 COMMENT '延迟执行时长',
  `detail` longtext COMMENT '任务定义详情',
  `if_enable` int NOT NULL DEFAULT 1 COMMENT '是否启用',
  `label` varchar(64) NOT NULL COMMENT '任务定义标识',
  `period` int NOT NULL DEFAULT 0 COMMENT '周期',
  `periodic_task` int NOT NULL DEFAULT 0 COMMENT '是否周期任务',
  `project_id` varchar(255) DEFAULT NULL COMMENT '项目 ID',
  `release_sites` bit(1) NOT NULL DEFAULT b'0' COMMENT '是否释放站点',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注',
  `status` int DEFAULT NULL COMMENT '任务定义状态',
  `template_name` varchar(255) DEFAULT NULL COMMENT '模板名称',
  `version` int NOT NULL DEFAULT 1 COMMENT '版本号',
  `windcategory_id` bigint DEFAULT NULL COMMENT 'Wind 分类 ID',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_t_windtaskdef_label_version` (`label`,`version`),
  KEY `idx_t_windtaskdef_project_id` (`project_id`),
  KEY `idx_t_windtaskdef_status` (`status`),
  KEY `idx_t_windtaskdef_if_enable` (`if_enable`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务定义表';

CREATE TABLE `t_windtaskrecord` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `created_on` datetime DEFAULT NULL COMMENT '创建时间',
  `def_id` bigint DEFAULT NULL COMMENT '定义 ID',
  `def_label` varchar(150) DEFAULT NULL COMMENT '定义标识',
  `def_version` int DEFAULT NULL COMMENT '定义版本',
  `ended_on` datetime DEFAULT NULL COMMENT '结束时间',
  `ended_reason` longtext COMMENT '结束原因',
  `input_params` longtext COMMENT '输入参数',
  `status` int DEFAULT NULL COMMENT '任务执行状态',
  `task_def_detail` longtext COMMENT '任务定义快照',
  `variables` longtext COMMENT '变量快照',
  `agv_id` varchar(64) DEFAULT NULL COMMENT 'AGV ID',
  `executor_time` int DEFAULT NULL COMMENT '执行耗时',
  `first_executor_time` datetime DEFAULT NULL COMMENT '首次执行时间',
  `is_del` int NOT NULL DEFAULT 0 COMMENT '删除标记(未删除=0，删除=1)',
  `out_order_no` varchar(128) DEFAULT NULL COMMENT '外部订单号',
  `path` text COMMENT '任务完整路径和分段信息 JSON，节点编码使用地图 instanceName',
  `map_version_id` bigint DEFAULT NULL COMMENT '任务实际使用的地图版本 ID，首次调度规划时确定并固定',
  `periodic_task` int NOT NULL DEFAULT 0 COMMENT '是否周期任务',
  `priority` int NOT NULL DEFAULT 0 COMMENT '优先级',
  `root_task_record_id` bigint DEFAULT NULL COMMENT '根任务记录 ID',
  PRIMARY KEY (`id`),
  KEY `idx_t_windtaskrecord_def_id` (`def_id`),
  KEY `idx_t_windtaskrecord_def_label` (`def_label`),
  KEY `idx_t_windtaskrecord_status` (`status`),
  KEY `idx_t_windtaskrecord_agv_id` (`agv_id`),
  KEY `idx_t_windtaskrecord_created_on` (`created_on`),
  KEY `idx_t_windtaskrecord_out_order_no` (`out_order_no`),
  KEY `idx_t_windtaskrecord_map_version_id` (`map_version_id`),
  KEY `idx_t_windtaskrecord_root_task_record_id` (`root_task_record_id`),
  KEY `idx_t_windtaskrecord_priority_status` (`priority`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务执行记录表';

CREATE TABLE `t_windblockrecord` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `block_config_id` bigint DEFAULT NULL COMMENT '流程块配置 ID',
  `block_id` varchar(64) DEFAULT NULL COMMENT '流程块 ID',
  `parent_block_id` varchar(64) DEFAULT NULL COMMENT '父流程块 ID，RootBp 为空，CAgvOperationBp 指向所属 RootBp',
  `block_input_params_value` longtext COMMENT '流程块输入参数值',
  `block_name` varchar(255) DEFAULT NULL COMMENT '流程块名称',
  `ended_on` datetime DEFAULT NULL COMMENT '结束时间',
  `ended_reason` longtext COMMENT '结束原因',
  `internal_variables` longtext COMMENT '内部变量',
  `order_id` varchar(128) DEFAULT NULL COMMENT '订单 ID',
  `output_params` longtext COMMENT '输出参数',
  `started_on` datetime DEFAULT NULL COMMENT '开始时间',
  `status` int DEFAULT NULL COMMENT '流程块状态',
  `task_record_id` bigint DEFAULT NULL COMMENT '任务记录 ID',
  `version` int NOT NULL DEFAULT 1 COMMENT '版本号',
  PRIMARY KEY (`id`),
  KEY `idx_t_windblockrecord_block_id` (`block_id`),
  KEY `idx_t_windblockrecord_parent_block_id` (`parent_block_id`),
  KEY `idx_t_windblockrecord_block_name` (`block_name`),
  KEY `idx_t_windblockrecord_status` (`status`),
  KEY `idx_t_windblockrecord_task_record_id` (`task_record_id`),
  KEY `idx_t_windblockrecord_order_id` (`order_id`),
  KEY `idx_t_windblockrecord_started_on` (`started_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='流程块执行记录表';

CREATE TABLE `t_windtasklog` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `level` varchar(255) DEFAULT NULL COMMENT '日志级别',
  `message` longtext COMMENT '日志内容',
  `task_block_id` int DEFAULT NULL COMMENT '任务块 ID',
  `task_id` bigint DEFAULT NULL COMMENT '任务 ID',
  `task_record_id` bigint DEFAULT NULL COMMENT '任务记录 ID',
  PRIMARY KEY (`id`),
  KEY `idx_t_windtasklog_task_record_id` (`task_record_id`),
  KEY `idx_t_windtasklog_task_id` (`task_id`),
  KEY `idx_t_windtasklog_task_block_id` (`task_block_id`),
  KEY `idx_t_windtasklog_level` (`level`),
  KEY `idx_t_windtasklog_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务日志表';

CREATE TABLE `t_alarmsrecord` (
  `id` bigint NOT NULL COMMENT '主键 ID',
  `alarms_code` varchar(255) DEFAULT NULL COMMENT '报警编码',
  `alarms_cost_time` decimal(19,2) DEFAULT NULL COMMENT '报警持续时长',
  `alarms_desc` varchar(1024) DEFAULT NULL COMMENT '报警描述',
  `ended_on` datetime DEFAULT NULL COMMENT '结束时间',
  `level` varchar(255) DEFAULT NULL COMMENT '报警级别',
  `started_on` datetime DEFAULT NULL COMMENT '开始时间',
  `type` int DEFAULT NULL COMMENT '报警记录类型',
  `vehicle_id` varchar(64) DEFAULT NULL COMMENT '车辆 ID',
  PRIMARY KEY (`id`),
  KEY `idx_t_alarmsrecord_vehicle_id` (`vehicle_id`),
  KEY `idx_t_alarmsrecord_level` (`level`),
  KEY `idx_t_alarmsrecord_type` (`type`),
  KEY `idx_t_alarmsrecord_started_on` (`started_on`),
  KEY `idx_t_alarmsrecord_ended_on` (`ended_on`),
  KEY `idx_t_alarmsrecord_alarms_code` (`alarms_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='报警记录表';
