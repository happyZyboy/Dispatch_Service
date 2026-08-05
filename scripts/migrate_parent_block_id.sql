-- Existing databases created before the sequential RootBp change need this one-time migration.
ALTER TABLE `t_windblockrecord`
  ADD COLUMN `parent_block_id` varchar(64) DEFAULT NULL COMMENT '父流程块 ID，RootBp 为空，CAgvOperationBp 指向所属 RootBp' AFTER `block_id`,
  ADD KEY `idx_t_windblockrecord_parent_block_id` (`parent_block_id`);
