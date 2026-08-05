-- 已存在的 t_windblockrecord 表执行一次即可。
-- 如果数据库是全新初始化的，直接使用 rds.sql，无需执行本脚本。
ALTER TABLE `t_windblockrecord`
  DROP INDEX `idx_t_windblockrecord_task_id`,
  DROP COLUMN `block_input_params`,
  DROP COLUMN `block_internal_variables`,
  DROP COLUMN `input_params`,
  DROP COLUMN `task_id`;
