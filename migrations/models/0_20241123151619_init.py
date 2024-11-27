from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE `role` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `created_time` DATETIME(6) NOT NULL  COMMENT '创建时间' DEFAULT CURRENT_TIMESTAMP(6),
    `modified_time` DATETIME(6) NOT NULL  COMMENT '更新时间' DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    `role_name` VARCHAR(255) NOT NULL UNIQUE COMMENT '角色名称',
    `role_status` BOOL NOT NULL  COMMENT 'True： 启用 False: 禁用' DEFAULT 0,
    `role_desc` VARCHAR(255),
    `permissions` JSON NOT NULL
) CHARACTER SET utf8mb4 COMMENT='角色表';
CREATE TABLE `role_binding` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `created_time` DATETIME(6) NOT NULL  COMMENT '创建时间' DEFAULT CURRENT_TIMESTAMP(6),
    `modified_time` DATETIME(6) NOT NULL  COMMENT '更新时间' DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    `user_id` VARCHAR(50) NOT NULL,
    `role_id` INT NOT NULL,
    CONSTRAINT `fk_role_bin_role_dbe7775f` FOREIGN KEY (`role_id`) REFERENCES `role` (`id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='用户角色绑定表';
CREATE TABLE `user` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `created_time` DATETIME(6) NOT NULL  COMMENT '创建时间' DEFAULT CURRENT_TIMESTAMP(6),
    `modified_time` DATETIME(6) NOT NULL  COMMENT '更新时间' DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    `username` VARCHAR(128) NOT NULL  COMMENT '用户名',
    `password` VARCHAR(255) NOT NULL  COMMENT '密码',
    `email` VARCHAR(255)   COMMENT '邮箱',
    `is_active` BOOL NOT NULL  COMMENT '用户状态' DEFAULT 0,
    `is_superuser` BOOL NOT NULL  COMMENT '是否是超级用户' DEFAULT 0,
    `phone` VARCHAR(30) NOT NULL  COMMENT '手机号码'
) CHARACTER SET utf8mb4 COMMENT='用户表';
CREATE TABLE `aerich` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `version` VARCHAR(255) NOT NULL,
    `app` VARCHAR(100) NOT NULL,
    `content` JSON NOT NULL
) CHARACTER SET utf8mb4;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """
