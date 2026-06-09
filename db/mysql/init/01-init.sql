CREATE DATABASE IF NOT EXISTS analysis DEFAULT CHARACTER SET utf8mb4;

USE analysis;

-- 系统用户表（登录用）
CREATE TABLE IF NOT EXISTS sys_user (
    user_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(50) NOT NULL UNIQUE,
    password    VARCHAR(100) NOT NULL,
    nickname    VARCHAR(50),
    email       VARCHAR(100),
    status      CHAR(1) DEFAULT '0',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 插入默认管理员
INSERT INTO sys_user (username, password, nickname) VALUES
('admin', '$2a$10$7JB720yubVSZvUI0rEqK/.VqGOZTH.ulu33dHOiBEzByNZdLUMs42', '管理员')
ON DUPLICATE KEY UPDATE username=username;
-- 密码是 admin123 的 BCrypt 哈希，后续可用 Spring Security 校验

-- 菜单表
CREATE TABLE IF NOT EXISTS sys_menu (
    menu_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    menu_name   VARCHAR(50) NOT NULL,
    parent_id   BIGINT DEFAULT 0,
    path        VARCHAR(200),
    component   VARCHAR(200),
    perms       VARCHAR(100),
    icon        VARCHAR(50),
    sort        INT DEFAULT 0,
    visible     CHAR(1) DEFAULT '0',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 插入基础菜单
INSERT INTO sys_menu (menu_name, parent_id, path, component, perms, icon, sort) VALUES
('实时大屏', 0, '/dashboard', 'dashboard/index', 'dashboard:view', 'monitor', 1),
('PV分析',   0, '/analysis/pv', 'analysis/pv', 'analysis:pv:view', 'chart', 2),
('UV分析',   0, '/analysis/uv', 'analysis/uv', 'analysis:uv:view', 'chart', 3),
('来源分析', 0, '/analysis/referer', 'analysis/referer', 'analysis:referer:view', 'search', 4),
('性能监控', 0, '/analysis/performance', 'analysis/performance', 'analysis:performance:view', 'speed', 5),
('漏斗分析', 0, '/analysis/funnel', 'analysis/funnel', 'analysis:funnel:view', 'funnel', 6);
