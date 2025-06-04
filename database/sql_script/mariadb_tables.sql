-- MariaDB表结构创建脚本

-- 创建BuyPoint表
CREATE TABLE IF NOT EXISTS buypoint (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL COMMENT '股票名称',
    symbol VARCHAR(50) NOT NULL COMMENT '股票代码',
    ts_code VARCHAR(50) NOT NULL COMMENT '股票ts代码',
    freq VARCHAR(50) NOT NULL COMMENT '级别（日线、周线等）',
    signals VARCHAR(255) NULL COMMENT '第几类买点',
    fx_pwr VARCHAR(255) NULL COMMENT '分型强度',
    expect_profit FLOAT NULL COMMENT '预估收益比例（%）',
    industry VARCHAR(255) NULL COMMENT '板块',
    mark VARCHAR(255) NULL COMMENT '顶分型、底分型',
    high FLOAT NULL COMMENT '分型高点',
    low FLOAT NULL COMMENT '分型低点',
    date DATETIME NOT NULL COMMENT '买点检测到的时间，通常是分型的第三根K线',
    reason TEXT NULL COMMENT '买点原因',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_symbol (symbol),
    INDEX idx_date (date),
    INDEX idx_ts_code (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建ConceptName表
CREATE TABLE IF NOT EXISTS concept_name (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    name VARCHAR(100) NOT NULL COMMENT '板块名称',
    code VARCHAR(20) NOT NULL COMMENT '板块代码',
    rank INT NOT NULL COMMENT '排名',
    rise_ratio FLOAT NOT NULL COMMENT '涨跌比',
    up_count INT NOT NULL COMMENT '上涨家数',
    down_count INT NOT NULL COMMENT '下跌家数',
    timestamp DATETIME NOT NULL COMMENT '数据插入时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_code (code),
    INDEX idx_timestamp (timestamp),
    INDEX idx_rank (rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建ConceptCons表
CREATE TABLE IF NOT EXISTS concept_cons (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    name VARCHAR(100) NOT NULL COMMENT '板块名称',
    code VARCHAR(20) NOT NULL COMMENT '板块代码',
    symbol VARCHAR(20) NOT NULL COMMENT '股票代码',
    stock_name VARCHAR(100) NOT NULL COMMENT '股票名称',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_code_symbol (code, symbol) COMMENT '唯一性索引',
    INDEX idx_code (code),
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;