-- Tạo cơ sở dữ liệu mới (nếu chưa có)
CREATE DATABASE IF NOT EXISTS ai_price_analyzer;
USE ai_price_analyzer;

-- Tắt kiểm tra khóa ngoại tạm thời để tránh lỗi khi tạo bảng
SET FOREIGN_KEY_CHECKS = 0;

-- -----------------------------------------------------
-- 1. Table: Users
-- Lưu thông tin tài khoản người dùng
-- -----------------------------------------------------
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id CHAR(36) NOT NULL COMMENT 'UUID',
    email VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    role ENUM('USER', 'ADMIN') DEFAULT 'USER',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP NULL,
    PRIMARY KEY (user_id),
    UNIQUE INDEX email_UNIQUE (email ASC)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 2. Table: Subscriptions
-- Quản lý gói đăng ký (Lite vs Premium)
-- -----------------------------------------------------
DROP TABLE IF EXISTS subscriptions;
CREATE TABLE subscriptions (
    subscription_id CHAR(36) NOT NULL COMMENT 'UUID',
    user_id CHAR(36) NOT NULL,
    plan_type ENUM('LITE', 'PREMIUM') NOT NULL,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP NULL,
    status ENUM('ACTIVE', 'EXPIRED', 'CANCELLED') DEFAULT 'ACTIVE',
    PRIMARY KEY (subscription_id),
    INDEX idx_user_sub (user_id ASC),
    CONSTRAINT fk_subscriptions_users
        FOREIGN KEY (user_id)
        REFERENCES users (user_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 3. Table: Products
-- Danh mục sản phẩm chuẩn (Canonical Product Catalog)
-- -----------------------------------------------------
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id CHAR(36) NOT NULL COMMENT 'UUID',
    name VARCHAR(200) NOT NULL COMMENT 'VD: iPhone 13 Pro Max 128GB',
    brand VARCHAR(50) COMMENT 'Apple, Samsung, Dell...',
    model_series VARCHAR(100) COMMENT 'iPhone 13, XPS 13...',
    category VARCHAR(50) COMMENT 'Smartphone, Laptop, Tablet...',
    base_specs JSON COMMENT 'Lưu cấu hình chuẩn dạng JSON (Ram, Chip, Screen...)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id),
    INDEX idx_product_name (name ASC)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 4. Table: Active Listings
-- Tin đăng đã được làm sạch từ MongoDB, phục vụ hiển thị
-- -----------------------------------------------------
DROP TABLE IF EXISTS active_listings;
CREATE TABLE active_listings (
    listing_id CHAR(36) NOT NULL COMMENT 'UUID',
    product_id CHAR(36) NOT NULL,
    source_url TEXT,
    platform VARCHAR(50) COMMENT 'Shopee, Chotot, Nhattao...',
    price DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'VND',
    condition_rank VARCHAR(50) COMMENT 'Like New, 99%, Trầy xước...',
    battery_health INT COMMENT 'Lưu % pin nếu có',
    warranty_status VARCHAR(50),
    color VARCHAR(50),
    posted_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (listing_id),
    INDEX idx_listing_price (price ASC),
    INDEX idx_listing_condition (condition_rank ASC),
    CONSTRAINT fk_listings_products
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 5. Table: Price History
-- Dữ liệu tổng hợp để vẽ biểu đồ lịch sử (Data Warehousing nhẹ)
-- -----------------------------------------------------
DROP TABLE IF EXISTS price_history;
CREATE TABLE price_history (
    history_id CHAR(36) NOT NULL COMMENT 'UUID',
    product_id CHAR(36) NOT NULL,
    record_date DATE NOT NULL,
    avg_price DECIMAL(15, 2),
    min_price DECIMAL(15, 2),
    max_price DECIMAL(15, 2),
    listing_count INT COMMENT 'Số lượng tin dùng để tính toán',
    PRIMARY KEY (history_id),
    INDEX idx_history_date (record_date ASC),
    CONSTRAINT fk_history_products
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 6. Table: Price Forecasts
-- Dữ liệu dự đoán từ AI model
-- -----------------------------------------------------
DROP TABLE IF EXISTS price_forecasts;
CREATE TABLE price_forecasts (
    forecast_id CHAR(36) NOT NULL COMMENT 'UUID',
    product_id CHAR(36) NOT NULL,
    forecast_date DATE NOT NULL COMMENT 'Ngày dự đoán trong tương lai',
    predicted_price DECIMAL(15, 2),
    confidence_score DECIMAL(5, 2) COMMENT 'Độ tin cậy của AI (0-100%)',
    model_version VARCHAR(50) COMMENT 'Phiên bản model AI',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_id),
    CONSTRAINT fk_forecast_products
        FOREIGN KEY (product_id)
        REFERENCES products (product_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB;

-- Bật lại kiểm tra khóa ngoại
SET FOREIGN_KEY_CHECKS = 1;