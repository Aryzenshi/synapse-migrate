CREATE DATABASE IF NOT EXISTS shop CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE shop;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email VARCHAR(128) NOT NULL,
  display_name VARCHAR(128) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'PLACED',
  total DECIMAL(18,2) NOT NULL DEFAULT 0.00,
  placed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY ix_orders_user_status (user_id, status),
  CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS order_items (
  order_id BIGINT UNSIGNED NOT NULL,
  line_no INT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  qty INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  PRIMARY KEY (order_id, line_no),
  KEY ix_order_items_sku (sku),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
  CONSTRAINT chk_items_qty CHECK (qty >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tags (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_tags_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS order_tags (
  order_id BIGINT UNSIGNED NOT NULL,
  tag_id BIGINT UNSIGNED NOT NULL,
  position INT NOT NULL,
  PRIMARY KEY (order_id, tag_id),
  UNIQUE KEY uq_order_tags_position (order_id, position),
  CONSTRAINT fk_order_tags_order FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
  CONSTRAINT fk_order_tags_tag FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO users(id, email, display_name) VALUES
 (1, 'aarya.n@example.com', 'Aarya N'),
 (2, 'dev.k@example.com', 'Dev K');
INSERT IGNORE INTO orders(id, user_id, status, total) VALUES
 (1, 1, 'PLACED', 998.00), (2, 1, 'SHIPPED', 49.50), (3, 2, 'PLACED', 100.00);
INSERT IGNORE INTO order_items(order_id, line_no, sku, qty, unit_price) VALUES
 (1,1,'SKU-1042',2,499.00), (1,2,'SKU-31',1,0.00), (2,1,'SKU-12',3,16.50), (3,1,'SKU-4',1,100.00);
INSERT IGNORE INTO tags(id, name) VALUES (1, 'sale'), (2, 'featured');
INSERT IGNORE INTO order_tags(order_id, tag_id, position) VALUES (1,1,0), (1,2,1), (2,1,0);
