CREATE TABLE `txn_history` (

  txn_id BIGINT(20) NOT NULL,
  user_id BIGINT(20) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  txn_type VARCHAR(64) NOT NULL,
  txn_state VARCHAR(64) NOT NULL DEFAULT 'NONE',
  txn_order_amount BIGINT(20) NOT NULL DEFAULT '0',
  txn_order_currency VARCHAR(64) NOT NULL DEFAULT 'JPY',
  txn_charge_amount BIGINT(20) NOT NULL DEFAULT '0',
  txn_charge_currency VARCHAR(64) NOT NULL DEFAULT 'JPY',
  txn_exchange_amount BIGINT(20) NOT NULL DEFAULT '0',
  txn_exchange_currency VARCHAR(64) NOT NULL DEFAULT 'JPY',
  txn_promo_amount BIGINT(20) NOT NULL DEFAULT '0',
  txn_promo_currency VARCHAR(64) NOT NULL DEFAULT 'JPY',
  disabled INT NOT NULL DEFAULT '0',
  version INT NOT NULL DEFAULT '0',

  order_id BIGINT(20) NOT NULL,
  order_state VARCHAR(64) DEFAULT NULL,
  order_error_code VARCHAR(128) DEFAULT NULL,
  order_type VARCHAR(64) DEFAULT NULL,
  order_created_at DATETIME DEFAULT NULL,
  order_updated_at DATETIME DEFAULT NULL,
  order_version INT DEFAULT NULL,
  order_items JSON DEFAULT NULL,

  payment_id BIGINT(20) DEFAULT NULL,
  payment_created_at DATETIME DEFAULT NULL,
  payment_updated_at DATETIME DEFAULT NULL,
  payment_paid_at DATETIME DEFAULT NULL,
  payment_version INT DEFAULT NULL,
  payment_state VARCHAR(64) DEFAULT NULL,
  payment_error_code VARCHAR(128) DEFAULT NULL,
  comments JSON DEFAULT NULL,
  payment_methods BIT(32) DEFAULT NULL,
  sub_payments JSON DEFAULT NULL,

  cb_amount BIGINT(20) DEFAULT NULL,
  cb_state VARCHAR(64) DEFAULT NULL,
  cb_release_date DATETIME DEFAULT NULL,
  cb_created_at DATETIME DEFAULT NULL,
  cb_updated_at DATETIME DEFAULT NULL,
  cb_version INT DEFAULT NULL,

  merchant_id BIGINT(20) DEFAULT NULL,
  merchant_name VARCHAR(512) DEFAULT NULL,
  merchant_cat BIGINT(20) DEFAULT NULL,
  merchant_sub_cat BIGINT(20) DEFAULT NULL,
  store_id VARCHAR(256) DEFAULT NULL,
  store_name VARCHAR(512) DEFAULT NULL,
  pos_id VARCHAR(128) DEFAULT NULL,
  biller_id BIGINT(20) DEFAULT NULL,

  logo_url VARCHAR(2048) DEFAULT NULL,

  peer_id BIGINT(20) DEFAULT NULL,
  peer_name VARCHAR(512) DEFAULT NULL,

  device_id VARCHAR(512) DEFAULT NULL,
  extra_info JSON DEFAULT NULL,

  PRIMARY KEY (txn_id)

)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_transaction_history ON txn_history (user_id, order_created_at DESC);
CREATE UNIQUE INDEX idx_order_id ON txn_history (user_id, order_id);

CREATE TABLE `WORKER_NODE` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `host_name` VARCHAR(64) NOT NULL,
  `port` VARCHAR(64) NOT NULL ,
  `type` INT NOT NULL COMMENT 'node type: ACTUAL or CONTAINER',
  `launch_date` DATE NOT NULL,
  `modified` DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  `created` DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
