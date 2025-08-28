CREATE DATABASE `personnal_finance_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE personnal_finance_db;

-- ======================================
-- TABLES DE BASE : BANQUES, DEVISES, COMPTES
-- ======================================

CREATE TABLE banks (
  id INT PRIMARY KEY,  -- identifiant externe (code banque)
  name VARCHAR(128) NOT NULL,
  country VARCHAR(64) NOT NULL DEFAULT 'FR'
);

CREATE TABLE currency (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(16) NOT NULL,
  abbreviation VARCHAR(4) NOT NULL,
  symbol VARCHAR(4) NOT NULL
);

CREATE TABLE account_type (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(45) NOT NULL,
  is_checking_account BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE accounts (
  id BIGINT PRIMARY KEY,  -- identifiant OFX
  name VARCHAR(128) NOT NULL,
  abbreviation VARCHAR(16),
  account_type_id INT NOT NULL,
  currency_id INT NOT NULL,
  bank_id INT,
  FOREIGN KEY (account_type_id) REFERENCES account_type(id),
  FOREIGN KEY (currency_id) REFERENCES currency(id),
  FOREIGN KEY (bank_id) REFERENCES banks(id)
);

-- ======================================
-- SOLDE, TRANSACTIONS, CATEGORIES
-- ======================================

CREATE TABLE balances (
  id INT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  date DATE NOT NULL,
  value DECIMAL(10,2) NOT NULL,
  UNIQUE(account_id, date),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE categories (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  parent_id INT,
  image_url TEXT,
  FOREIGN KEY (parent_id) REFERENCES categories(id)
);

CREATE TABLE transactions (
  id BIGINT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  date DATE NOT NULL,
  payee VARCHAR(256),
  clean_payee VARCHAR(128),
  memo VARCHAR(256),
  amount DECIMAL(10,2) NOT NULL,
  user_tag_category_id INT,
  user_ignore_transaction BOOLEAN NOT NULL DEFAULT FALSE,
  is_expense BOOLEAN NOT NULL,
  FOREIGN KEY (account_id) REFERENCES accounts(id),
  FOREIGN KEY (user_tag_category_id) REFERENCES categories(id)
);

CREATE TABLE categories_transaction_link (
  id INT AUTO_INCREMENT PRIMARY KEY,
  payee VARCHAR(256) NOT NULL,
  is_expense BOOLEAN NOT NULL,
  category_id INT NOT NULL,
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- ======================================
-- MOYENS DE PAIEMENT
-- ======================================

CREATE TABLE payment_methods (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE payment_methods_transaction_link (
  id INT AUTO_INCREMENT PRIMARY KEY,
  payment_method_id INT NOT NULL,
  transaction_memo_pattern VARCHAR(128) NOT NULL,
  FOREIGN KEY (payment_method_id) REFERENCES payment_methods(id)
);

-- ======================================
-- TRANSACTIONS PLANIFIÉES
-- ======================================

CREATE TABLE planned_transactions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  is_expense BOOLEAN NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  maximum_amount DECIMAL(10,2),
  start_date DATE NOT NULL,
  due_day INT NOT NULL,
  end_date DATE,
  frequency VARCHAR(32) NOT NULL,
  payee VARCHAR(128)
);

-- ======================================
-- PRÊTS ET REMBOURSEMENTS ANTICIPÉS
-- ======================================

CREATE TABLE loan (
  id INT AUTO_INCREMENT PRIMARY KEY,
  description VARCHAR(128) NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  currency_id INT NOT NULL,
  is_flat_rate BOOLEAN NOT NULL,
  flat_rate_value FLOAT DEFAULT NULL,
  subscription_date DATE NOT NULL,
  first_installment_date DATE NOT NULL,
  total_month_duration INT NOT NULL,
  bank_loan_id BIGINT,
  FOREIGN KEY (currency_id) REFERENCES currency(id)
);

CREATE TABLE loan_early_payments (
  id INT AUTO_INCREMENT PRIMARY KEY,
  loan_id INT NOT NULL,
  date DATE NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  FOREIGN KEY (loan_id) REFERENCES loan(id)
);

-- ======================================
-- BOURSE : TITRES, OPÉRATIONS, COURS
-- ======================================

CREATE TABLE securities (
  isin VARCHAR(12) PRIMARY KEY,
  ticker VARCHAR(32) NOT NULL,
  name VARCHAR(128) NOT NULL,
  type VARCHAR(32) NOT NULL,
  currency_id INT NOT NULL,
  market VARCHAR(64),
  FOREIGN KEY (currency_id) REFERENCES currency(id)
);

CREATE TABLE security_operations (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  date DATE NOT NULL,
  isin VARCHAR(12) NOT NULL,
  operation_type ENUM('purchase', 'sale', 'tax') NOT NULL,
  quantity INT NOT NULL,
  net_amount DECIMAL(10,4) NOT NULL,
  gross_amount DECIMAL(10,4) NOT NULL,
  net_unit_price DECIMAL(10,4) NOT NULL,
  gross_unit_price DECIMAL(10,4),
  fees DECIMAL(10,2) DEFAULT 0,
  account_id BIGINT NOT NULL,
  FOREIGN KEY (isin) REFERENCES securities(isin),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE security_prices (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  isin VARCHAR(12) NOT NULL,
  date DATE NOT NULL,
  open_price DECIMAL(10,4),
  close_price DECIMAL(10,4),
  high DECIMAL(10,4),
  low DECIMAL(10,4),
  volume BIGINT,
  UNIQUE(isin, date),
  FOREIGN KEY (isin) REFERENCES securities(isin)
);
