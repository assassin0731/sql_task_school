DROP TABLE IF EXISTS date_of_analysis_formation CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS sku CASCADE;
DROP TABLE IF EXISTS groups_sku CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS personal_data CASCADE;
DROP PROCEDURE IF EXISTS import_all_tables;
DROP PROCEDURE IF EXISTS pr_import_data_from_file;
DROP PROCEDURE IF EXISTS pr_export_all_tables;
DROP PROCEDURE IF EXISTS pr_export_data_to_file;
DROP VIEW IF EXISTS periods;
DROP VIEW IF EXISTS purchase_history_v;
DROP VIEW IF EXISTS groups;

SET datestyle = "ISO, DMY";

-- Таблица Персональные данные
CREATE TABLE personal_data (
    "Customer_ID" SERIAL PRIMARY KEY,
    "Customer_Name" VARCHAR NOT NULL,
    "Customer_Surname" VARCHAR NOT NULL,
    "Customer_Primary_Email" VARCHAR NOT NULL,
    "Customer_Primary_Phone" VARCHAR NOT NULL
);

-- Таблица Карты
CREATE TABLE cards (
    "Customer_Card_ID" SERIAL PRIMARY KEY,
    "Customer_ID" INTEGER NOT NULL,
    CONSTRAINT fk_cards_customer_id FOREIGN KEY ("Customer_ID") REFERENCES personal_data("Customer_ID")
);

--Таблица Группы SKU
CREATE TABLE groups_sku (
    "Group_ID" SERIAL PRIMARY KEY,
    "Group_Name" VARCHAR NOT NULL
);

--Таблица Товарная матрица
CREATE TABLE sku (
    "SKU_ID" SERIAL PRIMARY KEY,
    "SKU_Name" VARCHAR NOT NULL,
    "Group_ID" INTEGER NOT NULL,
    CONSTRAINT fk_sku_group_id FOREIGN KEY ("Group_ID") REFERENCES groups_sku("Group_ID")
);

-- Таблица Торговые точки
CREATE TABLE stores (
    "Transaction_Store_ID" INTEGER NOT NULL,
    "SKU_ID" INTEGER NOT NULL,
    "SKU_Purchase_Price" NUMERIC NOT NULL,
    "SKU_Retail_Price" NUMERIC NOT NULL,
    CONSTRAINT fk_stores_sku_id FOREIGN KEY ("SKU_ID") REFERENCES sku("SKU_ID")
);

-- Таблица Транзакции
CREATE TABLE transactions (
    "Transaction_ID" SERIAL PRIMARY KEY,
    "Customer_Card_ID" INTEGER NOT NULL,
    "Transaction_Summ" NUMERIC NOT NULL,
    "Transaction_DateTime" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "Transaction_Store_ID" INTEGER NOT NULL,
    CONSTRAINT fk_transactions_customer_card_id FOREIGN KEY ("Customer_Card_ID") REFERENCES cards("Customer_Card_ID")
);
--    CONSTRAINT fk_transactions_transaction_store_id FOREIGN KEY ("Transaction_Store_ID") REFERENCES stores("Transaction_Store_ID")

CREATE TABLE checks(
    "Transaction_ID" INTEGER NOT NULL,
    "SKU_ID" INTEGER NOT NULL,
    "SKU_Amount" NUMERIC NOT NULL,
    "SKU_Summ" NUMERIC NOT NULL,
    "SKU_Summ_Paid" NUMERIC NOT NULL,
    "SKU_Discount" NUMERIC NOT NULL,
    CONSTRAINT fk_checks_transaction_id FOREIGN KEY ("Transaction_ID") REFERENCES transactions("Transaction_ID"),
    CONSTRAINT fk_checks_sku_id FOREIGN KEY ("SKU_ID") REFERENCES sku("SKU_ID")
);

CREATE TABLE date_of_analysis_formation ( "Analysis_Formation" TIMESTAMP DEFAULT CURRENT_TIMESTAMP );

--------
--Импорт
--------

-- Процедура для импорта данных из файла в указанную таблицу с заданным разделителем
CREATE OR REPLACE PROCEDURE pr_import_data_from_file(
    tables_name TEXT,
    file_path TEXT,
    delimiter TEXT DEFAULT ','
)
LANGUAGE plpgsql
AS $$
DECLARE
    file_extension TEXT;
    column_list TEXT;
BEGIN
    file_extension := lower(substring(file_path from '\.(\w+)$'));

    IF file_extension = 'tsv' THEN
        delimiter := E'\t';
    END IF;

    EXECUTE format('COPY %I FROM %L WITH (FORMAT csv, DELIMITER %L, HEADER false)',
                   tables_name, file_path, delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_import_all_tables(project_source TEXT)
LANGUAGE PLPGSQL
AS $$
BEGIN
    -- CALL pr_import_data_from_file('personal_data', project_source || 'datasets/Personal_Data_.tsv');
    -- CALL pr_import_data_from_file('cards', project_source || 'datasets/Cards.tsv');
    -- CALL pr_import_data_from_file('groups_sku', project_source || 'datasets/Groups_SKU.tsv');
    -- CALL pr_import_data_from_file('sku', project_source || 'datasets/SKU.tsv');
    -- CALL pr_import_data_from_file('stores', project_source || 'datasets/Stores.tsv');
    -- CALL pr_import_data_from_file('transactions', project_source || 'datasets/Transactions.tsv');
    -- CALL pr_import_data_from_file('checks', project_source || 'datasets/Checks.tsv');
    -- CALL pr_import_data_from_file('date_of_analysis_formation', project_source || 'datasets/Date_Of_Analysis_Formation.tsv');
    CALL pr_import_data_from_file('personal_data', project_source || 'datasets/Personal_Data_Mini.tsv');
    CALL pr_import_data_from_file('cards', project_source || 'datasets/Cards_Mini.tsv');
    CALL pr_import_data_from_file('groups_sku', project_source || 'datasets/Groups_SKU_Mini.tsv');
    CALL pr_import_data_from_file('sku', project_source || 'datasets/SKU_Mini.tsv');
    CALL pr_import_data_from_file('stores', project_source || 'datasets/Stores_Mini.tsv');
    CALL pr_import_data_from_file('transactions', project_source || 'datasets/Transactions_Mini.tsv');
    CALL pr_import_data_from_file('checks', project_source || 'datasets/Checks_Mini.tsv');
    CALL pr_import_data_from_file('date_of_analysis_formation', project_source || 'datasets/Date_Of_Analysis_Formation.tsv');
END;
$$;


--------
--Экспорт
--------

-- Процедура для экспорта данных в файл из указанной таблицы с заданным разделителем
CREATE OR REPLACE PROCEDURE pr_export_data_to_file(
    tables_name TEXT,
    file_path TEXT,
    delimiter TEXT DEFAULT ','
)
LANGUAGE plpgsql
AS $$
DECLARE
    file_extension TEXT;
BEGIN
    file_extension := lower(substring(file_path from '\.(\w+)$'));

    IF file_extension = 'tsv' THEN
        delimiter := E'\t';
    END IF;

    EXECUTE format('COPY %I TO %L WITH (FORMAT csv, DELIMITER %L, HEADER false)',
                   tables_name, file_path, delimiter);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_export_all_tables(project_source TEXT)
LANGUAGE PLPGSQL
AS $$
BEGIN
    CALL pr_export_data_to_file('personal_data', project_source || 'Personal_Data_Mini.tsv');
    CALL pr_export_data_to_file('cards', project_source || 'Cards_Mini.tsv');
    CALL pr_export_data_to_file('groups_sku', project_source || 'Groups_SKU_Mini.tsv');
    CALL pr_export_data_to_file('sku', project_source || 'SKU_Mini.tsv');
    CALL pr_export_data_to_file('stores', project_source || 'Stores_Mini.tsv');
    CALL pr_export_data_to_file('transactions', project_source || 'Transactions_Mini.tsv');
    CALL pr_export_data_to_file('checks', project_source || 'Checks_Mini.tsv');
    CALL pr_export_data_to_file('date_of_analysis_formation', project_source || 'Date_Of_Analysis_Formation.tsv');
END;
$$;



--------
--Проверка работы
--------
SET path_to_project.var TO '/tmp/';

-- Импортировать tsv из materials (mini)
SET path_to_project.var TO '/Users/vladimir/s21_school/1_SQL_Bootcamp/SQL3_RetailAnalitycs_v1.0-1/';
SET datestyle = "ISO, DMY";

CALL pr_import_all_tables(current_setting('path_to_project.var'));

-- Экспортировать tsv в export
SET path_to_project.var TO '/Users/vladimir/s21_school/1_SQL_Bootcamp/SQL3_RetailAnalitycs_v1.0-1/src/export/';
SET datestyle = "ISO, DMY";

CALL pr_export_all_tables(current_setting('path_to_project.var'));
