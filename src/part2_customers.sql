DROP VIEW customers_v;
CREATE OR REPLACE VIEW customers_v AS WITH cards_id AS (
        -- Составляем массив всех карт клиента
        SELECT pd."Customer_ID",
            array_agg(cards."Customer_Card_ID") AS cust_cards
        FROM personal_data AS pd
            JOIN cards ON pd."Customer_ID" = cards."Customer_ID"
        GROUP BY pd."Customer_ID"
    ),
    with_summ AS (
        -- Суммируем транзакции по всем картам клиента и делим на кол-во транзакций, получаем средний чек
        SELECT *,
            ROW_NUMBER() OVER() AS percent
        FROM(
                SELECT "Customer_ID",
                    SUM("Transaction_Summ") / count("Transaction_Summ") AS summa
                FROM cards_id
                    JOIN transactions AS t ON t."Customer_Card_ID" = ANY(cust_cards)
                GROUP BY "Customer_ID"
                ORDER BY summa DESC
            ) AS table1
    ),
    avg_check AS (
        -- Определяем сегмент по среднему чеку
        SELECT "Customer_ID",
            summa AS "Customer_Average_Check",
            CASE
                WHEN percent::NUMERIC /(
                    SELECT MAX(percent)
                    FROM with_summ
                ) <= 0.1 THEN 'Высокий'
                WHEN percent::NUMERIC /(
                    SELECT MAX(percent)
                    FROM with_summ
                ) <= 0.35 THEN 'Средний'
                ELSE 'Низкий'
            END AS "Customer_Average_Check_Segment"
        FROM with_summ
    ),
    dates_count AS (
        -- Берём первую и последнюю транзакцию клиента, считаем кол-во дней между ними, 
        -- считаем сколько всего было у него транзакций, делим первое и второй и получаем частоту транзакций
        SELECT *,
            ROW_NUMBER() OVER() AS percent
        FROM(
                SELECT "Customer_ID",
                    (
                        SELECT EXTRACT(
                                epoch
                                FROM(
                                        MAX("Transaction_DateTime") - MIN("Transaction_DateTime")
                                    )
                            ) /(60 * 60 * 24 * count(*))
                        FROM transactions AS t
                            JOIN cards_id ON t."Customer_Card_ID" = ANY(cards_id.cust_cards)
                        WHERE cards_id."Customer_ID" = c_i."Customer_ID"
                    ) AS "Customer_Frequency"
                FROM cards_id c_i
                ORDER BY "Customer_Frequency"
            ) AS table1
    ),
    avg_trans AS (
        -- Определяем сегмент по частоте тарзакций
        SELECT "Customer_ID",
            "Customer_Frequency",
            CASE
                WHEN percent::NUMERIC /(
                    SELECT MAX(percent)
                    FROM dates_count
                ) <= 0.1 THEN 'Часто'
                WHEN percent::NUMERIC /(
                    SELECT MAX(percent)
                    FROM dates_count
                ) <= 0.35 THEN 'Средне'
                ELSE 'Редко'
            END AS "Customer_Frequency_Segment"
        FROM dates_count
    ),
    after_last_trans AS (
        -- сколько прошло дней после последней транзакции клиента до заданного момента
        SELECT "Customer_ID",
            (
                SELECT (
                        SELECT EXTRACT(
                                epoch
                                FROM (
                                        (
                                            SELECT *
                                            FROM date_of_analysis_formation
                                        ) - MAX("Transaction_DateTime")
                                    )
                            ) /(60 * 60 * 24)
                        FROM transactions AS t
                            JOIN cards_id ON t."Customer_Card_ID" = ANY(cards_id.cust_cards)
                        WHERE cards_id."Customer_ID" = c_i."Customer_ID"
                    )
            ) AS "Customer_Churn"
        FROM cards_id c_i
    ),
    churn_rate AS (
        -- Определяем коэффициент оттока(кол-во дней после последней на частоту транзакций клиента)
        SELECT alt."Customer_ID",
            "Customer_Churn" / "Customer_Frequency" AS "Customer_Churn_Rate",
            "Customer_Churn",
            CASE
                WHEN "Customer_Churn" / "Customer_Frequency" <= 2 THEN 'Низкая'
                WHEN "Customer_Churn" / "Customer_Frequency" <= 5 THEN 'Средняя'
                ELSE 'Высокая'
            END AS "Customer_Churn_Segment"
        FROM after_last_trans AS alt
            JOIN dates_count AS dc on alt."Customer_ID" = dc."Customer_ID"
    ),
    check_seg AS (
        SELECT unnest(ARRAY ['Низкий', 'Средний', 'Высокий']) AS check_
    ),
    buy_seg AS (
        SELECT unnest(ARRAY ['Редко', 'Средне', 'Часто']) AS buy
    ),
    churn_seg AS (
        SELECT unnest(ARRAY ['Низкая', 'Средняя', 'Высокая']) AS churn
    ),
    req_table AS (
        -- таблица сегментов клиентов
        SELECT ROW_NUMBER() OVER() AS id,
            check_,
            buy,
            churn
        FROM check_seg,
            buy_seg,
            churn_seg
    ),
    num_seg AS (
        --определяем номер сегмента клиента(3 его сегмента сравниваем с сегментами из таблицы сегментов и выдаем номер)
        SELECT avg_check."Customer_ID",
            "Customer_Average_Check_Segment",
            "Customer_Frequency_Segment",
            "Customer_Churn_Segment",
            req_table.id AS "Customer_Segment"
        FROM avg_check
            JOIN avg_trans on avg_check."Customer_ID" = avg_trans."Customer_ID"
            JOIN churn_rate on avg_check."Customer_ID" = churn_rate."Customer_ID"
            CROSS JOIN req_table
        WHERE req_table.check_ = avg_check."Customer_Average_Check_Segment"
            AND req_table.buy = avg_trans."Customer_Frequency_Segment"
            AND req_table.churn = churn_rate."Customer_Churn_Segment"
    ),
    store_id AS (
        -- таблица всех транзакций клиента
        SELECT "Customer_ID",
            "Transaction_Store_ID",
            "Transaction_DateTime",
            "Transaction_ID"
        FROM cards_id
            JOIN transactions AS t ON t."Customer_Card_ID" = ANY(cust_cards)
    ),
    store_percent AS (
        -- таблица доли транзакций в каждом магазине для клиента
        SELECT "Customer_ID",
            "Transaction_Store_ID",
            count(*)::NUMERIC / (
                SELECT count
                FROM (
                        SELECT "Customer_ID",
                            count(*)
                        FROM store_ID
                        GROUP BY "Customer_ID"
                    ) AS table1
                WHERE table1."Customer_ID" = store_id."Customer_ID"
            ) AS percent
        FROM store_id
        GROUP BY "Customer_ID",
            "Transaction_Store_ID"
    ),
    all_trans AS (
        -- транзакции "сгруппирова" по id клиента
        SELECT "Customer_ID",
            "Transaction_Store_ID",
            "Transaction_DateTime",
            "Transaction_ID",
            ROW_NUMBER() OVER(
                PARTITION BY "Customer_ID"
                ORDER BY "Transaction_DateTime" DESC
            ) AS ordered
        FROM store_id
    ),
    last_trans AS (
        -- берем последние 3 транзакции клиента + группируем по магазинам
        SELECT "Customer_ID",
            "Transaction_Store_ID",
            percent,
            "Transaction_ID",
            "Transaction_DateTime"
        FROM (
                SELECT t."Customer_ID",
                    t."Transaction_Store_ID",
                    percent,
                    "Transaction_ID",
                    "Transaction_DateTime",
                    ROW_NUMBER() OVER(
                        PARTITION BY t."Customer_ID",
                        t."Transaction_Store_ID",
                        percent
                        ORDER BY "Transaction_DateTime" DESC
                    ) AS ordered
                FROM all_trans AS t
                    JOIN store_percent ON t."Customer_ID" = store_percent."Customer_ID"
                WHERE t.ordered <= 3
                    AND t."Transaction_Store_ID" = store_percent."Transaction_Store_ID"
                ORDER BY t."Customer_ID"
            ) AS table1
        WHERE table1.ordered = 1
    ),
    last_trans_union AS (
        -- объединяем с таблицей всех долей транзакций, чтобы учесть те магазины, которые не попали в последние 3 транзакции
        SELECT *
        FROM last_trans
        UNION
        SELECT *,
            null,
            null
        FROM store_percent
        ORDER BY "Customer_ID",
            "Transaction_Store_ID",
            "Transaction_DateTime"
    ),
    group_cust AS (
        -- избавляемся от добавленных в предыдущей таблице строк магазинов, если транзакция в этом магазине входила в последние 3
        SELECT "Customer_ID",
            "Transaction_Store_ID",
            percent,
            "Transaction_DateTime"
        FROM (
                SELECT "Customer_ID",
                    "Transaction_Store_ID",
                    percent,
                    "Transaction_DateTime",
                    ROW_NUMBER() OVER(
                        PARTITION BY "Customer_ID",
                        "Transaction_Store_ID",
                        percent
                        ORDER BY "Transaction_DateTime"
                    ) AS ordered
                FROM last_trans_union
            ) AS table1
        WHERE table1.ordered = 1
        ORDER BY "Customer_ID",
            percent DESC,
            COALESCE("Transaction_DateTime", '1900-01-01') DESC
    ),
    store_result AS (
        -- магазин для каждого клиента
        SELECT "Customer_ID",
            CASE
                WHEN (
                    SELECT count(*)
                    FROM last_trans AS lt
                    WHERE lt."Customer_ID" = pd."Customer_ID"
                    GROUP BY "Customer_ID"
                ) = 1 THEN (
                    SELECT "Transaction_Store_ID"
                    FROM last_trans AS lt
                    WHERE lt."Customer_ID" = pd."Customer_ID"
                )
                ELSE (
                    SELECT "Transaction_Store_ID"
                    FROM (
                            SELECT "Customer_ID",
                                "Transaction_Store_ID",
                                ROW_NUMBER() OVER(PARTITION BY "Customer_ID") AS ordered
                            FROM group_cust
                            WHERE group_cust."Customer_ID" = pd."Customer_ID"
                        ) AS table1
                    WHERE table1.ordered = 1
                )
            END
        FROM personal_data AS pd
    )
SELECT avg_check."Customer_ID",
    "Customer_Average_Check"::REAL,
    avg_check."Customer_Average_Check_Segment",
    "Customer_Frequency"::REAL,
    avg_trans."Customer_Frequency_Segment",
    "Customer_Churn"::REAL AS "Customer_Inactive_Period",
    "Customer_Churn_Rate"::REAL,
    churn_rate."Customer_Churn_Segment",
    "Customer_Segment",
    store_result."Transaction_Store_ID" AS "Customer_Primary_Store"
FROM avg_check
    JOIN avg_trans ON avg_check."Customer_ID" = avg_trans."Customer_ID"
    JOIN churn_rate ON avg_check."Customer_ID" = churn_rate."Customer_ID"
    JOIN num_seg ON avg_check."Customer_ID" = num_seg."Customer_ID"
    JOIN store_result ON avg_check."Customer_ID" = store_result."Customer_ID";
;
SELECT *
FROM customers_v;
