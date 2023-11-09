DROP FUNCTION fnc_offerings;
CREATE OR REPLACE FUNCTION fnc_offerings(method INTEGER DEFAULT 1,
										 first_date DATE DEFAULT NULL,
										 last_date DATE DEFAULT NULL,
										 trans_count INTEGER DEFAULT NULL,
										 coefficient NUMERIC DEFAULT 1,
										 index_left NUMERIC DEFAULT 0,
										 max_trans_discont NUMERIC DEFAULT 0,
										 margin NUMERIC DEFAULT 0
										 )
RETURNS TABLE("Customer_ID" INTEGER, "Required_Check_Measure" numeric, "Group_Name" VARCHAR(255), "Offer_Discount_Depth" numeric) AS 
$$
DECLARE
	min_date DATE = (SELECT min("Transaction_DateTime") FROM transactions);
	max_date DATE = (SELECT max("Transaction_DateTime") FROM transactions);
BEGIN
	IF method NOT IN (1,2) THEN
		RAISE EXCEPTION 'Wrong method';
	END IF;
	IF method = 1 AND (first_date IS NULL OR last_date IS NULL) THEN
		RAISE EXCEPTION 'Some dates are not selected';
	END IF;
	IF method = 1 AND (first_date > last_date OR first_date > max_date OR last_date < min_date) THEN
		RAISE EXCEPTION 'Wrong dates';
	END IF;
	IF method = 2 AND trans_count IS NULL THEN
		RAISE EXCEPTION 'Transaction count is not selected';
	END IF;
	IF trans_count IS NOT NULL AND (first_date IS NOT NULL OR last_date IS NOT NULL) THEN
		RAISE EXCEPTION 'Wrong Parametrs';
	END IF;
	RETURN QUERY 
	WITH cards_id AS ( -- Составляем массив всех карт клиента
		SELECT pd."Customer_ID" AS id, array_agg(cards."Customer_Card_ID") AS cust_cards 
		FROM personal_data AS pd
		JOIN cards ON pd."Customer_ID" = cards."Customer_ID"
		GROUP BY pd."Customer_ID"
	),
	all_trans AS ( -- Таблица со всеми транзакциями клиента
		SELECT * FROM (
		SELECT id, t."Transaction_ID", "Transaction_DateTime", "Transaction_Summ", 
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY "Transaction_DateTime" DESC) AS trans_num
		FROM cards_id
		JOIN transactions AS t ON t."Customer_Card_ID" = ANY(cust_cards)
		JOIN checks ON checks."Transaction_ID" = t."Transaction_ID"
		JOIN sku ON checks."SKU_ID" = sku."SKU_ID"
		ORDER BY "Transaction_DateTime" DESC) AS t
		WHERE (trans_count IS NOT NULL AND trans_num <= trans_count) OR
		(first_date IS NOT NULL AND last_date IS NOT NULL AND "Transaction_DateTime" BETWEEN first_date AND last_date)
	),
	avg_margin AS (
		SELECT table1."Customer_ID", table1."Group_ID", (sum_group_summ - sum_group_cost) / sum_group_summ as avg_margin_
		FROM (
			SELECT ph."Customer_ID", ph."Group_ID", SUM(ph."Group_Cost") AS sum_group_cost,                  
			SUM(ph."Group_Summ") AS sum_group_summ, count(*) AS cnt        
			FROM purchase_history_v ph
			GROUP BY ph."Customer_ID", ph."Group_ID") as table1
	),
	max_group AS (
		SELECT t_2."Customer_ID", gs."Group_Name", avg_margin_, "Group_Minimum_Discount" FROM(
			SELECT *, ROW_NUMBER() OVER(PARTITION BY t_1."Customer_ID") FROM (
				SELECT gv."Customer_ID", gv."Group_ID", max("Group_Affinity_Index"), avg_margin_, "Group_Minimum_Discount"
				FROM groups_v gv
				JOIN avg_margin ag ON ag."Customer_ID" = gv."Customer_ID" AND ag."Group_ID" = gv."Group_ID"
				WHERE "Group_Discount_Share" < max_trans_discont / 100 AND "Group_Churn_Rate" <= index_left 
				AND avg_margin_ * margin > CASE WHEN 
												CEIL("Group_Minimum_Discount" * 100 * 0.2) * 5 < 5 THEN 5
												ELSE CEIL("Group_Minimum_Discount" * 100 * 0.2) * 5
											END
				GROUP BY gv."Customer_ID", gv."Group_ID", ag.avg_margin_, gv."Group_Minimum_Discount"
				ORDER BY gv."Customer_ID", max DESC) AS t_1) AS t_2
		JOIN groups_sku gs ON t_2."Group_ID" = gs."Group_ID"
		WHERE row_number = 1
	)
	SELECT id, sum("Transaction_Summ") / count(id) * coefficient, mg."Group_Name",
	CASE WHEN 
		CEIL("Group_Minimum_Discount" * 100 * 0.2) * 5 < 5 THEN 5
		ELSE CEIL("Group_Minimum_Discount" * 100 * 0.2) * 5
	END
	FROM all_trans at
	JOIN max_group mg ON at.id = mg."Customer_ID"
	GROUP BY id, mg."Group_Name", avg_margin_, mg."Group_Minimum_Discount";
END;
$$ LANGUAGE plpgsql;

--Errors tests
SELECT * FROM fnc_offerings(1, trans_count := 100, coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)
SELECT * FROM fnc_offerings(2, first_date := '2021-05-05', last_date := '2022-01-01', coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)
SELECT * FROM fnc_offerings(1, first_date := '2023-05-05', last_date := '2022-01-01', coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)
SELECT * FROM fnc_offerings(3, first_date := '2021-05-05', last_date := '2022-01-01', coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)

--Other tests
SELECT * FROM fnc_offerings(2, trans_count := 100, coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)
SELECT * FROM fnc_offerings(1, first_date := '2021-05-05', last_date := '2022-01-01', coefficient := 1.15, index_left := 3, max_trans_discont := 70, margin := 30)

SELECT * FROM fnc_offerings(2, trans_count := 10, coefficient := 1.25, index_left := 4, max_trans_discont := 60, margin := 40)
SELECT * FROM fnc_offerings(1, first_date := '2019-05-05', last_date := '2022-01-01', coefficient := 1.25, index_left := 4, max_trans_discont := 60, margin := 40)

SELECT * FROM fnc_offerings(2, trans_count := 50, coefficient := 1.05, index_left := 5, max_trans_discont := 90, margin := 70)
SELECT * FROM fnc_offerings(1, first_date := '2021-05-05', last_date := '2022-01-01', coefficient := 1.05, index_left := 5, max_trans_discont := 90, margin := 70)

