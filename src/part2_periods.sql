CREATE OR REPLACE VIEW periods AS WITH cards_id AS (
		-- Составляем массив всех карт клиента
		SELECT pd."Customer_ID",
			array_agg(cards."Customer_Card_ID") AS cust_cards
		FROM personal_data AS pd
			JOIN cards ON pd."Customer_ID" = cards."Customer_ID"
		GROUP BY pd."Customer_ID"
	),
	all_trans AS (
		-- Таблица со всеми транзакциями клиента
		SELECT "Customer_ID",
			t."Transaction_ID",
			sku."SKU_ID",
			"Group_ID",
			CASE
				WHEN "SKU_Discount" = 0 THEN NULL
				ELSE "SKU_Discount"
			END / "SKU_Summ" AS "Group_Min_Discount",
			"Transaction_DateTime"
		FROM cards_id
			JOIN transactions AS t ON t."Customer_Card_ID" = ANY(cust_cards)
			JOIN checks ON checks."Transaction_ID" = t."Transaction_ID"
			JOIN sku ON checks."SKU_ID" = sku."SKU_ID"
	),
	with_dates AS (
		-- Группировка по клиенту и группе, максимальная и минимальная дата для каждой группы
		SELECT "Customer_ID",
			"Group_ID",
			(
				SELECT min("Transaction_DateTime")
				FROM all_trans a_t
				WHERE a_t."Customer_ID" = all_trans."Customer_ID"
					AND a_t."Group_ID" = all_trans."Group_ID"
			) AS "First_Group_Purchase_Date",
			(
				SELECT max("Transaction_DateTime")
				FROM all_trans a_t
				WHERE a_t."Customer_ID" = all_trans."Customer_ID"
					AND a_t."Group_ID" = all_trans."Group_ID"
			) AS "Last_Group_Purchase_Date",
			count(*) AS "Group_Purchase"
		FROM all_trans
		GROUP BY "Customer_ID",
			"Group_ID",
			"First_Group_Purchase_Date",
			"Last_Group_Purchase_Date"
	) -- Вычисляем частоту покупок для каждой группы и минимальную скидку
SELECT w_d."Customer_ID",
	w_d."Group_ID",
	w_d."First_Group_Purchase_Date",
	w_d."Last_Group_Purchase_Date",
	w_d."Group_Purchase",
	(
		(
			SELECT EXTRACT(
					DAY
					FROM (
							"Last_Group_Purchase_Date" - "First_Group_Purchase_Date"
						)
				)
		) + 1
	) / "Group_Purchase" AS "Group_Frequency",
	coalesce(min("Group_Min_Discount"), 0) AS "Group_Min_Discount"
FROM with_dates w_d
	JOIN all_trans a_t ON a_t."Customer_ID" = w_d."Customer_ID"
	AND a_t."Group_ID" = w_d."Group_ID"
GROUP BY w_d."Customer_ID",
	w_d."Group_ID",
	w_d."First_Group_Purchase_Date",
	w_d."Last_Group_Purchase_Date",
	w_d."Group_Purchase",
	"Group_Frequency";
SELECT *
FROM periods;