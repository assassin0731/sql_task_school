CREATE OR REPLACE VIEW purchase_history_v AS WITH cte_checks AS (
		SELECT (
				SELECT (
						SELECT "Customer_ID"
						FROM cards
						WHERE "Customer_Card_ID" = transactions."Customer_Card_ID"
					)
				FROM transactions
				WHERE "Transaction_ID" = checks."Transaction_ID"
			) AS "Customer_ID",
			"Transaction_ID",
			"SKU_ID",
			"SKU_Amount",
			"SKU_Summ",
			"SKU_Summ_Paid",
			(
				SELECT "Transaction_Store_ID"
				FROM transactions
				WHERE "Transaction_ID" = checks."Transaction_ID"
			) AS "Transaction_Store_ID"
		FROM checks
	),
	cte_checks2 AS (
		SELECT "Customer_ID",
			"Transaction_ID",
			(
				SELECT "Transaction_DateTime"
				FROM transactions
				WHERE "Transaction_ID" = cte_checks."Transaction_ID"
			) AS "Transaction_DateTime",
			"SKU_ID",
			(
				SELECT "Group_ID"
				FROM sku
				WHERE "SKU_ID" = cte_checks."SKU_ID"
			) AS "Group_ID",
			"SKU_Amount",
			"SKU_Summ",
			"SKU_Summ_Paid",
			(
				SELECT "SKU_Purchase_Price"
				FROM stores
				WHERE "SKU_ID" = cte_checks."SKU_ID"
					AND "Transaction_Store_ID" = cte_checks."Transaction_Store_ID"
			)
		FROM cte_checks
	)
SELECT "Customer_ID",
	"Transaction_ID",
	"Transaction_DateTime",
	"Group_ID",
	SUM("SKU_Purchase_Price" * "SKU_Amount") AS "Group_Cost",
	SUM("SKU_Summ") AS "Group_Summ",
	SUM("SKU_Summ_Paid") AS "Group_Summ_Paid"
FROM cte_checks2
GROUP BY "Customer_ID",
	"Group_ID",
	"Transaction_ID",
	"Transaction_DateTime";
;