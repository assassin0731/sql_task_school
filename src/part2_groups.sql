--
-- Customer_ID
-- Group_ID
--
DROP FUNCTION fnc_temporary_groups_data();
CREATE OR REPLACE FUNCTION fnc_temporary_groups_data() RETURNS TABLE (
		"Customers_ID" INTEGER,
		"Groups_ID" INTEGER,
		"Group_Affinity_Index" NUMERIC,
		"Group_Churn_Rate" NUMERIC,
		"Group_Stability_Index" NUMERIC,
		"Group_Discount_Shares" NUMERIC,
		"Group_Minimum_Discount" NUMERIC,
		"Group_Average_Discount" NUMERIC
	) AS $$ BEGIN RETURN QUERY WITH cte_checks AS (
		SELECT DISTINCT (
				SELECT (
						SELECT "Customer_ID"
						FROM cards
						WHERE "Customer_Card_ID" = transactions."Customer_Card_ID"
					)
				FROM transactions
				WHERE "Transaction_ID" = checks."Transaction_ID"
			) AS "Customer_ID",
			(
				SELECT "Group_ID"
				FROM sku
				WHERE "SKU_ID" = checks."SKU_ID"
			) AS "Group_ID"
		FROM checks
		GROUP BY "Customer_ID",
			"Group_ID"
	),
	cte1 AS (
		SELECT (
				SELECT (
						SELECT "Customer_ID"
						FROM cards
						WHERE "Customer_Card_ID" = transactions."Customer_Card_ID"
					)
				FROM transactions
				WHERE "Transaction_ID" = checks."Transaction_ID"
			) AS "Customer_ID",
			(
				SELECT "Group_ID"
				FROM sku
				WHERE "SKU_ID" = checks."SKU_ID"
			) AS "Group_ID",
			COUNT(DISTINCT checks."Transaction_ID")::NUMERIC AS "Disc_Transactions"
		FROM checks
		WHERE "SKU_Discount" > 0
		GROUP BY "Customer_ID",
			"Group_ID"
	),
	cte2 AS (
		SELECT cte_checks."Customer_ID",
			cte_checks."Group_ID",
			"First_Group_Purchase_Date",
			"Last_Group_Purchase_Date",
			"Group_Frequency",
			COALESCE (
				(
					SELECT "Disc_Transactions"
					FROM cte1
					WHERE "Customer_ID" = cte_checks."Customer_ID"
						AND "Group_ID" = cte_checks."Group_ID"
				) / "Group_Purchase",
				0
			) AS "Group_Discount_Share"
		FROM cte_checks
			JOIN periods ON cte_checks."Customer_ID" = periods."Customer_ID"
			AND cte_checks."Group_ID" = periods."Group_ID"
	),
	--
-- Group Affinity Index, Group Churn Rate
	--
cte3 AS (
SELECT "Customer_ID",
	"Group_ID",
	(
		SELECT COUNT(DISTINCT "Transaction_ID")::NUMERIC
		FROM purchase_history_v
		WHERE "Customer_ID" = cte2."Customer_ID"
			AND "Group_ID" = cte2."Group_ID"
			AND "Transaction_DateTime" <= cte2."Last_Group_Purchase_Date"
			AND "Transaction_DateTime" >= cte2."First_Group_Purchase_Date"
	) / (
		SELECT COUNT(DISTINCT "Transaction_ID")::NUMERIC
		FROM purchase_history_v
		WHERE "Customer_ID" = cte2."Customer_ID"
			AND "Transaction_DateTime" <= cte2."Last_Group_Purchase_Date"
			AND "Transaction_DateTime" >= cte2."First_Group_Purchase_Date"
	) AS "Group_Affinity_Index",
	EXTRACT(
		DAY
		FROM (
				SELECT "Analysis_Formation"
				FROM date_of_analysis_formation
			) - "Last_Group_Purchase_Date"
	) / "Group_Frequency" AS "Group_Churn_Rate",
	"Group_Discount_Share",
	(
		SELECT SUM("Group_Summ_Paid") / SUM("Group_Summ")
		FROM purchase_history_v
		WHERE "Customer_ID" = cte2."Customer_ID"
			AND "Group_ID" = cte2."Group_ID"
			AND "Group_Summ_Paid" != "Group_Summ"
		GROUP BY "Customer_ID",
			"Group_ID"
	) AS "Group_Average_Discount"
FROM cte2
),
cte4 AS (
	SELECT v1."Customer_ID",
		v1."Group_ID",
		EXTRACT(
			DAY
			FROM (
					v1."Transaction_DateTime" - COALESCE(
						(
							SELECT MAX("Transaction_DateTime")
							FROM purchase_history_v
							WHERE "Customer_ID" = v1."Customer_ID"
								AND "Group_ID" = v1."Group_ID"
								AND "Transaction_DateTime" < v1."Transaction_DateTime"
						),
						v1."Transaction_DateTime"
					)
				)
		) AS "Periods"
	FROM purchase_history_v AS v1
),
--
-- Group Stability Index
--
cte5 AS (
SELECT v1."Customer_ID",
	v1."Group_ID",
	AVG(
		ABS(
			v1."Periods" - (
				SELECT "Group_Frequency"
				FROM public.periods
				WHERE "Customer_ID" = v1."Customer_ID"
					AND "Group_ID" = v1."Group_ID"
			)
		) / (
			SELECT "Group_Frequency"
			FROM public.periods
			WHERE "Customer_ID" = v1."Customer_ID"
				AND "Group_ID" = v1."Group_ID"
		)
	) AS "Group_Stability_Index"
FROM cte4 AS v1
WHERE "Periods" != 0
GROUP BY v1."Customer_ID",
	v1."Group_ID"
),
cte6 AS (
	SELECT "Customer_ID",
		"Group_ID",
		(MIN("Group_Min_Discount")) AS "Group_Minimum_Discount" -- "Group_Min_Discount"
	FROM periods
	WHERE "Group_Min_Discount" > 0
	GROUP BY "Customer_ID",
		"Group_ID"
)
SELECT cte3."Customer_ID" AS "Customers_ID",
	cte3."Group_ID" AS "Groups_ID",
	cte3."Group_Affinity_Index" AS "Group_Affinity_Index",
	cte3."Group_Churn_Rate" AS "Group_Churn_Rate",
	COALESCE(cte5."Group_Stability_Index", 0) AS "Group_Stability_Index",
	cte3."Group_Discount_Share" AS "Group_Discount_Shares",
	COALESCE(cte6."Group_Minimum_Discount", 0) AS "Group_Minimum_Discount",
	COALESCE(cte3."Group_Average_Discount", 1) AS "Group_Average_Discount"
FROM cte3
	LEFT JOIN cte5 ON cte5."Customer_ID" = cte3."Customer_ID"
	AND cte5."Group_ID" = cte3."Group_ID"
	LEFT JOIN cte6 ON cte5."Customer_ID" = cte6."Customer_ID"
	AND cte5."Group_ID" = cte6."Group_ID";
END;
$$ LANGUAGE plpgsql;
-- $$ -- CREATE OR REPLACE FUNCTION create_view_function()
-- RETURNS void AS $$
-- DECLARE
--     view_name TEXT := 'your_view_name';
--     view_definition TEXT := 'SELECT column1, column2 FROM your_source_table;';
-- BEGIN
--     EXECUTE 'CREATE OR REPLACE VIEW '|| view_name || ' AS ' || view_definition;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE
-- OR REPLACE FUNCTION fnc_temporary_groups_data(
--     IN flag INTEGER DEFAULT 1,
--     IN days_int INTERVAL DEFAULT '6000 days'::INTERVAL,
--     IN transactions_ct INTEGER DEFAULT 200
-- )
CREATE OR REPLACE FUNCTION fnc_create_v_groups(
		IN flag INTEGER DEFAULT 1,
		IN days_int INTERVAL DEFAULT '6000 days'::interval,
		IN tr_count INTEGER DEFAULT 200
	) RETURNS void AS $$
DECLARE view_name TEXT := 'groups_v';
last_date TIMESTAMP := (
	SELECT "Analysis_Formation"
	FROM date_of_analysis_formation
	ORDER BY "Analysis_Formation" DESC
	LIMIT 1
);
BEGIN IF flag = 1 THEN EXECUTE 'CREATE OR REPLACE VIEW ' || view_name || ' AS 
    WITH cte1 AS (
    SELECT "Customer_ID",
        "Group_ID",
        SUM("Group_Summ_Paid" - "Group_Cost") FILTER(
            WHERE "Transaction_DateTime" BETWEEN ''' || (last_date - days_int) || ''' AND ''' || last_date || ''' ) AS "Group_Margin"
    FROM purchase_history_v
    GROUP BY "Customer_ID",
        "Group_ID"
)
SELECT asd."Customers_ID" AS "Customer_ID",
    asd."Groups_ID" AS "Group_ID",
    asd."Group_Affinity_Index",
    asd."Group_Churn_Rate",
    asd."Group_Stability_Index",
    COALESCE(cte1."Group_Margin", 0) AS "Group_Margin",
    asd."Group_Discount_Shares" AS "Group_Discount_Share",
    asd."Group_Minimum_Discount",
    asd."Group_Average_Discount"
FROM fnc_temporary_groups_data() AS asd
    LEFT JOIN cte1 ON asd."Groups_ID" = cte1."Group_ID"
    AND asd."Customers_ID" = cte1."Customer_ID";';
ELSIF flag = 2 THEN EXECUTE 'CREATE OR REPLACE VIEW ' || view_name || ' AS 
    SELECT "Customers_ID" AS "Customer_ID",
    "Groups_ID" AS "Group_ID",
    "Group_Affinity_Index",
    "Group_Churn_Rate",
    "Group_Stability_Index",
    COALESCE((
        SELECT SUM(differ_cost)
        FROM (
                SELECT "Customer_ID",
                    "Transaction_ID",
                    "Group_ID",
                    "Group_Summ_Paid" - "Group_Cost" AS differ_cost
                FROM purchase_history_v
                ORDER BY "Transaction_DateTime" DESC
                LIMIT ' || tr_count || '
            ) AS buff
        WHERE asd."Customers_ID" = buff."Customer_ID"
            AND asd."Groups_ID" = buff."Group_ID"
        GROUP BY buff."Customer_ID",
            buff."Group_ID"
    ), 0) AS "Group_Margin",
    "Group_Discount_Shares" AS "Group_Discount_Share",
    "Group_Minimum_Discount",
    "Group_Average_Discount"
FROM fnc_temporary_groups_data() AS asd;';
END IF;
END;
$$ LANGUAGE plpgsql;
select fnc_create_v_groups(1, '300 days'::INTERVAL, 100);
select fnc_create_v_groups();
select *
from groups_v