
DROP FUNCTION IF EXISTS fnc_cross_sales(INTEGER, NUMERIC, NUMERIC, NUMERIC, NUMERIC);
CREATE OR REPLACE FUNCTION fnc_cross_sales(
    IN rows_number INTEGER,
        max_group_churn_rate NUMERIC, --
        max_group_stability_index NUMERIC, --
        max_sku_share NUMERIC,
        max_margin_share NUMERIC --
    ) RETURNS TABLE (
        "Customer_ID" INTEGER,
		"SKU_Name" VARCHAR,
		"Offer_Discount_Depth" NUMERIC
    ) AS $$
BEGIN
    IF max_sku_share < 0 OR max_sku_share > 100 THEN
        RAISE EXCEPTION 'Max sku share must be between 0 and 100.';
    END IF;
    IF max_margin_share < 0 OR max_margin_share > 100 THEN
        RAISE EXCEPTION 'Max margin share must be between 0 and 100.';
    END IF;
RETURN QUERY
WITH cte1 AS (
    SELECT DISTINCT purchase_history_v."Customer_ID",
        purchase_history_v."Group_ID"
    FROM purchase_history_v
),
cte2 AS (
    SELECT stores."Transaction_Store_ID",
        sku."Group_ID",
        stores."SKU_ID",
        stores."SKU_Purchase_Price",
        stores."SKU_Retail_Price",
        ROW_NUMBER() OVER (
            PARTITION BY stores."Transaction_Store_ID",
            sku."Group_ID"
            ORDER BY stores."SKU_Retail_Price" - stores."SKU_Purchase_Price" DESC
        ) AS row_num
    FROM stores
        LEFT JOIN sku ON stores."SKU_ID" = sku."SKU_ID"
),
cte3 AS (
    SELECT "Transaction_Store_ID",
        "Group_ID",
        "SKU_ID",
        "SKU_Retail_Price",
        "SKU_Retail_Price" - "SKU_Purchase_Price" AS "Margin"
    FROM cte2
    WHERE row_num = 1
),
cte4 AS (
    select cte1."Customer_ID" AS "Customers_ID",
        cte1."Group_ID",
        customers_v."Customer_Primary_Store",
        cte3."SKU_ID",
        cte3."Margin" / cte3."SKU_Retail_Price" AS coef_margin
    FROM cte1
        LEFT JOIN customers_v ON cte1."Customer_ID" = customers_v."Customer_ID"
        LEFT JOIN cte3 ON cte3."Transaction_Store_ID" = customers_v."Customer_Primary_Store"
        AND cte3."Group_ID" = cte1."Group_ID"
),
cte5 AS (
    SELECT cte4."Customers_ID" AS "Customer_ID", 
        cte4."Group_ID",
        cte4."Customer_Primary_Store",
        cte4."SKU_ID",
        cte4.coef_margin
    FROM cte4
    WHERE "SKU_ID" IS NOT NULL
),
-- with 
cte6 AS (
    SELECT 
		cards."Customer_ID",
        sku."Group_ID",
		        checks."SKU_ID"
    FROM checks
        LEFT JOIN sku ON checks."SKU_ID" = sku."SKU_ID"
		LEFT JOIN transactions ON transactions."Transaction_ID"=checks."Transaction_ID"
    	LEFT JOIN cards ON cards."Customer_Card_ID"=transactions."Customer_Card_ID"
 -- )
),
cte7 AS (
    SELECT DISTINCT 
        cte6."Customer_ID",
        cte6."Group_ID",
        COUNT(cte6."Group_ID") AS "C_Group"
    FROM cte6
    GROUP BY cte6."Customer_ID",cte6."Group_ID"
),
cte8 AS (
    SELECT DISTINCT cte6."Customer_ID",
    cte6."SKU_ID",
    COUNT(cte6."SKU_ID") AS "C_SKU"
    FROM cte6
    GROUP BY cte6."Customer_ID", cte6."SKU_ID"
),
 cte9 AS (
SELECT cte5."Customer_ID",
    cte5."Group_ID",
    cte5."SKU_ID",
    (SELECT 
        sku."SKU_Name"
    FROM sku
    WHERE sku."SKU_ID"=cte5."SKU_ID"),
    cte5."Customer_Primary_Store",
    CASE 
        -- WHEN cte5.coef_margin * max_margin_share/100 => CEIL(groups_v."Group_Minimum_Discount"*0.2) * 5
        -- THEN, CEIL(groups_v."Group_Minimum_Discount"*0.2) * 5
        WHEN cte5.coef_margin * max_margin_share /100 >= CEIL(groups_v."Group_Minimum_Discount"*0.2*100) / 100 * 5
        THEN CEIL(groups_v."Group_Minimum_Discount"*0.2*100) * 5
        ELSE NULL
    END AS "Offer_Discount_Depth",
	groups_v."Group_Minimum_Discount",
	cte8."C_SKU"::NUMERIC / cte7."C_Group"::NUMERIC * 100 AS percent_sku
FROM cte5
    LEFT JOIN cte7 ON cte5."Group_ID" = cte7."Group_ID"
    AND cte5."Customer_ID"=cte7."Customer_ID"
    LEFT JOIN cte8 ON cte5."SKU_ID" = cte8."SKU_ID"
    AND cte5."Customer_ID"=cte8."Customer_ID"
	LEFT JOIN groups_v ON cte5."Customer_ID"=groups_v."Customer_ID"
	AND cte5."Group_ID"=groups_v."Group_ID"
WHERE
    max_group_churn_rate >= groups_v."Group_Churn_Rate"
    AND max_group_stability_index >= groups_v."Group_Stability_Index"
),
cte10 AS (
SELECT
    cte9."Customer_ID",
    cte9."Group_ID",
    (SELECT sku."SKU_Name"
    FROM sku
    WHERE sku."SKU_ID"=cte9."SKU_ID") AS "SKU_Name",
    ROW_NUMBER() OVER (PARTITION BY cte9."Customer_ID") AS group_count,
    cte9."Offer_Discount_Depth"
FROM cte9
WHERE percent_sku <= max_sku_share
AND cte9."Offer_Discount_Depth" IS NOT NULL
AND cte9."Offer_Discount_Depth" != 0
)
SELECT 
    cte10."Customer_ID",
    cte10."SKU_Name",
    cte10."Offer_Discount_Depth"
FROM
    cte10
WHERE
    group_count <= rows_number;
END;
$$ LANGUAGE plpgsql;

select * from fnc_cross_sales(5, 3, 0.5, 100, 30);