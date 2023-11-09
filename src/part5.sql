DROP FUNCTION IF EXISTS fnc_depth;
DROP FUNCTION IF EXISTS fnc_personal_offers;
-- SELECT * FROM fnc_depth(3,70,30);

CREATE OR REPLACE FUNCTION fnc_depth(
    IN max_outflow_index FLOAT,
    IN max_share_disc_trans FLOAT,
    IN margin_share FLOAT
)
RETURNS TABLE (
    "Customer_ID" INTEGER,
    "Group_Name" VARCHAR,
    "Discount" NUMERIC,
    "MAX" NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        gv."Customer_ID",
        gs."Group_Name",
		CEIL(gv."Group_Minimum_Discount" * 20) * 5,
        MAX(gv."Group_Affinity_Index")
    FROM groups_v AS gv
    INNER JOIN (
        SELECT
            p."Customer_ID",
            p."Group_ID",
            margin_share * AVG((p."Group_Summ" - p."Group_Cost") / p."Group_Summ") AS calc
        FROM purchase_history_v p
        GROUP BY 1, 2
    ) AS sk USING ("Customer_ID", "Group_ID")
    INNER JOIN groups_sku AS gs USING ("Group_ID")
    WHERE
        "Group_Churn_Rate" <= max_outflow_index
        AND "Group_Discount_Share" * 100 < max_share_disc_trans
        AND sk.calc > CEIL(gv."Group_Minimum_Discount" * 100 * 0.2) * 5
        AND CEIL(gv."Group_Minimum_Discount" * 100 * 0.2) * 5 > 0
    GROUP BY 1,2,3
    ORDER BY 1, 4;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_personal_offers(
    IN first_date TIMESTAMP,
    IN last_date TIMESTAMP,
    IN count_transaction INTEGER,
    IN max_outflow_index FLOAT,
    IN max_share_disc_trans FLOAT,
    IN margin_share FLOAT
) RETURNS TABLE (
    "Customer_ID" INTEGER,
    "Start_Date" TIMESTAMP,
    "End_Date" TIMESTAMP,
    "Required_Transactions_Count" INTEGER,
    "Group_Name" VARCHAR,
    "Offer_Discount_Depth" NUMERIC
) AS $$
BEGIN
	IF first_date > last_date OR first_date IS NULL OR last_date IS NULL THEN
		RAISE EXCEPTION 'Wrong dates';
	END IF;
    IF count_transaction < 0 OR count_transaction IS NULL THEN
        RAISE EXCEPTION 'The added number of transactions must be > 0';
    END IF;
    IF max_share_disc_trans < 0 OR max_share_disc_trans > 100 OR max_share_disc_trans IS NULL THEN
        RAISE EXCEPTION 'The maximum share of transactions with a discount must be between 0 and 100.';
    END IF;
    IF margin_share < 0 OR margin_share > 100 OR margin_share IS NULL  THEN
        RAISE EXCEPTION 'Margin share must be between 0 and 100.';
    END IF;
    RETURN QUERY
    SELECT
        fn."Customer_ID",
        first_date,
        last_date,
        ( DATE_PART('day', last_date - first_date) / "Customer_Frequency"
        ) :: INTEGER + count_transaction,
        fn."Group_Name",
        "Discount"
    FROM fnc_depth(max_outflow_index, max_share_disc_trans, margin_share) AS fn
    INNER JOIN customers_v USING ("Customer_ID")
    ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM
    fnc_personal_offers(
        '2022-08-18 00:00:00' :: TIMESTAMP,
        '2022-08-18 00:00:00' :: TIMESTAMP,
        1,
        3,
        70,
        30
    );