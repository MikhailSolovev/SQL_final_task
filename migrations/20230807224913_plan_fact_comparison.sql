-- +goose Up
-- +goose StatementBegin
CREATE MATERIALIZED VIEW mv_plan_fact_2014
AS
WITH fact AS (
    SELECT
        co.countrycode AS country,
        DATE_PART('year', sod.modifieddate) || '.' || DATE_PART('quarter', sod.modifieddate) AS qurter_id,
        p.pcid AS pcid,
        SUM(sod.linetotal) AS salesamt
    FROM salesorderdetail sod JOIN salesorderheader sh ON sod.salesorderid = sh.salesorderid
                              JOIN customer cust ON sh.customerid = cust.customerid
                              JOIN product2 p on sod.productid = p.productid
                              JOIN company co ON cust.companyname = co.cname
    WHERE DATE_PART('year', sod.modifieddate) = '2014' AND
            co.id IN (SELECT cid FROM company_sales WHERE ccls IN ('A', 'B') AND year = 2013)
    GROUP BY  co.countrycode, DATE_PART('year', sod.modifieddate), DATE_PART('quarter', sod.modifieddate), p.pcid)
SELECT
    fact.qurter_id AS "Quarter",
    fact.country AS "Country",
    fact.pcid AS "Category Name",
    CASE
        WHEN plan_data.salesamt IS NOT NULL THEN ROUND(plan_data.salesamt - fact.salesamt, 2)
        END AS "Dev.",
    CASE
        WHEN plan_data.salesamt IS NOT NULL AND plan_data.salesamt != 0 THEN
            ROUND((plan_data.salesamt - fact.salesamt) / plan_data.salesamt * 100, 2)
        END AS "Dev., %"
FROM fact LEFT JOIN plan_data ON plan_data.pcid = fact.pcid AND plan_data.quarterid = fact.qurter_id AND
                                 plan_data.country = fact.country AND plan_data.versionid = 'A';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP MATERIALIZED VIEW mv_plan_fact_2014;
-- +goose StatementEnd
