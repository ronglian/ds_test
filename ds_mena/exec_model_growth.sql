/*
{"email_me":null,
 "write_schema": "exec_dash",
 "text_me":"18455370734",
 "schedule":
 [{
  "table_name": "mv_regional_growth",
  "enabled":true,
 "start_at": "2016-01-01",
  "timezone": "UTC",
  "cron_schedule": "0 2 * * *",
  "input_json": {
  },
  "type": "normal",
  "incremental_field": null,
  "keys":{
    "primarykey": null,
    "sortkey": null,
    "distkey": "all"
    }
}]}
*/

WITH cap AS
(
SELECT
	month,
	region,
	SUM(capacity) AS desk_cap_bom,
	COUNT(DISTINCT reservable_uuid) AS res_cap_bom
FROM public_kpi.snapshot_desks ssd
INNER JOIN dw.mv_dim_location l
	ON l.location_uuid = ssd.location_uuid
WHERE l.region NOT IN ('India')
GROUP BY 1,2
),

occ_raw AS
(
SELECT
	month,
	region,
	SUM(real_capacity) AS desk_occ_bom,
	COUNT(DISTINCT reservable_uuid) AS res_occ_bom,
	COUNT(DISTINCT account_uuid) AS accounts_count,
	COUNT(DISTINCT organization_uuid) AS orgs_count
FROM public_kpi.snapshot_occupied_desks ssod
INNER JOIN dw.mv_dim_location l
	ON l.location_uuid = ssod.location_uuid
WHERE l.region NOT IN ('India')
GROUP BY 1,2
),

occ AS
(
SELECT
	month,
	series_month,
	region,
	desk_cap_bom,
	desk_occ_bom,
	desk_occ_perc_bom,
	res_occ_perc_bom,
	nonhd_cap_bom,
	nonhd_occ_bom,
	nonhd_desk_occ_perc_bom,
	members_target_amt AS occ_target,
	desk_occ_bom / members_target_amt::FLOAT AS occ_perc_to_target,
	accounts_count,
	orgs_count
FROM
(
SELECT
	cap.month,
	DATEDIFF('month',DATE_TRUNC('month',cap.month),DATE_TRUNC('month',CURRENT_DATE)) AS series_month,
	cap.region,
	desk_cap_bom,
	desk_occ_bom,
	desk_occ_bom / desk_cap_bom::FLOAT AS desk_occ_perc_bom,
	res_occ_bom / res_cap_bom::FLOAT AS res_occ_perc_bom,
	0 AS nonhd_cap_bom,
	0 AS nonhd_occ_bom,
	0 AS nonhd_desk_occ_perc_bom,
	--placeholders for now
	accounts_count,
	orgs_count
FROM cap
LEFT JOIN occ_raw
	ON occ_raw.month = cap.month
		AND occ_raw.region = cap.region
UNION
(
SELECT
	DATE_TRUNC('month',month) AS month,
	DATEDIFF('month',DATE_TRUNC('month',month),DATE_TRUNC('month',CURRENT_DATE)) AS series_month,
	l.region,
	SUM(capacity) AS desk_cap_bom,
	SUM(desk_occ) AS desk_occ_bom,
	SUM(desk_occ)/SUM(capacity::FLOAT) AS desk_occ_perc_bom,
	COUNT(DISTINCT(CASE WHEN desk_occ > 0 THEN reservable_uuid END))/COUNT(DISTINCT reservable_uuid)::FLOAT AS res_occ_perc_bom,
	SUM(non_hotdesk_capacity) AS nonhd_cap_bom,
	SUM(non_hotdesk_occ) AS nonhd_occ_bom,
	SUM(non_hotdesk_occ)/SUM(non_hotdesk_capacity::FLOAT) AS nonhd_desk_occ_perc_bom,
	-- target placeholder
	COUNT(DISTINCT(oc.account_uuid)) AS accounts_count,
	COUNT(DISTINCT(a.organization_uuid)) AS orgs_count
FROM dw.mv_bom_eom_occ_cap oc
INNER JOIN dw.mv_dim_location l
	ON l.location_uuid = oc.location_uuid
LEFT JOIN dw.mv_dim_account a
	ON a.account_uuid = oc.account_uuid
WHERE bom_or_eom IN ('bom') AND l.region NOT IN ('India')
AND DATE_TRUNC('month',month) NOT IN (SELECT DISTINCT month FROM public_kpi.snapshot_desks)
GROUP BY 1,2,3
ORDER BY month DESC
)
) occ
LEFT JOIN exec_dash.v_occ_rev_ebita_opex_target ot
	ON occ.month = ot.month_start_date
		AND occ.region = ot.region_name
),

churn AS (
-- Desk churn measures, grouped by region and month
SELECT
	churn.month,
	churn.region,
	desk_churn,
	desk_churn/desk_occ_bom::FLOAT AS churn_perc,
	1000 AS churn_target,
	0 AS churn_perc_to_target
	-- Placeholders
FROM
(
	SELECT
		DATE_TRUNC('month',month) AS month,
		region,
		SUM(real_capacity) AS desk_churn
	FROM dw.mv_fact_monthly_churn
	INNER JOIN dw.mv_dim_location l
		ON l.location_uuid = mv_fact_monthly_churn.location_uuid
	WHERE l.region NOT IN ('India')
	GROUP BY 1,2
) churn
INNER JOIN
	(
	SELECT
		DATE_TRUNC('month',month) AS month,
		region,
		SUM(desk_occ) AS desk_occ_bom
	FROM dw.mv_bom_eom_occ_cap
	INNER JOIN dw.mv_dim_location l
		ON l.location_uuid = mv_bom_eom_occ_cap.location_uuid
	WHERE bom_or_eom IN ('bom')
	GROUP BY 1,2
	) occ
ON occ.region = churn.region AND occ.month = DATE_TRUNC('month',churn.month)
),

account_occ AS
-- This CTE used to calculate retention
(
SELECT
	month,
	account_uuid,
	l.region,
	SUM(desk_occ) AS desk_occ,
	SUM(non_hotdesk_occ) AS nonhd_desk_occ
-- Just in case they will want to see non-HD desk occupancy, I brought it in for easy
FROM dw.mv_bom_eom_occ_cap oc
INNER JOIN dw.mv_dim_location l
	ON l.location_uuid = oc.location_uuid
WHERE bom_or_eom IN ('bom') AND l.region NOT IN ('India')
GROUP BY 1,2,3
ORDER BY month DESC
),

retention AS (
-- Retention measures AKA
-- are the accounts this month staying with us next month?
SELECT
	month,
	region,
	SUM(desk_occ_next_month)/SUM(desk_occ)::FLOAT AS retention_perc
FROM
	(
	SELECT
		a1.month AS month,
		a2.month AS next_month,
		a1.region,
		a1.desk_occ AS desk_occ,
		a2.desk_occ as desk_occ_next_month
	FROM account_occ a1
	LEFT JOIN account_occ a2
		ON a2.month = DATE_ADD('month',1,a1.month)
			AND a2.account_uuid = a1.account_uuid
			AND a2.region = a1.region
	)
GROUP BY 1,2
ORDER BY month DESC
),

sales_by_region AS
-- Logic borrowed from Sales Reporting Looker Model, when user selection = Region and Month
(
SELECT 
	new_sales_reporting.user_selection  AS "region",
	DATE(new_sales_reporting.date) AS "month",
	COALESCE(SUM(new_sales_reporting.new_sales), 0) AS "new_sales",
	COALESCE(SUM(new_sales_reporting.upgrades), 0) AS "upgrades",
	COALESCE(SUM(new_sales_reporting.upgrades + new_sales_reporting.new_sales), 0) AS "total_desk_sales",
	COALESCE(COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(monthly_desk_sales_goals.goal ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0), 0) AS "sales_goal"
FROM
	(select
        'Month' as time_grouping,
        'no' as include_jv_markets,
        'Region' as select_grouping,
        date_trunc (lower('Month'), v.date_reserved_local)::date as date,
      v.account_uuid,
      v.account_name,
      t.region as user_selection,
      t.region as region,
        case
          when sum(case when transfer_type is not null then desks_changed else 0 end) >0
        then
          sum(case when transfer_type is not null then desks_changed else 0 end)
        else
          sum(0)
        end as
        upgrades,
        sum(case when action_type = 'Move In' then desks_changed else 0 end) as new_sales,
	row_number() over() as id
        from dw.v_transaction v
        left join dw.mv_dim_location t
        on v.location_uuid = t.location_uuid
        where CASE WHEN 'no' = 'no'
              THEN NOT t.is_joint_venture ELSE True END
        group by 1,2,3,4,5,6,7,8
        order by 2
      ) new_sales_reporting
FULL OUTER JOIN dw.mv_monthly_desk_sales_goals  AS monthly_desk_sales_goals ON new_sales_reporting.select_grouping = monthly_desk_sales_goals.select_grouping
          and new_sales_reporting.time_grouping = 'Month'
          and new_sales_reporting.user_selection = monthly_desk_sales_goals.user_selection
          and (DATE(new_sales_reporting.date)) = (DATE(monthly_desk_sales_goals.month ))
GROUP BY 1,2
ORDER BY 2 DESC
),

sales_global AS
-- Logic borrowed from Sales Reporting Looker Model, when user selection = Global and Month
(
WITH new_sales_reporting AS (select
        'Month' as time_grouping,
        'no' as include_jv_markets,
        'Global' as select_grouping,
        date_trunc (lower('Month'), v.date_reserved_local)::date as date,
      v.account_uuid,
      v.account_name,
      'Global' AS user_selection,
        case
          when sum(case when transfer_type is not null then desks_changed else 0 end) >0
        then
          sum(case when transfer_type is not null then desks_changed else 0 end)
        else
          sum(0)
        end as
        upgrades,
        sum(case when action_type = 'Move In' then desks_changed else 0 end) as new_sales,
        row_number() over() as id
        from dw.v_transaction v
        left join dw.mv_dim_location t
        on v.location_uuid = t.location_uuid
        where CASE WHEN 'no' = 'no'
              THEN NOT t.is_joint_venture ELSE True END
        group by 1,2,3,4,5,6,7
        order by 2
      )
SELECT 
	('(All)')  AS region,
	DATE(new_sales_reporting.date) AS report_month,
	COALESCE(SUM(new_sales_reporting.new_sales), 0) AS new_sales,
	COALESCE(SUM(new_sales_reporting.upgrades), 0) AS upgrades,
	COALESCE(SUM(new_sales_reporting.upgrades + new_sales_reporting.new_sales), 0) AS total_desk_sales,
	COALESCE(COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(monthly_desk_sales_goals.goal ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CONVERT(VARCHAR,monthly_desk_sales_goals.id)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0), 0) AS sales_goal
FROM new_sales_reporting
FULL OUTER JOIN dw.mv_monthly_desk_sales_goals  AS monthly_desk_sales_goals ON new_sales_reporting.select_grouping = monthly_desk_sales_goals.select_grouping
          and new_sales_reporting.time_grouping = 'Month'
          and new_sales_reporting.user_selection = monthly_desk_sales_goals.user_selection
          and (DATE(new_sales_reporting.date)) = (DATE(monthly_desk_sales_goals.month ))
WHERE 
	(((new_sales_reporting.date) >= ((DATEADD(month,-11, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()))) ))) AND (new_sales_reporting.date) < ((DATEADD(month,12, DATEADD(month,-11, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()))) ) )))))
GROUP BY 1,2
ORDER BY 2 DESC
),

regions AS
(
SELECT
	occ.month AS report_month,
	occ.series_month,
	occ.region,
	desk_cap_bom,
	desk_occ_bom,
	desk_occ_perc_bom,
	res_occ_perc_bom,
	nonhd_cap_bom,
	nonhd_occ_bom,
	nonhd_desk_occ_perc_bom,
	-- target placeholder
	occ_target,
	occ_perc_to_target,
	churn_target,
	churn_perc_to_target,
	new_sales,
	upgrades,
	total_desk_sales,
	sales_goal,
	total_desk_sales/NULLIF(sales_goal::FLOAT,0) AS sales_perc_to_goal,
	accounts_count,
	orgs_count,
	desk_churn,
	churn_perc,
	retention_perc
FROM occ
LEFT JOIN churn
	ON churn.month = occ.month
		AND churn.region = occ.region
LEFT JOIN retention
	ON retention.month = occ.month
		AND retention.region = occ.region
LEFT JOIN sales_by_region
	ON sales_by_region.month = occ.month
		AND sales_by_region.region = occ.region
)

SELECT *
FROM regions

UNION

SELECT
	all_occ.report_month,
	all_occ.series_month,
	all_occ.region,
	desk_cap_bom,
	desk_occ_bom,
	desk_occ_perc_bom,
	res_occ_perc_bom,
	nonhd_cap_bom,
	nonhd_occ_bom,
	nonhd_desk_occ_perc_bom,
	occ_target,
	occ_perc_to_target,
	churn_target,
	churn_perc_to_target,
	new_sales,
	upgrades,
	total_desk_sales,
	sales_goal,
	total_desk_sales/NULLIF(sales_goal::FLOAT,0) AS sales_perc_to_goal,
	accounts_count,
	orgs_count,
	desk_churn,
	churn_perc,
	retention_perc
FROM
	(
	SELECT
		regions.report_month,
		regions.series_month,
		'(All)' AS region,
		SUM(desk_cap_bom) AS desk_cap_bom,
		SUM(desk_occ_bom) AS desk_occ_bom,
		AVG(desk_occ_perc_bom) AS desk_occ_perc_bom,
		AVG(res_occ_perc_bom) AS res_occ_perc_bom,
		SUM(nonhd_cap_bom::FLOAT) AS nonhd_cap_bom,
		SUM(nonhd_occ_bom::FLOAT) AS nonhd_occ_bom,
		SUM(nonhd_desk_occ_perc_bom::FLOAT) AS nonhd_desk_occ_perc_bom,
		-- target placeholder
		SUM(occ_target) AS occ_target,
		AVG(occ_perc_to_target) AS occ_perc_to_target,
		SUM(churn_target) AS churn_target,
		AVG(churn_perc_to_target::FLOAT) AS churn_perc_to_target,
		SUM(accounts_count) AS accounts_count,
		SUM(orgs_count) AS orgs_count,
		SUM(desk_churn) AS desk_churn,
		AVG(churn_perc) AS churn_perc,
		AVG(retention_perc) AS retention_perc
	FROM regions
	GROUP BY 1,2,3
	) all_occ
LEFT JOIN sales_global
ON sales_global.report_month = all_occ.report_month
	AND sales_global.region = all_occ.region

UNION
-- Need to union month's that Japan has not been open yet for Tableau visualization correction

SELECT
start_date::DATE AS report_month,
DATEDIFF('month',start_date::DATE,DATE_TRUNC('month',CURRENT_DATE)) AS series_month,
'Japan' AS region,
NULL AS desk_cap_bom,
NULL AS desk_occ_bom,
NULL AS desk_occ_perc_bom,
NULL AS res_occ_perc_bom,
NULL AS nonhd_cap_bom,
NULL AS nonhd_occ_bom,
NULL AS nonhd_desk_occ_perc_bom,
NULL AS occ_target,
NULL AS occ_perc_to_target,
0 AS churn_target,
'0' AS churn_perc_to_target,
NULL AS new_sales,
NULL AS upgrades,
NULL AS total_desk_sales,
NULL AS sales_goal,
NULL AS sales_perc_to_goal,
NULL AS accounts_count,
NULL AS orgs_count,
0 AS desk_churn,
0 AS churn_perc,
0 AS retention_perc
FROM dw.month_series
WHERE start_date IN ('2017-10-01','2017-11-01','2017-12-01','2018-01-01')
