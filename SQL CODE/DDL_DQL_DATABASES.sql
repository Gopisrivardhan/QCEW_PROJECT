-- Create typed raw ingestion table (Stage 1)
CREATE TABLE raw_table (
    area_fips TEXT,
    own_code TEXT,
    industry_code TEXT,
    agglvl_code TEXT,
    size_code TEXT,
    year TEXT,
    qtr TEXT,
    disclosure_code TEXT,
    qtrly_estabs NUMERIC,
    month1_emplvl NUMERIC,
    month2_emplvl NUMERIC,
    month3_emplvl NUMERIC,
    total_qtrly_wages NUMERIC,
    taxable_qtrly_wages NUMERIC,
    qtrly_contributions NUMERIC,
    avg_wkly_wage NUMERIC,
    lq_disclosure_code TEXT,
    lq_qtrly_estabs NUMERIC,
    lq_month1_emplvl NUMERIC,
    lq_month2_emplvl NUMERIC,
    lq_month3_emplvl NUMERIC,
    lq_total_qtrly_wages NUMERIC,
    lq_taxable_qtrly_wages NUMERIC,
    lq_qtrly_contributions NUMERIC,
    lq_avg_wkly_wage NUMERIC,
    oty_disclosure_code TEXT,
    oty_qtrly_estabs_chg NUMERIC,
    oty_qtrly_estabs_pct_chg NUMERIC,
    oty_month1_emplvl_chg NUMERIC,
    oty_month1_emplvl_pct_chg NUMERIC,
    oty_month2_emplvl_chg NUMERIC,
    oty_month2_emplvl_pct_chg NUMERIC,
    oty_month3_emplvl_chg NUMERIC,
    oty_month3_emplvl_pct_chg NUMERIC,
    oty_total_qtrly_wages_chg NUMERIC,
    oty_total_qtrly_wages_pct_chg NUMERIC,
    oty_taxable_qtrly_wages_chg NUMERIC,
    oty_taxable_qtrly_wages_pct_chg NUMERIC,
    oty_qtrly_contributions_chg NUMERIC,
    oty_qtrly_contributions_pct_chg NUMERIC,
    oty_avg_wkly_wage_chg NUMERIC,
    oty_avg_wkly_wage_pct_chg NUMERIC
);

select * from raw_table;

-- Create a cleaned copy with text fields trimmed and empty strings converted to NULL
CREATE TABLE raw_trimmed AS
SELECT
  NULLIF(TRIM(area_fips), '')                        AS area_fips,
  NULLIF(TRIM(own_code), '')                          AS own_code,
  NULLIF(TRIM(industry_code), '')                     AS industry_code,
  NULLIF(TRIM(agglvl_code), '')                       AS agglvl_code,
  NULLIF(TRIM(size_code), '')                         AS size_code,
  NULLIF(TRIM(year), '')                              AS year,
  NULLIF(TRIM(qtr), '')                               AS qtr,
  NULLIF(TRIM(disclosure_code), '')                   AS disclosure_code,

  -- numeric columns kept as-is
  qtrly_estabs,
  month1_emplvl,
  month2_emplvl,
  month3_emplvl,
  total_qtrly_wages,
  taxable_qtrly_wages,
  qtrly_contributions,
  avg_wkly_wage,

  NULLIF(TRIM(lq_disclosure_code), '')                AS lq_disclosure_code,
  lq_qtrly_estabs,
  lq_month1_emplvl,
  lq_month2_emplvl,
  lq_month3_emplvl,
  lq_total_qtrly_wages,
  lq_taxable_qtrly_wages,
  lq_qtrly_contributions,
  lq_avg_wkly_wage,

  NULLIF(TRIM(oty_disclosure_code), '')               AS oty_disclosure_code,
  oty_qtrly_estabs_chg,
  oty_qtrly_estabs_pct_chg,
  oty_month1_emplvl_chg,
  oty_month1_emplvl_pct_chg,
  oty_month2_emplvl_chg,
  oty_month2_emplvl_pct_chg,
  oty_month3_emplvl_chg,
  oty_month3_emplvl_pct_chg,
  oty_total_qtrly_wages_chg,
  oty_total_qtrly_wages_pct_chg,
  oty_taxable_qtrly_wages_chg,
  oty_taxable_qtrly_wages_pct_chg,
  oty_qtrly_contributions_chg,
  oty_qtrly_contributions_pct_chg,
  oty_avg_wkly_wage_chg,
  oty_avg_wkly_wage_pct_chg
FROM raw_table;

select * from raw_trimmed;


-- Create a cleaned table with numeric columns sanitized and cast properly
CREATE TABLE raw_numeric AS
SELECT
    area_fips,
    own_code,
    industry_code,
    agglvl_code,
    size_code,
    year,
    qtr,
    disclosure_code,

    NULLIF(regexp_replace(qtrly_estabs::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS qtrly_estabs,
    NULLIF(regexp_replace(month1_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS month1_emplvl,
    NULLIF(regexp_replace(month2_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS month2_emplvl,
    NULLIF(regexp_replace(month3_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS month3_emplvl,
    NULLIF(regexp_replace(total_qtrly_wages::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS total_qtrly_wages,
    NULLIF(regexp_replace(taxable_qtrly_wages::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS taxable_qtrly_wages,
    NULLIF(regexp_replace(qtrly_contributions::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS qtrly_contributions,
    NULLIF(regexp_replace(avg_wkly_wage::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS avg_wkly_wage,

    lq_disclosure_code,

    NULLIF(regexp_replace(lq_qtrly_estabs::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_qtrly_estabs,
    NULLIF(regexp_replace(lq_month1_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_month1_emplvl,
    NULLIF(regexp_replace(lq_month2_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_month2_emplvl,
    NULLIF(regexp_replace(lq_month3_emplvl::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_month3_emplvl,
    NULLIF(regexp_replace(lq_total_qtrly_wages::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_total_qtrly_wages,
    NULLIF(regexp_replace(lq_taxable_qtrly_wages::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_taxable_qtrly_wages,
    NULLIF(regexp_replace(lq_qtrly_contributions::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_qtrly_contributions,
    NULLIF(regexp_replace(lq_avg_wkly_wage::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS lq_avg_wkly_wage,

    oty_disclosure_code,

    NULLIF(regexp_replace(oty_qtrly_estabs_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_qtrly_estabs_chg,
    NULLIF(regexp_replace(oty_qtrly_estabs_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_qtrly_estabs_pct_chg,
    NULLIF(regexp_replace(oty_month1_emplvl_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month1_emplvl_chg,
    NULLIF(regexp_replace(oty_month1_emplvl_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month1_emplvl_pct_chg,
    NULLIF(regexp_replace(oty_month2_emplvl_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month2_emplvl_chg,
    NULLIF(regexp_replace(oty_month2_emplvl_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month2_emplvl_pct_chg,
    NULLIF(regexp_replace(oty_month3_emplvl_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month3_emplvl_chg,
    NULLIF(regexp_replace(oty_month3_emplvl_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_month3_emplvl_pct_chg,
    NULLIF(regexp_replace(oty_total_qtrly_wages_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_total_qtrly_wages_chg,
    NULLIF(regexp_replace(oty_total_qtrly_wages_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_total_qtrly_wages_pct_chg,
    NULLIF(regexp_replace(oty_taxable_qtrly_wages_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_taxable_qtrly_wages_chg,
    NULLIF(regexp_replace(oty_taxable_qtrly_wages_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_taxable_qtrly_wages_pct_chg,
    NULLIF(regexp_replace(oty_qtrly_contributions_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_qtrly_contributions_chg,
    NULLIF(regexp_replace(oty_qtrly_contributions_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_qtrly_contributions_pct_chg,
    NULLIF(regexp_replace(oty_avg_wkly_wage_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_avg_wkly_wage_chg,
    NULLIF(regexp_replace(oty_avg_wkly_wage_pct_chg::text, '[^0-9\.\-]', '', 'g'), '')::NUMERIC AS oty_avg_wkly_wage_pct_chg
FROM raw_trimmed;

select * from raw_numeric;


CREATE TABLE raw_deduped AS
SELECT DISTINCT ON (area_fips, own_code, industry_code, agglvl_code, size_code)
    *
FROM raw_numeric
ORDER BY area_fips, own_code, industry_code, agglvl_code, size_code;

select * from raw_deduped;

-- Standardize categorical codes with correct ownership mapping
CREATE TABLE raw_categoricals AS
SELECT
    area_fips,
    
    -- Correct ownership mapping from official source
    CASE own_code
        WHEN '0' THEN 'Total Covered'
        WHEN '5' THEN 'Private'
        WHEN '4' THEN 'International Government'
        WHEN '3' THEN 'Local Government'
        WHEN '2' THEN 'State Government'
        WHEN '1' THEN 'Federal Government'
        ELSE NULL
    END AS own_code,

    industry_code,
    agglvl_code,

    -- Size code mapping
    CASE size_code
        WHEN '0' THEN 'All sizes'
        WHEN '1' THEN '0-4'
        WHEN '2' THEN '5-9'
        WHEN '3' THEN '10-19'
        WHEN '4' THEN '20-49'
        WHEN '5' THEN '50-99'
        WHEN '6' THEN '100-249'
        WHEN '7' THEN '250-499'
        WHEN '8' THEN '500-999'
        WHEN '9' THEN '1000+'
        ELSE NULL
    END AS size_code,

    year,
    qtr,

    -- Disclosure codes standardized
    CASE TRIM(disclosure_code)
        WHEN 'N' THEN 'Not disclosed'
        ELSE NULL
    END AS disclosure_code,

    qtrly_estabs,
    month1_emplvl,
    month2_emplvl,
    month3_emplvl,
    total_qtrly_wages,
    taxable_qtrly_wages,
    qtrly_contributions,
    avg_wkly_wage,

    CASE TRIM(lq_disclosure_code)
        WHEN 'N' THEN 'Not disclosed'
        ELSE NULL
    END AS lq_disclosure_code,

    lq_qtrly_estabs,
    lq_month1_emplvl,
    lq_month2_emplvl,
    lq_month3_emplvl,
    lq_total_qtrly_wages,
    lq_taxable_qtrly_wages,
    lq_qtrly_contributions,
    lq_avg_wkly_wage,

    CASE TRIM(oty_disclosure_code)
        WHEN 'N' THEN 'Not disclosed'
        ELSE NULL
    END AS oty_disclosure_code,

    oty_qtrly_estabs_chg,
    oty_qtrly_estabs_pct_chg,
    oty_month1_emplvl_chg,
    oty_month1_emplvl_pct_chg,
    oty_month2_emplvl_chg,
    oty_month2_emplvl_pct_chg,
    oty_month3_emplvl_chg,
    oty_month3_emplvl_pct_chg,
    oty_total_qtrly_wages_chg,
    oty_total_qtrly_wages_pct_chg,
    oty_taxable_qtrly_wages_chg,
    oty_taxable_qtrly_wages_pct_chg,
    oty_qtrly_contributions_chg,
    oty_qtrly_contributions_pct_chg,
    oty_avg_wkly_wage_chg,
    oty_avg_wkly_wage_pct_chg
FROM raw_deduped;

select * from raw_categoricals;

-- Add explicit time components for easier reporting
CREATE TABLE fact_with_time AS
SELECT
    *,
    -- Build a canonical reporting period start (YYYY-MM-DD) from year + qtr
    CASE qtr
        WHEN '1' THEN make_date(year::int, 1, 1)
        WHEN '2' THEN make_date(year::int, 4, 1)
        WHEN '3' THEN make_date(year::int, 7, 1)
        WHEN '4' THEN make_date(year::int, 10, 1)
    END AS reporting_period_start,

    CASE qtr
        WHEN '1' THEN make_date(year::int, 3, 31)
        WHEN '2' THEN make_date(year::int, 6, 30)
        WHEN '3' THEN make_date(year::int, 9, 30)
        WHEN '4' THEN make_date(year::int, 12, 31)
    END AS reporting_period_end,

    -- Extract components for grouping
    year::int AS year_num,
    qtr::int AS quarter_num
FROM raw_categoricals;

select * from fact_with_time;

-- Create a table for invalid or suspicious records
CREATE TABLE raw_table__invalid AS
SELECT *
FROM fact_with_time
WHERE
    -- Ownership code must be one of the official mappings
    own_code NOT IN ('Total Covered','Private','International Government',
                     'Local Government','State Government','Federal Government')
    OR qtrly_estabs < 0
    OR month1_emplvl < 0
    OR month2_emplvl < 0
    OR month3_emplvl < 0
    OR total_qtrly_wages < 0
    OR taxable_qtrly_wages < 0
    OR qtrly_contributions < 0
    OR avg_wkly_wage < 0;

select * from raw_table__invalid;


-- Create the final cleaned fact table for analysis
CREATE TABLE fact_qcew AS
SELECT
    area_fips,
    own_code,
    industry_code,
    agglvl_code,
    size_code,
    year_num,
    quarter_num,
    reporting_period_start,
    reporting_period_end,

    qtrly_estabs,
    month1_emplvl,
    month2_emplvl,
    month3_emplvl,
    total_qtrly_wages,
    taxable_qtrly_wages,
    qtrly_contributions,
    avg_wkly_wage,

    lq_qtrly_estabs,
    lq_month1_emplvl,
    lq_month2_emplvl,
    lq_month3_emplvl,
    lq_total_qtrly_wages,
    lq_taxable_qtrly_wages,
    lq_qtrly_contributions,
    lq_avg_wkly_wage,

    oty_qtrly_estabs_chg,
    oty_qtrly_estabs_pct_chg,
    oty_month1_emplvl_chg,
    oty_month1_emplvl_pct_chg,
    oty_month2_emplvl_chg,
    oty_month2_emplvl_pct_chg,
    oty_month3_emplvl_chg,
    oty_month3_emplvl_pct_chg,
    oty_total_qtrly_wages_chg,
    oty_total_qtrly_wages_pct_chg,
    oty_taxable_qtrly_wages_chg,
    oty_taxable_qtrly_wages_pct_chg,
    oty_qtrly_contributions_chg,
    oty_qtrly_contributions_pct_chg,
    oty_avg_wkly_wage_chg,
    oty_avg_wkly_wage_pct_chg
FROM fact_with_time;

select * from fact_qcew;

CREATE TABLE IF NOT EXISTS area_summary_clean (
  area_fips             TEXT,
  year                  INTEGER,
  qtr                   INTEGER,
  employment            NUMERIC,
  wages                 NUMERIC,
  avg_wage_per_employee NUMERIC,
  state_name            VARCHAR(100),
  county_name           VARCHAR(150)
);
--state summary
CREATE TABLE state_summary AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.qtrly_estabs) AS total_establishments,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(SUM(f.total_qtrly_wages) / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13,2) AS avg_weekly_wage
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;

--county summary 
CREATE TABLE county_summary AS
SELECT 
    a.state_name,
    a.county_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.qtrly_estabs) AS total_establishments,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(SUM(f.total_qtrly_wages) / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13,2) AS avg_weekly_wage
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, a.county_name, f.year_num, f.quarter_num;

--owner ships
CREATE TABLE ownership_summary AS
SELECT 
    a.state_name,
    f.own_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(SUM(f.total_qtrly_wages) / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13,2) AS avg_weekly_wage
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.own_code, f.year_num, f.quarter_num;

--industry summary
CREATE TABLE industry_summary AS
SELECT 
    f.industry_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.qtrly_estabs) AS total_establishments,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(SUM(f.total_qtrly_wages) / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13,2) AS avg_weekly_wage
FROM fact_qcew f
GROUP BY f.industry_code, f.year_num, f.quarter_num;

--size of the idustries
CREATE TABLE size_summary AS
SELECT 
    f.size_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.qtrly_estabs) AS total_establishments,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(SUM(f.total_qtrly_wages) / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13,2) AS avg_weekly_wage
FROM fact_qcew f
GROUP BY f.size_code, f.year_num, f.quarter_num;

--oer the year metrics
CREATE TABLE oty_summary AS
SELECT
    a.state_name,
    f.industry_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.oty_month1_emplvl_chg + f.oty_month2_emplvl_chg + f.oty_month3_emplvl_chg) / 3 AS avg_employment_change,
    AVG(f.oty_month1_emplvl_pct_chg + f.oty_month2_emplvl_pct_chg + f.oty_month3_emplvl_pct_chg) / 3 AS avg_employment_pct_change,
    SUM(f.oty_total_qtrly_wages_chg) AS total_wages_change,
    AVG(f.oty_total_qtrly_wages_pct_chg) AS avg_wages_pct_change
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.industry_code, f.year_num, f.quarter_num;

-----------------------------------------------------------------------------------

--kpi's for the data
--KPI – Employment per Establishment (kpi_emp_per_estab)

--👉 Formula:

--Average Employment per Establishment=Total Employment/Total Establishments

CREATE TABLE kpi_emp_per_estab AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.qtrly_estabs) AS total_establishments,
    ROUND(
        SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3)::NUMERIC 
        / NULLIF(SUM(f.qtrly_estabs),0), 2
    ) AS avg_emp_per_estab
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;


--KPI – Wage per Employee (kpi_wage_per_employee)

--👉 Formula:
--Wage per Employee=Total Quarterly Wages/Total Employment

CREATE TABLE kpi_wage_per_employee AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.total_qtrly_wages) AS total_wages,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    ROUND(
        SUM(f.total_qtrly_wages)::NUMERIC 
        / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0), 2
    ) AS wage_per_employee
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;

--KPI – Employment Growth Rate (kpi_emp_growth)

--👉 Formula (already available in your dataset, but aggregated):

--Growth %=Over-the-year Employment Change/Previous Employment×100

CREATE TABLE kpi_emp_growth AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.oty_month1_emplvl_chg + f.oty_month2_emplvl_chg + f.oty_month3_emplvl_chg) / 3 AS avg_emp_change,
    ROUND(AVG(f.oty_month1_emplvl_pct_chg + f.oty_month2_emplvl_pct_chg + f.oty_month3_emplvl_pct_chg) / 3, 2) 
        AS avg_emp_growth_pct
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;

--KPI – Wage Growth Rate (kpi_wage_growth)

--👉 Formula:

--Wage Growth %=Over-the-year Wages Change/Previous Wages×100

CREATE TABLE kpi_wage_growth AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.oty_total_qtrly_wages_chg) AS total_wage_change,
    ROUND(AVG(f.oty_total_qtrly_wages_pct_chg), 2) AS avg_wage_growth_pct
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;


--KPI – Employment-to-Wage Ratio (kpi_emp_wage_ratio)

--👉 Formula:

--Employment-to-Wage Ratio=Total Employment/Total Quarterly Wages

CREATE TABLE kpi_emp_wage_ratio AS
SELECT 
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    SUM(f.total_qtrly_wages) AS total_wages,
    ROUND(
        SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3)::NUMERIC 
        / NULLIF(SUM(f.total_qtrly_wages),0), 4
    ) AS emp_to_wage_ratio
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;


--KPI – Ownership Contribution to Employment (kpi_ownership_contrib)

--👉 Goal: % contribution of each ownership type (Private, Federal, State, Local)

CREATE TABLE kpi_ownership_contrib AS
SELECT 
    a.state_name,
    f.own_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    ROUND(
        100.0 * SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3)::NUMERIC
        / NULLIF(SUM(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3)) OVER (PARTITION BY a.state_name, f.year_num, f.quarter_num),0), 2
    ) AS pct_of_state_employment
FROM fact_qcew f
JOIN area_summary_clean a 
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.own_code, f.year_num, f.quarter_num;


--KPI – Employment Volatility (kpi_emp_volatility)

--👉 Formula: Standard Deviation of monthly employment within a quarter.

CREATE TABLE kpi_emp_volatility AS
SELECT
    a.state_name,
    f.year_num AS year,
    f.quarter_num AS qtr,
    STDDEV_POP(f.month1_emplvl) AS std_month1,
    STDDEV_POP(f.month2_emplvl) AS std_month2,
    STDDEV_POP(f.month3_emplvl) AS std_month3,
    STDDEV_POP((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl)/3) AS std_avg_employment
FROM fact_qcew f
JOIN area_summary_clean a
    ON f.area_fips = a.area_fips
GROUP BY a.state_name, f.year_num, f.quarter_num;

--KPI – Average Wage by Size Category (kpi_wage_size)

--👉 Compare wages between small vs medium vs large establishments.

CREATE TABLE kpi_wage_size AS
SELECT 
    f.size_code,
    f.year_num AS year,
    f.quarter_num AS qtr,
    SUM(f.total_qtrly_wages) AS total_wages,
    SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3) AS total_employment,
    ROUND(SUM(f.total_qtrly_wages)::NUMERIC / NULLIF(SUM((f.month1_emplvl + f.month2_emplvl + f.month3_emplvl) / 3),0) / 13, 2) AS avg_weekly_wage
FROM fact_qcew f
GROUP BY f.size_code, f.year_num, f.quarter_num;


