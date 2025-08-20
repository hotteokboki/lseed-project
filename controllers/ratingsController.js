const pgDatabase = require("../database");

exports.getFinanceRiskHeatmap = async ({ from = null, to = null, program = null } = {}) => {
  const sql = `
    WITH params AS (
      SELECT $1::date AS from_date, $2::date AS to_date, $3::text AS program_name
    ),
    se AS (
      SELECT se.se_id, se.team_name, COALESCE(se.abbr, se.team_name) AS abbr
      FROM socialenterprises se
      LEFT JOIN programs p ON p.program_id = se.program_id
      JOIN params pr ON TRUE
      WHERE pr.program_name IS NULL OR p.name = pr.program_name
    ),

    -- monthly inflows (sales + other revenue)
    cin AS (
      SELECT r.se_id,
             date_trunc('month', r.report_month)::date AS m,
             SUM(t.sales_amount + t.other_revenue_amount)::numeric AS inflow_sales
      FROM cash_in_report r
      JOIN cash_in_transaction t USING (cash_in_report_id)
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR r.report_month >= p.from_date)
        AND (p.to_date   IS NULL OR r.report_month <  p.to_date)
      GROUP BY 1,2
    ),

    -- monthly outflows (opex + inventory + liability + owner withdrawals)
    cout AS (
      SELECT r.se_id,
             date_trunc('month', r.report_month)::date AS m,
             SUM(t.cash_amount + t.inventory_amount + t.liability_amount + t.owners_withdrawal_amount)::numeric AS outflow_ops,
             SUM(t.inventory_amount)::numeric AS purchases
      FROM cash_out_report r
      JOIN cash_out_transaction t USING (cash_out_report_id)
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR r.report_month >= p.from_date)
        AND (p.to_date   IS NULL OR r.report_month <  p.to_date)
      GROUP BY 1,2
    ),

    -- monthly inventory values
    inv AS (
      SELECT i.se_id,
             i."month"::date AS m,
             SUM(COALESCE(begin_qty,0)*COALESCE(begin_unit_price,0))::numeric AS begin_val,
             SUM(COALESCE(final_qty,0)*COALESCE(final_unit_price,0))::numeric AS end_val
      FROM inventory_report i
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR i."month" >= p.from_date)
        AND (p.to_date   IS NULL OR i."month" <  p.to_date)
      GROUP BY 1,2
    ),

    -- derive COGS and avg inventory per month
    turn_raw AS (
      SELECT COALESCE(inv.se_id, cout.se_id, cin.se_id) AS se_id,
             COALESCE(inv.m,     cout.m,     cin.m)     AS m,
             GREATEST(COALESCE(inv.begin_val,0) + COALESCE(cout.purchases,0) - COALESCE(inv.end_val,0), 0)::numeric AS cogs,
             ((COALESCE(inv.begin_val,0) + COALESCE(inv.end_val,0))/2.0)::numeric AS avg_inventory
      FROM inv
      FULL JOIN cout ON cout.se_id = inv.se_id AND cout.m = inv.m
      FULL JOIN cin  ON cin.se_id  = COALESCE(inv.se_id, cout.se_id)
                    AND cin.m      = COALESCE(inv.m, cout.m)
    ),
    turn AS (
      SELECT se_id, m,
             CASE WHEN avg_inventory > 0 THEN (cogs/avg_inventory)::numeric ELSE NULL END AS turnover
      FROM turn_raw
    ),

    -- reporting completeness per month (0..3)
    rep AS (
      SELECT g.se_id,
             g."month"::date AS m,
             COUNT(DISTINCT report_type) AS rep_count
      FROM monthly_report_guard g
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR g."month" >= p.from_date)
        AND (p.to_date   IS NULL OR g."month" <  p.to_date)
      GROUP BY 1,2
    ),

    -- align months to avoid cartesian joins
    months AS (
      SELECT se_id, m FROM cin
      UNION SELECT se_id, m FROM cout
      UNION SELECT se_id, m FROM inv
      UNION SELECT se_id, m FROM rep
    ),
    monthly AS (
      SELECT
        mo.se_id, mo.m,
        COALESCE(ci.inflow_sales, 0)::numeric AS inflow_sales,
        COALESCE(co.outflow_ops,  0)::numeric AS outflow_ops,
        tu.turnover,
        COALESCE(rp.rep_count, 0)::int        AS rep_count
      FROM months mo
      LEFT JOIN cin  ci ON ci.se_id = mo.se_id AND ci.m = mo.m
      LEFT JOIN cout co ON co.se_id = mo.se_id AND co.m = mo.m
      LEFT JOIN turn tu ON tu.se_id = mo.se_id AND tu.m = mo.m
      LEFT JOIN rep  rp ON rp.se_id = mo.se_id AND rp.m = mo.m
    ),

    -- aggregate to per-SE over the selected period
    per_se AS (
      SELECT s.se_id, s.team_name, s.abbr,
             SUM(mo.inflow_sales)  AS inflow_total,
             SUM(mo.outflow_ops)   AS outflow_total,
             AVG(mo.turnover)      AS avg_turnover,
             AVG(mo.rep_count/3.0) AS reporting_rate
      FROM se s
      LEFT JOIN monthly mo ON mo.se_id = s.se_id
      GROUP BY s.se_id, s.team_name, s.abbr
    ),

    -- score bands
    scored AS (
      SELECT *,
        (inflow_total - outflow_total) AS net_total,
        CASE WHEN inflow_total <= 0 THEN 1
             WHEN (inflow_total - outflow_total)/NULLIF(inflow_total,0) >= 0.15 THEN 5
             WHEN (inflow_total - outflow_total)/NULLIF(inflow_total,0) >= 0.05 THEN 4
             WHEN (inflow_total - outflow_total)/NULLIF(inflow_total,0) >= 0    THEN 3
             WHEN (inflow_total - outflow_total)/NULLIF(inflow_total,0) >= -0.10 THEN 2
             ELSE 1 END AS cash_margin_score,
        CASE WHEN outflow_total <= 0 AND inflow_total > 0 THEN 5
             WHEN outflow_total <= 0 THEN 1
             WHEN inflow_total/NULLIF(outflow_total,0) >= 1.5 THEN 5
             WHEN inflow_total/NULLIF(outflow_total,0) >= 1.2 THEN 4
             WHEN inflow_total/NULLIF(outflow_total,0) >= 1.0 THEN 3
             WHEN inflow_total/NULLIF(outflow_total,0) >= 0.8 THEN 2
             ELSE 1 END AS inout_ratio_score,
        CASE WHEN avg_turnover IS NULL THEN 1
             WHEN avg_turnover >= 0.50 THEN 5
             WHEN avg_turnover >= 0.35 THEN 4
             WHEN avg_turnover >= 0.25 THEN 3
             WHEN avg_turnover >= 0.15 THEN 2
             ELSE 1 END AS turnover_score,
        ROUND(LEAST(GREATEST(reporting_rate,0),1)*5,2) AS reporting_score
      FROM per_se
    )
    SELECT
      se_id,
      team_name,
      abbr,
      cash_margin_score AS "Cash Margin",
      inout_ratio_score AS "In/Out Ratio",
      turnover_score    AS "Inventory Turnover",
      reporting_score   AS "Reporting",
      (cash_margin_score + inout_ratio_score + turnover_score + reporting_score) AS risk_total
    FROM scored
    ORDER BY risk_total ASC;
  `;

  const { rows } = await pgDatabase.query(sql, [from, to, program]);
  return rows ?? [];
};


exports.getTopStarTrend = async ({ from = null, to = null, se_id = null } = {}) => {
  // validate se_id if provided
  if (
    se_id &&
    !/^[0-9a-fA-F-]{8}-[0-9a-fA-F-]{4}-[1-5][0-9a-fA-F-]{3}-[89abAB][0-9a-fA-F-]{3}-[0-9a-fA-F-]{12}$/.test(se_id)
  ) {
    throw new Error("INVALID_SE_ID");
  }

  const sql = `
    WITH params AS (
      SELECT
        $1::date AS from_date,
        $2::date AS to_date,
        $3::uuid AS only_se
    ),
    filt AS (
      SELECT r.se_id, r.m_start, r.score_0_100
      FROM se_monthly_ratings r, params p
      WHERE (p.from_date IS NULL OR r.m_start >= p.from_date)
        AND (p.to_date   IS NULL OR r.m_start <  p.to_date)   -- end-exclusive; use <= for inclusive
        AND (p.only_se   IS NULL OR r.se_id = p.only_se)
    ),
    overall AS (
      SELECT se_id, AVG(score_0_100) AS avg_score
      FROM filt
      GROUP BY se_id
    ),
    ranked AS (
      SELECT
        se_id,
        avg_score,
        ROW_NUMBER() OVER (ORDER BY avg_score DESC, se_id) AS rn
      FROM overall
    )
    SELECT
      f.se_id,
      f.m_start::date                 AS month,
      ROUND((f.score_0_100/20.0)*2)/2 AS stars_half,    -- 0..5 in 0.5 steps
      ROUND(f.score_0_100, 1)         AS score_0_100
    FROM filt f
    JOIN ranked k USING (se_id)
    ORDER BY month, f.se_id;
  `;

  const { rows } = await pgDatabase.query(sql, [from, to, se_id]);
  return rows ?? [];
};

// Overall monthly cash flow across all SEs (no se_id filtering)
exports.getOverallCashFlow = async ({ from = null, to = null, opening_cash = 0 } = {}) => {
  const sql = `
    -- OVERALL CASH FLOW (Monthly, all SEs)
    -- $1: from::date (nullable)
    -- $2: to::date   (nullable, END-EXCLUSIVE)
    -- $3: opening_cash::numeric (nullable → defaults to 0)

    WITH params AS (
      SELECT
        $1::date AS from_date,
        $2::date AS to_date,
        COALESCE($3::numeric, 0)::numeric AS opening_cash
    ),

    -- --------- INFLOWS (overall) ----------
    cin AS (
      SELECT
        date_trunc('month', t.transaction_date)::date AS m_start,
        COALESCE(t.sales_amount, 0)::numeric          AS sales_in,
        COALESCE(t.other_revenue_amount, 0)::numeric  AS other_rev_in,
        COALESCE(t.liability_amount, 0)::numeric      AS loan_in,
        COALESCE(t.owners_capital_amount, 0)::numeric AS capital_in,
        COALESCE(t.cash_amount, 0)::numeric           AS cash_misc_in
      FROM cash_in_transaction t
      JOIN cash_in_report r ON r.cash_in_report_id = t.cash_in_report_id
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR t.transaction_date >= p.from_date)
        AND (p.to_date   IS NULL OR t.transaction_date <  p.to_date)
    ),
    cin_m AS (
      SELECT
        m_start,
        SUM(sales_in)         AS sales_in,
        SUM(other_rev_in)     AS other_rev_in,
        SUM(loan_in)          AS loan_in,
        SUM(capital_in)       AS capital_in,
        SUM(cash_misc_in)     AS cash_misc_in,
        SUM(sales_in + other_rev_in + loan_in + capital_in + cash_misc_in) AS total_in
      FROM cin
      GROUP BY m_start
    ),

    -- --------- OUTFLOWS (overall) ----------
    cout AS (
      SELECT
        date_trunc('month', t.transaction_date)::date AS m_start,
        COALESCE(t.cash_amount, 0)::numeric              AS opex_out,
        COALESCE(t.inventory_amount, 0)::numeric         AS inventory_out,
        COALESCE(t.liability_amount, 0)::numeric         AS debt_service_out,
        COALESCE(t.owners_withdrawal_amount, 0)::numeric AS owners_draw_out
      FROM cash_out_transaction t
      JOIN cash_out_report r ON r.cash_out_report_id = t.cash_out_report_id
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR t.transaction_date >= p.from_date)
        AND (p.to_date   IS NULL OR t.transaction_date <  p.to_date)
    ),
    cout_m AS (
      SELECT
        m_start,
        SUM(opex_out)          AS opex_out,
        SUM(inventory_out)     AS inventory_out,
        SUM(debt_service_out)  AS debt_service_out,
        SUM(owners_draw_out)   AS owners_draw_out,
        SUM(opex_out + inventory_out + debt_service_out + owners_draw_out) AS total_out
      FROM cout
      GROUP BY m_start
    ),

    -- --------- Month union, join inflow/outflow ----------
    months AS (
      SELECT m_start FROM cin_m
      UNION
      SELECT m_start FROM cout_m
    ),
    m AS (
      SELECT
        mo.m_start,
        COALESCE(ci.sales_in,        0) AS sales_in,
        COALESCE(ci.other_rev_in,    0) AS other_rev_in,
        COALESCE(ci.loan_in,         0) AS loan_in,
        COALESCE(ci.capital_in,      0) AS capital_in,
        COALESCE(ci.cash_misc_in,    0) AS cash_misc_in,
        COALESCE(ci.total_in,        0) AS total_in,

        COALESCE(co.opex_out,        0) AS opex_out,
        COALESCE(co.inventory_out,   0) AS inventory_out,
        COALESCE(co.debt_service_out,0) AS debt_service_out,
        COALESCE(co.owners_draw_out, 0) AS owners_draw_out,
        COALESCE(co.total_out,       0) AS total_out
      FROM months mo
      LEFT JOIN cin_m  ci USING (m_start)
      LEFT JOIN cout_m co USING (m_start)
    ),
    netted AS (
      SELECT
        m_start,
        sales_in, other_rev_in, loan_in, capital_in, cash_misc_in, total_in,
        opex_out, inventory_out, debt_service_out, owners_draw_out, total_out,
        (total_in - total_out) AS net,
        GREATEST(total_out - total_in, 0) AS burn_this_month
      FROM m
    ),
    running AS (
      SELECT
        n.*,
        -- Opening cash (overall) + cumulative net
        ( (SELECT opening_cash FROM params)
          + SUM(n.net) OVER (ORDER BY n.m_start
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS cash_on_hand,
        AVG(n.burn_this_month) OVER (ORDER BY n.m_start
                                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS burn_ma3
      FROM netted n
    )
    SELECT
      m_start::date AS date,
      total_in  AS inflow,
      total_out AS outflow,
      net,
      cash_on_hand,
      CASE WHEN net < 0    THEN cash_on_hand / NULLIF(ABS(net), 0) END AS runway_months_instant,
      CASE WHEN burn_ma3 > 0 THEN cash_on_hand / burn_ma3 END           AS runway_months_ma3
    FROM running
    ORDER BY date;
  `;

  const { rows } = await pgDatabase.query(sql, [from, to, opening_cash]);
  return rows ?? [];
};

// Overall "top selling" items across all SEs (proxy = inventory movement)
exports.getTopSellingItemsOverall = async ({
  from   = null,      // "YYYY-MM-DD" (optional)
  to     = null,      // "YYYY-MM-DD" (optional, END-EXCLUSIVE)
  metric = "value",   // "value" | "qty"
} = {}) => {
  const metricKey = String(metric).toLowerCase() === "qty" ? "qty" : "value";

  const sql = `
    /*
      Top-moving items across all social enterprises.

      moved_qty (per month) = GREATEST(begin_qty - final_qty, 0)
      moved_value           = moved_qty * item.item_price (proxy for sales value)
    */
    WITH params AS (
      SELECT $1::date AS from_date, $2::date AS to_date, $3::text AS metric
    ),
    movement AS (
      SELECT
        ir.item_id,
        i.item_name,
        COALESCE(i.item_price, 0)::numeric AS unit_price,
        SUM(GREATEST(COALESCE(ir.begin_qty,0) - COALESCE(ir.final_qty,0), 0))::numeric AS moved_qty
      FROM inventory_report ir
      JOIN item i ON i.item_id = ir.item_id
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR ir."month" >= p.from_date)
        AND (p.to_date   IS NULL OR ir."month" <  p.to_date)
      GROUP BY ir.item_id, i.item_name, i.item_price
    ),
    scored AS (
      SELECT
        item_id,
        item_name,
        unit_price,
        moved_qty,
        (moved_qty * unit_price)::numeric AS moved_value,
        CASE WHEN (SELECT metric FROM params) = 'qty' THEN moved_qty
             ELSE moved_qty * unit_price
        END AS score
      FROM movement
    )
    SELECT
      item_id,
      item_name,
      unit_price,
      moved_qty,
      moved_value
    FROM scored
    ORDER BY score DESC, item_name;
  `;

  const { rows } = await pgDatabase.query(sql, [from, to, metricKey]);
  return rows ?? [];
};

exports.getInventoryTurnoverOverall = async ({ from = null, to = null } = {}) => {
  const sql = `
    WITH params AS (
      SELECT $1::date AS from_date, $2::date AS to_date
    ),
    base AS (
      SELECT
        ir."month"::date AS month,
        COALESCE(ir.begin_qty, 0)::numeric AS bq,
        COALESCE(ir.final_qty, 0)::numeric AS fq,
        -- prefer begin cost for COGS; fallback to final/item price
        COALESCE(ir.begin_unit_price, ir.final_unit_price, i.item_price, 0)::numeric AS bprice,
        COALESCE(ir.final_unit_price, ir.begin_unit_price, i.item_price, 0)::numeric AS fprice
      FROM inventory_report ir
      JOIN item i ON i.item_id = ir.item_id
      JOIN params p ON TRUE
      WHERE (p.from_date IS NULL OR ir."month" >= p.from_date)
        AND (p.to_date   IS NULL OR ir."month" <  p.to_date)
    ),
    per_item AS (
      SELECT
        month,
        (bq * bprice)                      AS begin_value,
        (fq * fprice)                      AS end_value,
        GREATEST(bq - fq, 0)               AS sold_qty,
        GREATEST(bq - fq, 0) * bprice      AS cogs
      FROM base
    ),
    monthly AS (
      SELECT
        month,
        SUM(begin_value) AS begin_value,
        SUM(end_value)   AS end_value,
        SUM(sold_qty)    AS sold_qty,
        SUM(cogs)        AS cogs
      FROM per_item
      GROUP BY month
    )
    SELECT
      m.month,
      ROUND(m.cogs, 2)                                     AS cogs,
      ROUND(m.begin_value, 2)                              AS begin_value,
      ROUND(m.end_value, 2)                                AS end_value,
      ROUND( (m.begin_value + m.end_value) / 2.0, 2 )      AS avg_inventory,
      CASE
        WHEN (m.begin_value + m.end_value) > 0
          THEN ROUND( m.cogs / ((m.begin_value + m.end_value)/2.0), 4)
        ELSE NULL
      END                                                  AS turnover,
      ((date_trunc('month', m.month) + INTERVAL '1 month')::date - date_trunc('month', m.month)::date) AS days_in_month,
      CASE
        WHEN (m.begin_value + m.end_value) > 0 AND m.cogs > 0
          THEN ROUND(
            ((date_trunc('month', m.month) + INTERVAL '1 month')::date - date_trunc('month', m.month)::date)::numeric
             / (m.cogs / ((m.begin_value + m.end_value)/2.0))
          , 1)
        ELSE NULL
      END                                                  AS dio_days
    FROM monthly m
    ORDER BY m.month;
  `;

  const { rows } = await pgDatabase.query(sql, [from, to]);
  return rows ?? [];
};