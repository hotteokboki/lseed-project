const pgDatabase = require('../database.js'); // Import PostgreSQL client

exports.getEvaluations = async (program = null) => {
  try {
    const values = [];
    let programJoin = '';
    let programFilter = '';

    if (program && program !== "null") {
      programJoin = `JOIN programs AS p ON p.program_id = se.program_id`;
      programFilter = `AND p.name = $1`;
      values.push(program);
    }

    const query = `
      SELECT 
          e.evaluation_id,
          m.mentor_firstname || ' ' || m.mentor_lastname AS evaluator_name,
          se.team_name AS social_enterprise,
          TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date,
          e."isAcknowledge" AS acknowledged,
          e.evaluation_type
      FROM 
          evaluations AS e
      JOIN 
          mentors AS m ON e.mentor_id = m.mentor_id
      JOIN 
          socialenterprises AS se ON e.se_id = se.se_id
      ${programJoin}
      WHERE 
          e.evaluation_type = 'Social Enterprise'
          ${programFilter}
      ORDER BY e.created_at DESC;
    `;

    const result = await pgDatabase.query(query, values);

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching evaluations:", error);
    return [];
  }
};

exports.getEvaluationsByMentorID = async (mentor_id) => {
    try {
        const query = `
            SELECT 
                e.evaluation_id,
                se.team_name AS evaluator_name, -- ✅ Social Enterprise evaluating the mentor
                m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name, -- ✅ Mentor being evaluated
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                e."isAcknowledge" AS acknowledged
            FROM 
                evaluations AS e
            JOIN 
                mentors AS m ON e.mentor_id = m.mentor_id -- ✅ Get mentor details
            JOIN 
                socialenterprises AS se ON e.se_id = se.se_id -- ✅ Get SE details
            WHERE	
                e.mentor_id = $1 AND -- ✅ Filter by a specific mentor
                e.evaluation_type = 'Mentors' -- ✅ Ensure it's a mentor evaluation
            ORDER BY 
                e.created_at DESC; -- ✅ Order by most recent evaluations
        `;

        const values = [mentor_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
        return [];
    }
};

exports.getAllMentorTypeEvaluations = async () => {
    try {
        const query = `
            SELECT 
                e.evaluation_id,
                se.team_name AS evaluator_name, -- ✅ Social Enterprise evaluating the mentor
                m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name, -- ✅ Mentor being evaluated
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                e."isAcknowledge" AS acknowledged
            FROM 
                evaluations AS e
            JOIN 
                mentors AS m ON e.mentor_id = m.mentor_id -- ✅ Get mentor details
            JOIN 
                socialenterprises AS se ON e.se_id = se.se_id -- ✅ Get SE details
            WHERE	
                e.evaluation_type = 'Mentors' -- ✅ Ensure it's a mentor evaluation
            ORDER BY 
                e.created_at DESC; -- ✅ Order by most recent evaluations
        `;

        const result = await pgDatabase.query(query);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
        return [];
    }
};

exports.getEvaluationsMadeByMentor = async (mentor_id) => {
    try {
        const query = `
            SELECT 
                e.evaluation_id,
                m.mentor_firstname || ' ' || m.mentor_lastname AS evaluator_name, -- ✅ Mentor who evaluated the SE
                se.team_name AS social_enterprise, -- ✅ SE being evaluated
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                e."isAcknowledge" AS acknowledged
            FROM 
                evaluations AS e
            JOIN 
                mentors AS m ON e.mentor_id = m.mentor_id -- ✅ Get mentor details
            JOIN 
                socialenterprises AS se ON e.se_id = se.se_id -- ✅ Get SE details
            WHERE	
                e.mentor_id = $1 AND -- ✅ Filter by a specific mentor
                e.evaluation_type = 'Social Enterprise' -- ✅ Ensure it's an SE evaluation
            ORDER BY 
                e.created_at DESC; -- ✅ Order by most recent evaluations
        `;

        const values = [mentor_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
        return [];
    }
};

exports.getRecentEvaluationsMadeByMentor = async (mentor_id) => {
    try {
        const query = `
            SELECT 
                e.evaluation_id,
                m.mentor_firstname || ' ' || m.mentor_lastname AS evaluator_name, -- ✅ Mentor who evaluated the SE
                se.team_name AS social_enterprise, -- ✅ SE being evaluated
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                e."isAcknowledge" AS acknowledged
            FROM 
                evaluations AS e
            JOIN 
                mentors AS m ON e.mentor_id = m.mentor_id -- ✅ Get mentor details
            JOIN 
                socialenterprises AS se ON e.se_id = se.se_id -- ✅ Get SE details
            WHERE	
                e.mentor_id = $1 -- ✅ Filter by a specific mentor
                AND e.evaluation_type = 'Social Enterprise' -- ✅ Ensure it's an SE evaluation
            ORDER BY 
                e.created_at DESC -- ✅ Order by most recent evaluations
            LIMIT 10; -- ✅ Get only the latest 10 evaluations

        `;

        const values = [mentor_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
        return [];
    }
};

exports.getEvaluationsBySEID = async (se_id) => {
    try {
        const query = `
            SELECT 
                e.evaluation_id,
                m.mentor_firstname || ' ' || m.mentor_lastname AS evaluator_name,
                se.team_name AS social_enterprise,
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                e."isAcknowledge" AS acknowledged
            FROM 
                evaluations AS e
            JOIN 
                mentors AS m ON e.mentor_id = m.mentor_id
            JOIN 
                socialenterprises AS se ON e.se_id = se.se_id
            WHERE	
                e.se_id = $1 AND
				evaluation_type = 'Social Enterprise';
        `;

        const values = [se_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluations:", error);
        return [];
    }
};

exports.getEvaluationDetails = async (evaluation_id) => {
    try {
        console.log(`Fetching evaluation details for Evaluation ID: ${evaluation_id}`);

        const query = `
            SELECT 
                e.evaluation_id,
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                se.team_name AS social_enterprise, -- ✅ Social Enterprise being evaluated
                m.mentor_firstname || ' ' || m.mentor_lastname AS evaluator_name, -- ✅ Evaluator (Mentor)
                ec.category_name,
                ec.rating AS star_rating,
                ec.additional_comment,
                COALESCE(
                    JSON_AGG(DISTINCT esc.comment) FILTER (WHERE esc.comment IS NOT NULL), 
                    '[]'
                ) AS selected_comments
            FROM evaluations e
            JOIN socialenterprises se ON e.se_id = se.se_id -- ✅ Get evaluated SE
            JOIN mentors m ON e.mentor_id = m.mentor_id -- ✅ Get mentor who evaluated the SE
            LEFT JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
            LEFT JOIN evaluation_selected_comments esc ON ec.evaluation_category_id = esc.evaluation_category_id
            WHERE e.evaluation_id = $1 AND e.evaluation_type = 'Social Enterprise'
            GROUP BY e.evaluation_id, e.created_at, se.team_name, evaluator_name, ec.category_name, ec.rating, ec.additional_comment;
        `;

        const values = [evaluation_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluation details:", error);
        return [];
    }
};

exports.getEvaluationDetailsForMentorEvaluation = async (evaluation_id) => {
    try {
        console.log(`Fetching evaluation details for Evaluation ID: ${evaluation_id}`);

        const query = `
            SELECT 
                e.evaluation_id,
                TO_CHAR(e.created_at, 'FMMonth DD, YYYY') AS evaluation_date, -- ✅ Formatted date
                se.team_name AS evaluator_name, -- ✅ SE is the evaluator
                m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name,
                ec.category_name,
                ec.rating AS star_rating,
                ec.additional_comment
            FROM evaluations e
            JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id -- ✅ Includes categories & ratings
            JOIN mentors m ON e.mentor_id = m.mentor_id -- ✅ Gets mentor names
            JOIN socialenterprises se ON e.se_id = se.se_id -- ✅ Gets evaluator name (SE Team Name)
            WHERE e.evaluation_id = $1 -- ✅ Filters by a specific evaluation ID
            AND e.evaluation_type = 'Mentors' -- ✅ Ensures it's a mentor evaluation
            ORDER BY e.created_at DESC;
        `;

        const values = [evaluation_id];
        const result = await pgDatabase.query(query, values);

        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching evaluation details:", error);
        return [];
    }
};

exports.getTopSEPerformance = async (period = "overall", program = null, mentor_id = null, se_id = null) => {
    try {
        let query;
        const values = [];
        let idx = 1;

        // Dynamic filters
        let programFilter = '';
        let mentorFilter = '';
        let mentorshipsJoin = '';
        let seidFilter = '';

        if (program && program !== "null") {
            programFilter = `AND p.name = $${idx++}`;
            values.push(program);
        }

        if (mentor_id && mentor_id !== "null") {
            mentorshipsJoin = `LEFT JOIN mentorships m ON e.se_id = m.se_id`;
            mentorFilter = `AND m.mentor_id = $${idx++}`;
            values.push(mentor_id);
        }

        if (se_id && se_id !== "null") {
            seidFilter = `AND e.se_id = $${idx++}`;
            values.push(se_id);
        }

        const baseCTEs = `
            ${mentorshipsJoin}
            WHERE
                e.evaluation_type = 'Social Enterprise'
                ${programFilter}
                ${mentorFilter}
                ${seidFilter}
        `;

        // Main query based on period
        if (period === "quarterly") {
            query = `
                WITH QuarterBounds AS (
                    SELECT 
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' AS latest_quarter,
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' AS previous_quarter
                ),
                FilteredEvaluations AS (
                    SELECT
                        e.se_id,
                        s.abbr AS social_enterprise,
                        ec.rating,
                        DATE_TRUNC('quarter', e.created_at) AS quarter_start
                    FROM evaluations e
                    JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                    JOIN socialenterprises s ON e.se_id = s.se_id
                    JOIN programs p ON p.program_id = s.program_id
                    CROSS JOIN QuarterBounds qb
                    ${baseCTEs}
                    AND DATE_TRUNC('quarter', e.created_at) IN (qb.latest_quarter, qb.previous_quarter)
                ),
                QuarterlyRatings AS (
                    SELECT
                        se_id,
                        social_enterprise,
                        quarter_start,
                        ROUND(AVG(rating), 2) AS avg_rating,
                        COUNT(*) AS eval_count
                    FROM FilteredEvaluations
                    GROUP BY se_id, social_enterprise, quarter_start
                ),
                TopSEsWeighted AS (
                    SELECT 
                        se_id,
                        social_enterprise,
                        SUM(avg_rating * eval_count) / SUM(eval_count) AS weighted_avg_rating
                    FROM QuarterlyRatings
                    GROUP BY se_id, social_enterprise
                    ORDER BY weighted_avg_rating DESC
                )
                SELECT 
                    q.se_id,
                    q.social_enterprise,
                    q.quarter_start,
                    q.avg_rating,
                    CASE 
                        WHEN q.quarter_start = (SELECT latest_quarter FROM QuarterBounds) THEN 'latest quarter'
                        ELSE 'previous quarter'
                    END AS period
                FROM QuarterlyRatings q
                JOIN TopSEsWeighted t ON q.se_id = t.se_id
                ORDER BY t.weighted_avg_rating DESC, q.social_enterprise, q.quarter_start;
            `;
        } else if (period === "yearly") {
            query = `
                WITH QuarterBounds AS (
                    SELECT 
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '0 months' AS q1,
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' AS q2,
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 months' AS q3,
                        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' AS q4
                ),
                LatestFourQuarters AS (
                    SELECT unnest(ARRAY[
                        (SELECT q1 FROM QuarterBounds),
                        (SELECT q2 FROM QuarterBounds),
                        (SELECT q3 FROM QuarterBounds),
                        (SELECT q4 FROM QuarterBounds)
                    ]) AS quarter_start
                ),
                FilteredEvaluations AS (
                    SELECT
                        e.se_id,
                        s.abbr AS social_enterprise,
                        ec.rating,
                        DATE_TRUNC('quarter', e.created_at) AS quarter_start
                    FROM evaluations e
                    JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                    JOIN socialenterprises s ON e.se_id = s.se_id
                    JOIN programs p ON p.program_id = s.program_id
                    ${baseCTEs}
                    AND DATE_TRUNC('quarter', e.created_at) IN (SELECT quarter_start FROM LatestFourQuarters)
                ),
                QuarterlyRatings AS (
                    SELECT
                        se_id,
                        social_enterprise,
                        quarter_start,
                        ROUND(AVG(rating), 2) AS avg_rating,
                        COUNT(*) AS eval_count
                    FROM FilteredEvaluations
                    GROUP BY se_id, social_enterprise, quarter_start
                ),
                TopSEsWeighted AS (
                    SELECT 
                        se_id,
                        social_enterprise,
                        SUM(avg_rating * eval_count) / SUM(eval_count) AS weighted_avg_rating
                    FROM QuarterlyRatings
                    GROUP BY se_id, social_enterprise
                    ORDER BY weighted_avg_rating DESC
                )
                SELECT 
                    q.se_id,
                    q.social_enterprise,
                    q.quarter_start,
                    q.avg_rating,
                    CASE 
                        WHEN q.quarter_start = (SELECT q1 FROM QuarterBounds) THEN 'Q4 (latest)'
                        WHEN q.quarter_start = (SELECT q2 FROM QuarterBounds) THEN 'Q3'
                        WHEN q.quarter_start = (SELECT q3 FROM QuarterBounds) THEN 'Q2'
                        WHEN q.quarter_start = (SELECT q4 FROM QuarterBounds) THEN 'Q1'
                        ELSE 'Other'
                    END AS period
                FROM QuarterlyRatings q
                JOIN TopSEsWeighted t ON q.se_id = t.se_id
                ORDER BY t.weighted_avg_rating DESC, q.social_enterprise, q.quarter_start;
            `;
        } else if (period === "overall") {
            query = `
                WITH EvaluationRange AS (
                    SELECT 
                        DATE_TRUNC('quarter', MIN(created_at)) AS earliest_quarter,
                        DATE_TRUNC('quarter', MAX(created_at)) AS latest_quarter
                    FROM evaluations
                    WHERE evaluation_type = 'Social Enterprise'
                ),
                AllQuarters AS (
                    SELECT generate_series(
                        (SELECT earliest_quarter FROM EvaluationRange),
                        (SELECT latest_quarter FROM EvaluationRange),
                        INTERVAL '3 months'
                    ) AS quarter_start
                ),
                FilteredEvaluations AS (
                    SELECT
                        e.se_id,
                        s.abbr AS social_enterprise,
                        ec.rating,
                        DATE_TRUNC('quarter', e.created_at) AS quarter_start
                    FROM evaluations e
                    JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                    JOIN socialenterprises s ON e.se_id = s.se_id
                    JOIN programs p ON p.program_id = s.program_id
                    ${baseCTEs}
                    AND DATE_TRUNC('quarter', e.created_at) IN (SELECT quarter_start FROM AllQuarters)
                ),
                QuarterlyRatings AS (
                    SELECT
                        se_id,
                        social_enterprise,
                        quarter_start,
                        ROUND(AVG(rating), 2) AS avg_rating,
                        COUNT(*) AS eval_count
                    FROM FilteredEvaluations
                    GROUP BY se_id, social_enterprise, quarter_start
                ),
                TopSEsWeighted AS (
                    SELECT 
                        se_id,
                        social_enterprise,
                        SUM(avg_rating * eval_count) / SUM(eval_count) AS weighted_avg_rating
                    FROM QuarterlyRatings
                    GROUP BY se_id, social_enterprise
                    ORDER BY weighted_avg_rating DESC
                )
                SELECT 
                    q.se_id,
                    q.social_enterprise,
                    q.quarter_start,
                    q.avg_rating,
                    TO_CHAR(q.quarter_start, '"Q"Q YYYY') AS period
                FROM QuarterlyRatings q
                JOIN TopSEsWeighted t ON q.se_id = t.se_id
                ORDER BY t.weighted_avg_rating DESC, q.social_enterprise, q.quarter_start;
            `;
        } else {
            throw new Error("Invalid period specified.");
        }

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error in getTopSEPerformance:", error);
        return [];
    }
};

exports.getCommonChallengesBySEID = async (se_id) => {
    try {
        const query = `
            WITH low_rated_categories AS (
                SELECT 
                    ec.category_name AS category,
                    ec.rating,
                    COUNT(ec.evaluation_category_id) AS count
                FROM evaluations e
                JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                WHERE e.se_id = $1 -- ✅ Filter by the specific SE
                AND e.evaluation_type = 'Social Enterprise'
                AND ec.rating <= 2  
                GROUP BY ec.category_name, ec.rating
            ),
            ranked_low_ratings AS (
                SELECT 
                    category, 
                    rating, 
                    count,
                    RANK() OVER (PARTITION BY category ORDER BY count DESC, rating ASC) AS rank
                FROM low_rated_categories
            ),
            top_low_rated AS (
                SELECT 
                    category,
                    rating,
                    count
                FROM ranked_low_ratings
                WHERE rank = 1  -- ✅ Select only the most common low rating per category
            ),
            total_top AS (
                SELECT SUM(count) AS top_total FROM top_low_rated
            ),
            final_result AS (
                SELECT 
                    tlr.category, 
                    tlr.rating,
                    MIN(esc.comment) AS comment,  -- ✅ Select only one distinct comment (avoiding repetition)
                    tlr.count,
                    ROUND(tlr.count * 100.0 / COALESCE(tt.top_total, 1), 0) AS percentage
                FROM top_low_rated tlr
                CROSS JOIN total_top tt
                LEFT JOIN evaluation_categories ec ON tlr.category = ec.category_name AND tlr.rating = ec.rating
                LEFT JOIN evaluation_selected_comments esc ON ec.evaluation_category_id = esc.evaluation_category_id
                GROUP BY tlr.category, tlr.rating, tlr.count, tt.top_total
            )
            SELECT DISTINCT * FROM final_result
            ORDER BY count DESC;
        `;
        const values = [se_id];

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getStatsForHeatmap = async (period = "overall", program = null) => {
    try {
        let dateCondition = "";
        let programFilter = program ? `WHERE p.name = '${program}'` : '';


        if (period === "quarterly") {
            dateCondition = `
                e.created_at >= (CURRENT_DATE - INTERVAL '3 months')
                AND e.created_at < CURRENT_DATE
            `;
        } else if (period === "yearly") {
            dateCondition = `
                e.created_at >= (CURRENT_DATE - INTERVAL '12 months')
                AND e.created_at < CURRENT_DATE
            `;
        } else if (period === "overall") {
            dateCondition = "1 = 1"; // No date filter, fetches all data
        }

        const query = `
            WITH recent_evaluations AS (
                SELECT 
                    e.se_id,  
                    ec.category_name,
                    AVG(ec.rating) AS avg_rating
                FROM public.evaluation_categories ec
                JOIN public.evaluations e ON ec.evaluation_id = e.evaluation_id
                WHERE 
                    ${dateCondition}
                    AND e.evaluation_type = 'Social Enterprise'
                GROUP BY e.se_id, ec.category_name
            )
            SELECT 
                ROW_NUMBER() OVER () AS row_id,  
                se.se_id,
                TRIM(se.team_name) AS team_name,
                TRIM(COALESCE(se.abbr, se.team_name)) AS abbr,  
                jsonb_object_agg(re.category_name, re.avg_rating) AS category_ratings  
            FROM recent_evaluations re
            INNER JOIN public.socialenterprises se ON re.se_id = se.se_id  -- 🔥 INNER JOIN ensures only SEs with data
			JOIN public.programs p ON p.program_id = se.program_id
			${programFilter}
            GROUP BY se.se_id, se.team_name, se.abbr
            ORDER BY se.abbr;
        `;
        const result = await pgDatabase.query(query);

        return result.rows.map(row => ({
            team_name: row.team_name,
            abbr: row.abbr,
            ...row.category_ratings
        }));
    } catch (error) {
        console.error("❌ Error fetching heatmap data:", error);
        return [];
    }
};

exports.getPermanceScoreBySEID = async (se_id) => {
    try {
        const query = `
            SELECT 
                e.se_id,
                ec.category_name,
                ec.rating,
                COUNT(ec.rating) AS rating_count
            FROM evaluations e
            JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
            WHERE e.se_id = $1 AND e.evaluation_type = 'Social Enterprise'
            GROUP BY e.se_id, ec.category_name, ec.rating
            ORDER BY ec.category_name, ec.rating;
        `;
        const values = [se_id];

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getAverageScoreForAllSEPerCategory = async (program = null) => {
    try {
        let programFilter = program ? `AND p.name = '${program}'` : '';

        const query = `
            SELECT 
                ec.category_name AS category,
                ROUND(AVG(ec.rating), 2) AS score
            FROM evaluations e
            JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
            JOIN socialenterprises AS s ON s.se_id = e.se_id
            JOIN programs AS p ON p.program_id = s.program_id
            WHERE e.evaluation_type = 'Social Enterprise'
            ${programFilter}
            GROUP BY ec.category_name
            ORDER BY category;
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getMentorEvaluationCount = async (mentor_id) => {
    try {
        const query = `
            SELECT 
                COUNT(DISTINCT evaluation_id) AS total_evaluations
            FROM evaluations
            WHERE evaluation_type = 'Mentors' AND mentor_id = $1;
        `;
        const result = await pgDatabase.query(query, [mentor_id]);
        return result.rows[0]?.total_evaluations || 0; // Return count or 0 if no evaluations exist
    } catch (error) {
        console.error("❌ Error fetching mentor evaluation count:", error);
        return 0; // Return 0 in case of an error
    }
};

exports.getCategoryHealthOverview = async ({ programId = null, period = "3m", start = null, end = null } = {}) => {
  const sql = `
    WITH params AS (
      SELECT
        /* from_date */
        CASE
          WHEN $3::date IS NOT NULL AND $4::date IS NOT NULL
            THEN date_trunc('month', $3::date)
          WHEN $2::text = 'ytd'
            THEN date_trunc('year', now() AT TIME ZONE 'Asia/Manila')::date
          WHEN $2::text IN ('6m','12m')
            THEN (date_trunc('month', now() AT TIME ZONE 'Asia/Manila') - 
                  (CASE WHEN $2='6m' THEN interval '6 months' ELSE interval '12 months' END))::date
          ELSE /* default 3m */
            (date_trunc('month', now() AT TIME ZONE 'Asia/Manila') - interval '3 months')::date
        END AS from_date,
        /* to_date (end-exclusive, first day of current month unless explicit) */
        CASE
          WHEN $3::date IS NOT NULL AND $4::date IS NOT NULL
            THEN date_trunc('month', $4::date)
          ELSE date_trunc('month', now() AT TIME ZONE 'Asia/Manila')::date
        END AS to_date,
        $1::uuid AS only_program
    ),

    scope_se AS (
      SELECT s.se_id, TRIM(s.team_name) AS team_name, TRIM(COALESCE(s.abbr, s.team_name)) AS abbr
      FROM socialenterprises s, params p
      WHERE (p.only_program IS NULL OR s.program_id = p.only_program)
    ),

    /* windowed reporting (for completeness checks) */
    mrg AS (
      SELECT g.se_id, g."month", COUNT(DISTINCT g.report_type) AS rep_count
      FROM monthly_report_guard g
      JOIN scope_se s ON s.se_id = g.se_id
      JOIN params p ON TRUE
      WHERE g."month" >= p.from_date AND g."month" < p.to_date
      GROUP BY g.se_id, g."month"
    ),

    /* per-SE month coverage inside window */
    rep_per_se AS (
      SELECT
        se_id,
        COUNT(*)                              AS months_any,
        COUNT(*) FILTER (WHERE rep_count=3)   AS months_complete
      FROM mrg
      GROUP BY se_id
    ),

    /* monthly plumbing limited to scope_se (not eligibility), we’ll gate later */
    cin AS (
      SELECT r.se_id, date_trunc('month', r.report_month)::date AS m,
             SUM(t.sales_amount + t.other_revenue_amount)::numeric AS inflow_sales
      FROM cash_in_report r
      JOIN cash_in_transaction t USING (cash_in_report_id)
      JOIN scope_se s ON s.se_id = r.se_id
      JOIN params p ON TRUE
      WHERE r.report_month >= p.from_date AND r.report_month < p.to_date
      GROUP BY 1,2
    ),
    cout AS (
      SELECT r.se_id, date_trunc('month', r.report_month)::date AS m,
             SUM(t.cash_amount + t.inventory_amount + t.liability_amount + t.owners_withdrawal_amount)::numeric AS outflow_ops,
             SUM(t.inventory_amount)::numeric AS purchases
      FROM cash_out_report r
      JOIN cash_out_transaction t USING (cash_out_report_id)
      JOIN scope_se s ON s.se_id = r.se_id
      JOIN params p ON TRUE
      WHERE r.report_month >= p.from_date AND r.report_month < p.to_date
      GROUP BY 1,2
    ),
    inv AS (
      SELECT i.se_id, i."month"::date AS m,
             SUM(COALESCE(begin_qty,0)*COALESCE(begin_unit_price,0))::numeric AS begin_val,
             SUM(COALESCE(final_qty,0)*COALESCE(final_unit_price,0))::numeric AS end_val
      FROM inventory_report i
      JOIN scope_se s ON s.se_id = i.se_id
      JOIN params p ON TRUE
      WHERE i."month" >= p.from_date AND i."month" < p.to_date
      GROUP BY 1,2
    ),
    turn_raw AS (
      SELECT COALESCE(inv.se_id, cout.se_id, cin.se_id) AS se_id,
             COALESCE(inv.m,     cout.m,     cin.m)     AS m,
             GREATEST(COALESCE(inv.begin_val,0) + COALESCE(cout.purchases,0) - COALESCE(inv.end_val,0), 0)::numeric AS cogs,
             ((COALESCE(inv.begin_val,0) + COALESCE(inv.end_val,0))/2.0)::numeric AS avg_inventory
      FROM inv
      FULL JOIN cout ON cout.se_id = inv.se_id AND cout.m = inv.m
      FULL JOIN cin  ON cin.se_id  = COALESCE(inv.se_id, cout.se_id) AND cin.m = COALESCE(inv.m, cout.m)
    ),
    turn AS (
      SELECT se_id, m,
             CASE WHEN avg_inventory > 0 THEN (cogs/avg_inventory)::numeric ELSE NULL END AS turnover
      FROM turn_raw
    ),
    rep AS (
      SELECT g.se_id, g."month"::date AS m, COUNT(DISTINCT report_type) AS rep_count
      FROM monthly_report_guard g
      JOIN scope_se s ON s.se_id = g.se_id
      JOIN params p ON TRUE
      WHERE g."month" >= p.from_date AND g."month" < p.to_date
      GROUP BY 1,2
    ),
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
        COALESCE(rp.rep_count, 0)::int AS rep_count
      FROM months mo
      LEFT JOIN cin  ci ON ci.se_id = mo.se_id AND ci.m = mo.m
      LEFT JOIN cout co ON co.se_id = mo.se_id AND co.m = mo.m
      LEFT JOIN turn tu ON tu.se_id = mo.se_id AND tu.m = mo.m
      LEFT JOIN rep  rp ON rp.se_id = mo.se_id AND rp.m = mo.m
    ),
    per_se AS (
      SELECT s.se_id,
             SUM(mo.inflow_sales)  AS inflow_total,
             SUM(mo.outflow_ops)   AS outflow_total,
             AVG(mo.turnover)      AS avg_turnover,
             AVG(mo.rep_count/3.0) AS reporting_rate
      FROM scope_se s
      LEFT JOIN monthly mo ON mo.se_id = s.se_id
      GROUP BY s.se_id
    ),
    scored AS (
      SELECT *,
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
        ROUND(1 + LEAST(GREATEST(reporting_rate,0),1)*4, 2) AS reporting_score
      FROM per_se
    ),
    classified AS (
      SELECT sc.*,
             COALESCE(rps.months_any, 0)      AS months_any,
             COALESCE(rps.months_complete, 0) AS months_complete,
             ((CASE WHEN sc.cash_margin_score   <= 1.5 THEN 1 ELSE 0 END) +
              (CASE WHEN sc.inout_ratio_score  <= 1.5 THEN 1 ELSE 0 END) +
              (CASE WHEN sc.turnover_score     <= 1.5 THEN 1 ELSE 0 END) +
              (CASE WHEN sc.reporting_score    <= 1.5 THEN 1 ELSE 0 END)) AS red_count
      FROM scored sc
      LEFT JOIN rep_per_se rps USING (se_id)
    ),
    agg AS (
      SELECT
        COUNT(*) AS se_count,
        /* gate by months_complete >= 1 to avoid flagging no-data SEs */
        SUM(CASE WHEN months_complete >= 1 AND red_count > 2 THEN 1 ELSE 0 END) AS flagged_count,
        SUM(CASE WHEN months_complete >= 1
                  AND cash_margin_score>3 AND inout_ratio_score>3
                  AND turnover_score>3   AND reporting_score>3
                 THEN 1 ELSE 0 END) AS healthy_se_count,

        /* coverage counts */
        SUM(CASE WHEN months_complete >= 1 THEN 1 ELSE 0 END) AS se_with_financials,

        /* category totals (only count SEs with data) */
        SUM(CASE WHEN months_complete >= 1 AND cash_margin_score<=1.5 THEN 1 ELSE 0 END)                      AS cash_red,
        SUM(CASE WHEN months_complete >= 1 AND cash_margin_score>1.5 AND cash_margin_score<=3.0 THEN 1 ELSE 0 END) AS cash_moderate,
        SUM(CASE WHEN months_complete >= 1 AND cash_margin_score>3.0 THEN 1 ELSE 0 END)                        AS cash_healthy,

        SUM(CASE WHEN months_complete >= 1 AND inout_ratio_score<=1.5 THEN 1 ELSE 0 END)                       AS inout_red,
        SUM(CASE WHEN months_complete >= 1 AND inout_ratio_score>1.5 AND inout_ratio_score<=3.0 THEN 1 ELSE 0 END) AS inout_moderate,
        SUM(CASE WHEN months_complete >= 1 AND inout_ratio_score>3.0 THEN 1 ELSE 0 END)                        AS inout_healthy,

        SUM(CASE WHEN months_complete >= 1 AND turnover_score<=1.5 THEN 1 ELSE 0 END)                          AS turn_red,
        SUM(CASE WHEN months_complete >= 1 AND turnover_score>1.5 AND turnover_score<=3.0 THEN 1 ELSE 0 END)   AS turn_moderate,
        SUM(CASE WHEN months_complete >= 1 AND turnover_score>3.0 THEN 1 ELSE 0 END)                           AS turn_healthy,

        SUM(CASE WHEN months_complete >= 1 AND reporting_score<=1.5 THEN 1 ELSE 0 END)                         AS report_red,
        SUM(CASE WHEN months_complete >= 1 AND reporting_score>1.5 AND reporting_score<=3.0 THEN 1 ELSE 0 END) AS report_moderate,
        SUM(CASE WHEN months_complete >= 1 AND reporting_score>3.0 THEN 1 ELSE 0 END)                          AS report_healthy
      FROM classified
    )
    SELECT *,
           (se_count - se_with_financials) AS no_data_se_count
    FROM agg;
  `;

  const params = [programId, period, start, end];
  const { rows } = await pgDatabase.query(sql, params);
  const r = rows?.[0] ?? {};

  const seCount = Number(r.se_count || 0);
  const eligible = Number(r.se_with_financials || 0);

  const mk = (name, red, mod, healthy) => ({
    category: name,
    red: Number(red || 0),
    moderate: Number(mod || 0),
    healthy: Number(healthy || 0),
    // pct base = eligible SEs only
    redPct:      eligible ? +((red     || 0) * 100 / eligible).toFixed(1) : 0,
    moderatePct: eligible ? +((mod     || 0) * 100 / eligible).toFixed(1) : 0,
    healthyPct:  eligible ? +((healthy || 0) * 100 / eligible).toFixed(1) : 0,
  });

  return {
    seCount,
    seWithFinancials: eligible,
    noDataSeCount: Number(r.no_data_se_count || 0),

    flaggedCount: Number(r.flagged_count || 0),
    healthySeCount: Number(r.healthy_se_count || 0),
    moderateSeCount: Math.max(eligible - Number(r.flagged_count || 0) - Number(r.healthy_se_count || 0), 0),

    categories: [
      mk("Cash Margin",        r.cash_red,   r.cash_moderate,   r.cash_healthy),
      mk("In/Out Ratio",       r.inout_red,  r.inout_moderate,  r.inout_healthy),
      mk("Inventory Turnover", r.turn_red,   r.turn_moderate,   r.turn_healthy),
      mk("Reporting",          r.report_red, r.report_moderate, r.report_healthy),
    ],
  };
};

exports.getImprovementScorePerMonthAnnually= async (program = null) => {
    try {
        let programFilter = program ? `AND p.name = '${program}'` : '';

        const query = `
            WITH DateRange AS ( -- Dynamically finds the range of evaluations
                SELECT 
                    DATE_TRUNC('month', MIN(created_at))::DATE AS start_date,
                    DATE_TRUNC('month', MAX(created_at))::DATE AS end_date
                FROM evaluations
                WHERE evaluation_type = 'Social Enterprise'
            ),
            Months AS ( -- Generates months dynamically based on the min/max date
                SELECT generate_series(
                    (SELECT start_date FROM DateRange), 
                    (SELECT end_date FROM DateRange), 
                    INTERVAL '3 months'
                )::DATE AS month
            ),
            MonthlyRatings AS ( -- Calculates the average rating per month
                SELECT 
                    e.se_id,
                    s.abbr AS social_enterprise, 
                    DATE_TRUNC('month', e.created_at)::DATE AS month, 
                    ROUND(AVG(ec.rating), 3) AS avg_rating
                FROM evaluations e
                JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                JOIN socialenterprises s ON e.se_id = s.se_id
                JOIN programs p ON p.program_id = s.program_id
                WHERE e.evaluation_type = 'Social Enterprise'
                ${programFilter}
                GROUP BY e.se_id, s.abbr, month
            ),
            FilledMonths AS ( -- Ensures all months exist, even if no evaluations happened
                SELECT 
                    m.month, 
                    mr.se_id, 
                    mr.social_enterprise, 
                    COALESCE(mr.avg_rating, 0) AS avg_rating
                FROM Months m
                LEFT JOIN MonthlyRatings mr ON m.month = mr.month
            ),
            RankedRatings AS ( -- Calculates improvement compared to previous months
                SELECT 
                    se_id, 
                    social_enterprise, 
                    month, 
                    avg_rating,
                    LAG(avg_rating) OVER (PARTITION BY se_id ORDER BY month) AS prev_avg_rating
                FROM FilledMonths
            )
            SELECT 
                month,
                ROUND(AVG(avg_rating - COALESCE(prev_avg_rating, avg_rating)), 3) AS overall_avg_improvement,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_rating - COALESCE(prev_avg_rating, avg_rating)) AS median_improvement
            FROM RankedRatings
            GROUP BY month
            ORDER BY month;
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getAllEvaluationStats= async (program = null) => {
    try {
        let programFilter = program ? `WHERE p.name = '${program}'` : '';

        const query = `
            SELECT 
                COUNT(*) AS totalEvaluations,
                COUNT(CASE WHEN "isAcknowledge" = true THEN 1 END) AS acknowledgedEvaluations
            FROM 
                evaluations AS e
            JOIN 
                socialenterprises AS s ON e.se_id = s.se_id
            JOIN 
                programs AS p ON p.program_id = s.program_id
            ${programFilter}
            ;
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getTotalEvaluationCount= async (se_id) => {
    try {
        const query = `
            SELECT COUNT(*) AS total_evaluations
            FROM evaluations
            WHERE se_id = $1;
        `;

        const values = [se_id];

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getPendingEvaluationCount= async (se_id) => {
    try {
        const query = `
            SELECT COUNT(*) AS pending_evaluations
            FROM evaluations AS e
            WHERE e.se_id = $1
            AND e."isAcknowledge" = false;
        `;

        const values = [se_id];

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getAcknowledgedEvaluationCount= async (se_id) => {
    try {
        const query = `
            SELECT COUNT(*) AS acknowledged_evaluations
            FROM evaluations AS e
            WHERE e.se_id = $1
            AND e."isAcknowledge" = true;
        `;

        const values = [se_id];

        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getGrowthScoreOverallAnually= async (program = null) => {
    try {
        let programFilter = program ? `AND p.name = '${program}'` : '';

        const query = `
            WITH MonthlyRatings AS (
                SELECT 
                    e.se_id,
                    DATE_TRUNC('month', e.created_at) AS month,
                    ROUND(AVG(ec.rating), 2) AS avg_rating
                FROM evaluations e
                JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                WHERE e.evaluation_type = 'Social Enterprise'
                GROUP BY e.se_id, month
            ),
            RankedRatings AS (
                SELECT 
                    se_id, 
                    month, 
                    avg_rating,
                    LAG(avg_rating) OVER (PARTITION BY se_id ORDER BY month) AS prev_avg_rating,
                    FIRST_VALUE(avg_rating) OVER (PARTITION BY se_id ORDER BY month) AS first_recorded_rating
                FROM MonthlyRatings
            ),
            Growth AS (
                SELECT 
                    se_id,
                    month,
                    avg_rating,
                    prev_avg_rating,
                    first_recorded_rating,
                    (avg_rating - prev_avg_rating) AS monthly_growth,
                    ((avg_rating - prev_avg_rating) / NULLIF(prev_avg_rating, 0)) * 100 AS monthly_growth_rate,
                    ((avg_rating / NULLIF(first_recorded_rating, 0)) - 1) * 100 AS cumulative_growth_percentage  -- ✅ Keep original name
                FROM RankedRatings
            )
            SELECT 
                g.se_id, 
                g.month, 
                s.abbr,  -- ✅ Fetch the abbreviation from socialenterprises
                ROUND(g.avg_rating, 2) AS current_avg_rating, 
                ROUND(COALESCE(g.prev_avg_rating, g.avg_rating), 2) AS previous_avg_rating, 
                ROUND(g.monthly_growth, 2) AS growth,
                ROUND(g.monthly_growth_rate, 2) AS growth_change_rate,
                ROUND(g.cumulative_growth_percentage, 2) AS cumulative_growth  -- ✅ Correct column name
            FROM Growth g
            JOIN socialenterprises s ON g.se_id = s.se_id 
            JOIN programs AS p ON p.program_id = s.program_id
            WHERE g.cumulative_growth_percentage IS NOT NULL
            ${programFilter}
            ORDER BY g.cumulative_growth_percentage DESC  -- ✅ Correct column name
            LIMIT 1;  -- ✅ Return only 1 record
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getMonthlyGrowthDetails= async () => {
    try {
        const query = `
            WITH MonthlyRatings AS (
          SELECT 
              e.se_id,
              DATE_TRUNC('month', e.created_at) AS month,
              ROUND(AVG(ec.rating), 2) AS avg_rating
          FROM evaluations e
          JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
          WHERE e.evaluation_type = 'Social Enterprise'
          GROUP BY e.se_id, month
      ),
      RankedRatings AS (
          SELECT 
              se_id, 
              month, 
              avg_rating,
              LAG(avg_rating) OVER (PARTITION BY se_id ORDER BY month) AS prev_avg_rating
          FROM MonthlyRatings
      ),
      Growth AS (
          SELECT 
              se_id,
              month,
              avg_rating,
              prev_avg_rating,
              (avg_rating - prev_avg_rating) AS monthly_growth
          FROM RankedRatings
          WHERE prev_avg_rating IS NOT NULL
      ),
      FinalGrowth AS (
          SELECT 
              se_id, 
              month, 
              avg_rating, 
              prev_avg_rating, 
              monthly_growth,
              LAG(monthly_growth) OVER (PARTITION BY se_id ORDER BY month) AS prev_monthly_growth
          FROM Growth
      )
      SELECT 
          month, 
          ROUND(avg_rating, 2) AS current_avg_rating, 
          ROUND(prev_avg_rating, 2) AS previous_avg_rating, 
          ROUND(monthly_growth, 2) AS growth,
          ROUND(
              CASE 
                  WHEN prev_monthly_growth = 0 OR prev_monthly_growth IS NULL THEN 0 
                  ELSE ((monthly_growth - prev_monthly_growth) / prev_monthly_growth) * 100 
              END, 2
          ) AS growth_change_rate
      FROM FinalGrowth
      ORDER BY month;
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getSELeaderboards= async (program = null) => {
    try {
        let programFilter = program ? `AND p.name = '${program}'` : '';

        const query = `
            WITH MonthlyRatings AS (
                SELECT 
                    e.se_id,
                    s.abbr AS social_enterprise, 
                    s.team_name AS full_name,  -- Fetch full name for tooltip
                    DATE_TRUNC('month', e.created_at) AS month,
                    ROUND(AVG(ec.rating), 2) AS avg_rating,
                    COUNT(*) AS eval_count -- Count number of evaluations per SE per month
                FROM evaluations e
                JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
                JOIN socialenterprises s ON e.se_id = s.se_id
                JOIN programs AS p ON p.program_id = s.program_id
                WHERE e.created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months') -- Allow flexibility for time frame
                    AND e.evaluation_type = 'Social Enterprise'
                    ${programFilter}
                GROUP BY e.se_id, s.abbr, s.team_name, month
            ),
            WeightedRatings AS (
                SELECT 
                    mr.se_id, 
                    mr.social_enterprise,
                    mr.full_name, -- Include full name
                    mr.month,
                    mr.avg_rating,
                    mr.eval_count,
                    CASE
                        WHEN mr.month = DATE_TRUNC('month', CURRENT_DATE) THEN 1.0
                        WHEN mr.month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN 0.75
                        WHEN mr.month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months') THEN 0.5
                        ELSE 0.25 -- Past months get decreasing weights
                    END AS weight
                FROM MonthlyRatings mr
            ),
            TopSEs AS (
                SELECT 
                    wr.se_id,
                    wr.social_enterprise,
                    wr.full_name,
                    SUM(wr.avg_rating * wr.eval_count * wr.weight) / SUM(wr.eval_count * wr.weight) AS weighted_avg_rating,
                    AVG(wr.avg_rating) AS simple_avg_rating
                FROM WeightedRatings wr
                GROUP BY wr.se_id, wr.social_enterprise, wr.full_name
                HAVING COUNT(wr.se_id) >= 3  -- Ensure sufficient evaluations per SE
                ORDER BY weighted_avg_rating DESC, simple_avg_rating DESC
            )
            SELECT 
                t.se_id, 
                t.social_enterprise,  -- Abbreviated name for axis
                t.full_name,          -- Full name for tooltip
                ROUND(t.simple_avg_rating, 2) AS most_recent_avg_rating,
                ROUND(t.weighted_avg_rating, 2) AS overall_weighted_avg_rating,
                ROUND(t.simple_avg_rating - t.weighted_avg_rating, 2) AS performance_change -- Ensure 2 decimal places
            FROM TopSEs t
            ORDER BY t.weighted_avg_rating DESC;
        `;
        const result = await pgDatabase.query(query);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching leaderboards:", error);
        return [];
    }
};

exports.updateAcknowledgeEvaluation= async (evaluationId) => {
    try {
        const query = `
            UPDATE evaluations SET "isAcknowledge" = true WHERE evaluation_id = $1 RETURNING *
        `;
        const values = [evaluationId];
        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.avgRatingPerSE= async (se_id) => {
    try {
        const query = `
            SELECT 
                e.se_id,
                s.abbr AS social_enterprise, 
                s.team_name AS full_name,
                ROUND(AVG(ec.rating), 2) AS avg_rating
            FROM evaluations e
            JOIN evaluation_categories ec ON e.evaluation_id = ec.evaluation_id
            JOIN socialenterprises s ON e.se_id = s.se_id
            WHERE e.evaluation_type = 'Social Enterprise'
            AND e.se_id = $1
            GROUP BY e.se_id, s.abbr, s.team_name
            ORDER BY avg_rating DESC;
        `;
        const values = [se_id];
        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching top SE performance:", error);
        return [];
    }
};

exports.getEvaluationSubmittedCount= async (mentor_id) => {
    try {
        const query = `
            SELECT COUNT(*) AS evaluation_count
            FROM evaluations
            WHERE mentor_id = $1;
        `;
        const values = [mentor_id];
        const result = await pgDatabase.query(query, values);
        return result.rows;
    } catch (error) {
        console.error("❌ Error fetching submitted evaluations count:", error);
        return [];
    }
};