const pgDatabase = require('../database.js'); // Import PostgreSQL client

exports.getSocialEnterprisesByProgram = async (programId) => {
  try {
    // Query to get social enterprises by program_id
    const query = ` SELECT DISTINCT ON (se.se_id) 
                        se.se_id, 
                        se.team_name, 
                        se.abbr
                    FROM SocialEnterprises se
                    INNER JOIN Mentorships m ON se.se_id = m.se_id
                    WHERE se.program_id = $1`;
    const values = [programId];

    const result = await pgDatabase.query(query, values);

    // If no social enterprises are found, return an empty array
    if (!result.rows.length) {
      return [];
    }

    // Map the results to the desired format
    return result.rows.map(se => ({
      text: se.team_name, 
      abbr: se.abbr,
      callback_data: `enterprise_${se.se_id}`
    }));
  } catch (error) {
    console.error("Error fetching Social Enterprises:", error);
    return [];
  }
};

exports.getSocialEnterpriseByID = async (se_id) => {
  try {
    // Query to get a social enterprise by se_id
    const query = 'SELECT * FROM socialenterprises WHERE se_id = $1';
    const values = [se_id];

    const result = await pgDatabase.query(query, values);

    // If no matching social enterprise is found, return null
    if (!result.rows.length) {
      console.log(`⚠️ No social enterprise found for ID: ${se_id}`);
      return null;
    }

    return result.rows[0]; // Return the first (and only) matching row
  } catch (error) {
    console.error("❌ Error fetching social enterprise:", error);
    return null;
  }
};

exports.getAllSocialEnterprises = async () => {
  try {
    const res = await pgDatabase.query(`
      SELECT 
        se.se_id,
        se.team_name,
        se.abbr,
        se.description,
        se.accepted_application_id,
        array_agg(CONCAT('SDG ', sdg.sdg_number, ': ', sdg.name)) AS sdgs
      FROM socialenterprises se
      JOIN sdg ON sdg.sdg_id = ANY(se.sdg_id)
      GROUP BY se.se_id, se.team_name, se.abbr, se.description;
    `);

    if (!res.rows || res.rows.length === 0) {
      return null; // or return []
    }

    return res.rows;
  } catch (error) {
    console.error("Error fetching social enterprises:", error);
    return null;
  }
};

exports.getAcceptedApplications = async (id) => {
  try {
    const res = await pgDatabase.query(`
      SELECT
        id AS application_id,
        team_name,
        se_abbreviation AS abbr,
        se_description AS description,
        "timestamp" AS submitted_at,
        enterprise_idea_start,
        social_problem,
        se_nature,
        team_characteristics,
        team_challenges,
        critical_areas,
        action_plans,
        pitch_deck_url,
        focal_email,
        focal_phone,
        focal_person_contact,
        social_media_link,
        mentoring_team_members,
        preferred_mentoring_time,
        mentoring_time_note,
        meeting_frequency,
        communication_modes
      FROM mentees_form_submissions
      WHERE id = $1 AND status = 'Accepted'
    `, [id]);

    if (!res.rows || res.rows.length === 0) {
      return null;
    }

    return res.rows[0];
  } catch (error) {
    console.error("Error fetching application by ID:", error);
    throw error;
  }
};


exports.getAllSocialEnterprisesForComparison = async (program = null) => {
  try {
    let programFilter = program ? `WHERE p.name = '${program}'` : '';
    
    // Query to get a social enterprise by se_id
    const query = `
        SELECT DISTINCT se.*
        FROM socialenterprises se
        JOIN evaluations e ON se.se_id = e.se_id
        JOIN programs p ON p.program_id = se.program_id
        ${programFilter};
        `;
    const res = await pgDatabase.query(query);
    
    if (!res.rows || res.rows.length === 0) {
      return null; // or return an empty array []
    }

    return res.rows; // return the list of users
  } catch (error) {
    console.error("Error fetching user:", error);
    return null; // or handle error more gracefully
  }
};

exports.getFlaggedSEs = async (arg = null) => {
  // Back-compat: plain string => program_name
  const opts = (typeof arg === "string" || arg == null)
    ? { program_id: null, program_name: arg ?? null, se_id: null }
    : arg;

  const {
    program_id   = null, // UUID or null
    program_name = null, // exact name or null
    se_id        = null, // UUID or null
  } = opts ?? {};

  const isUuid = v =>
    !v || /^[0-9a-fA-F-]{8}-[0-9a-fA-F-]{4}-[1-5][0-9a-fA-F-]{3}-[89abAB][0-9a-fA-F-]{3}-[1-9a-fA-F][0-9a-fA-F]{2}$/.test(v);

  if (!isUuid(program_id)) throw new Error("INVALID_PROGRAM_ID");
  if (!isUuid(se_id))      throw new Error("INVALID_SE_ID");

  const sql = `
    WITH params AS (
      SELECT
        -- last 3 *complete* months in Asia/Manila
        (date_trunc('month', (now() AT TIME ZONE 'Asia/Manila')) - interval '3 months')::date AS from_date,
        (date_trunc('month', (now() AT TIME ZONE 'Asia/Manila')))::date                     AS to_date,
        $1::uuid AS only_program,
        $2::uuid AS only_se,
        $3::text AS only_program_name
    ),
    -- Program/SE filter baseline (ALL SEs in scope regardless of data)
    scope_all AS (
      SELECT s.se_id, TRIM(s.team_name) AS team_name, TRIM(COALESCE(s.abbr, s.team_name)) AS abbr
      FROM socialenterprises s
      JOIN programs pr ON pr.program_id = s.program_id
      JOIN params p ON TRUE
      WHERE (p.only_program      IS NULL OR s.program_id = p.only_program)
        AND (p.only_se           IS NULL OR s.se_id      = p.only_se)
        AND (p.only_program_name IS NULL OR pr.name      = p.only_program_name)
    ),
    -- Monthly-report completeness within the 3-month window
    mrg AS (
      SELECT g.se_id, g."month", COUNT(DISTINCT g.report_type) AS rep_count
      FROM monthly_report_guard g
      JOIN scope_all s ON s.se_id = g.se_id
      JOIN params p ON TRUE
      WHERE g."month" >= p.from_date AND g."month" < p.to_date
      GROUP BY g.se_id, g."month"
    ),
    eligible AS (
      SELECT se_id, SUM((rep_count = 3)::int) AS months_complete
      FROM mrg
      GROUP BY se_id
      HAVING SUM((rep_count = 3)::int) >= 1
    ),

    /* ===== calculations ONLY for eligible SEs ===== */
    cin AS (
      SELECT r.se_id, date_trunc('month', r.report_month)::date AS m,
             SUM(t.sales_amount + t.other_revenue_amount)::numeric AS inflow_sales
      FROM cash_in_report r
      JOIN cash_in_transaction t USING (cash_in_report_id)
      JOIN eligible e ON e.se_id = r.se_id
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
      JOIN eligible e ON e.se_id = r.se_id
      JOIN params p ON TRUE
      WHERE r.report_month >= p.from_date AND r.report_month < p.to_date
      GROUP BY 1,2
    ),
    inv AS (
      SELECT i.se_id, i."month"::date AS m,
             SUM(COALESCE(begin_qty,0)*COALESCE(begin_unit_price,0))::numeric AS begin_val,
             SUM(COALESCE(final_qty,0)*COALESCE(final_unit_price,0))::numeric AS end_val
      FROM inventory_report i
      JOIN eligible e ON e.se_id = i.se_id
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
      JOIN eligible e ON e.se_id = g.se_id
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
        COALESCE(rp.rep_count, 0)::int        AS rep_count
      FROM months mo
      LEFT JOIN cin  ci ON ci.se_id = mo.se_id AND ci.m = mo.m
      LEFT JOIN cout co ON co.se_id = mo.se_id AND co.m = mo.m
      LEFT JOIN turn tu ON tu.se_id = mo.se_id AND tu.m = mo.m
      LEFT JOIN rep  rp ON rp.se_id = mo.se_id AND rp.m = mo.m
    ),
    per_se AS (
      SELECT e.se_id,
             SUM(mo.inflow_sales)  AS inflow_total,
             SUM(mo.outflow_ops)   AS outflow_total,
             AVG(mo.turnover)      AS avg_turnover,
             AVG(mo.rep_count/3.0) AS reporting_rate
      FROM eligible e
      LEFT JOIN monthly mo ON mo.se_id = e.se_id
      GROUP BY e.se_id
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
    flagged AS (
      SELECT
        s.se_id,
        sa.team_name,
        sa.abbr,
        s.cash_margin_score AS "Cash Margin",
        s.inout_ratio_score AS "In/Out Ratio",
        s.turnover_score    AS "Inventory Turnover",
        s.reporting_score   AS "Reporting",
        (s.cash_margin_score + s.inout_ratio_score + s.turnover_score + s.reporting_score) AS risk_total,
        (
          (CASE WHEN s.cash_margin_score   <= 1.5 THEN 1 ELSE 0 END) +
          (CASE WHEN s.inout_ratio_score  <= 1.5 THEN 1 ELSE 0 END) +
          (CASE WHEN s.turnover_score     <= 1.5 THEN 1 ELSE 0 END) +
          (CASE WHEN s.reporting_score    <= 1.5 THEN 1 ELSE 0 END)
        ) AS red_count
      FROM scored s
      JOIN scope_all sa ON sa.se_id = s.se_id
      WHERE
        ((CASE WHEN s.cash_margin_score   <= 1.5 THEN 1 ELSE 0 END) +
         (CASE WHEN s.inout_ratio_score  <= 1.5 THEN 1 ELSE 0 END) +
         (CASE WHEN s.turnover_score     <= 1.5 THEN 1 ELSE 0 END) +
         (CASE WHEN s.reporting_score    <= 1.5 THEN 1 ELSE 0 END)) > 2
    )
    SELECT
      (SELECT COUNT(*) FROM scope_all)                                                   AS total_se_count,
      (SELECT COUNT(*) FROM eligible)                                                    AS eligible_se_count,
      (SELECT COUNT(*) FROM scope_all) - (SELECT COUNT(*) FROM eligible)                 AS no_data_se_count,
      COALESCE(
        JSON_AGG(
          flagged
          ORDER BY flagged.red_count DESC, flagged.risk_total ASC, flagged.team_name ASC
        ),
        '[]'::json
      ) AS flagged
    FROM flagged;
  `;

  const params = [program_id, se_id, program_name];

  try {
    const { rows } = await pgDatabase.query(sql, params);
    const row = rows?.[0] || {};
    return {
      totalSeCount: Number(row.total_se_count || 0),
      eligibleSeCount: Number(row.eligible_se_count || 0),
      noDataSeCount: Number(row.no_data_se_count || 0),
      flagged: Array.isArray(row.flagged) ? row.flagged : [],
    };
  } catch (err) {
    console.error("getFlaggedSEs error:", err);
    return {
      totalSeCount: 0,
      eligibleSeCount: 0,
      noDataSeCount: 0,
      flagged: [],
      error: "QUERY_FAILED",
    };
  }
};

exports.getAllSocialEnterpriseswithMentorID = async (mentor_id) => {
  try {
      const query = `
        SELECT 
            se.se_id, 
            se.team_name, 
            p.name AS program_name, 
            se.contactnum,
            COALESCE(
                JSON_AGG(
                    CASE 
                        WHEN m.mentor_id IS NOT NULL 
                        THEN JSON_BUILD_OBJECT(
                            'mentor_id', m.mentor_id,
                            'mentor_name', CONCAT(m.mentor_firstname, ' ', m.mentor_lastname)
                        ) 
                        ELSE NULL 
                    END
                ) FILTER (WHERE m.mentor_id IS NOT NULL), 
                '[]'
            ) AS mentors
        FROM socialenterprises AS se
        LEFT JOIN mentorships AS ms ON se.se_id = ms.se_id
        LEFT JOIN mentors AS m ON ms.mentor_id = m.mentor_id
        LEFT JOIN programs AS p ON se.program_id = p.program_id -- ✅ Join with programs table
        WHERE m.mentor_id = $1 -- 🔍 Filter by a specific mentor
        GROUP BY se.se_id, se.team_name, p.name;
      `;
      const values = [mentor_id];

      const result = await pgDatabase.query(query, values);
      return result.rows.length ? result.rows : [];
  } catch (error) {
      console.error("❌ Error fetching social enterprises with mentorship info:", error);
      return [];
  }
};

exports.getAllSocialEnterprisesWithMentorship = async (program = null) => {
  try {
      let programFilter = program ? `WHERE p.name = '${program}'` : '';

      const query = `
      SELECT 
          se.se_id, 
          se.team_name, 
          p.name AS program_name,
          se.contactnum,
          COALESCE(
              JSON_AGG(
                  CASE 
                      WHEN m.mentor_id IS NOT NULL 
                      THEN JSON_BUILD_OBJECT(
                          'mentor_id', m.mentor_id,
                          'mentor_name', CONCAT(m.mentor_firstname, ' ', m.mentor_lastname)
                      ) 
                      ELSE NULL 
                  END
              ) FILTER (WHERE m.mentor_id IS NOT NULL), 
              '[]'
          ) AS mentors
      FROM socialenterprises AS se
      LEFT JOIN mentorships AS ms ON se.se_id = ms.se_id
      LEFT JOIN mentors AS m ON ms.mentor_id = m.mentor_id
      LEFT JOIN programs AS p ON se.program_id = p.program_id -- ✅ Join with programs table
      ${programFilter}
      GROUP BY se.se_id, se.team_name, p.name
      `;
      const result = await pgDatabase.query(query);
      return result.rows.length ? result.rows : [];
  } catch (error) {
      console.error("❌ Error fetching social enterprises with mentorship info:", error);
      return [];
  }
};

exports.getSocialEnterpriseByMentorID = async (mentor_id) => {
  const query = `
    SELECT se.se_id, se.team_name
    FROM socialenterprises se
    JOIN mentorships m ON se.se_id = m.se_id
    WHERE m.mentor_id = $1
  `;
  const result = await pgDatabase.query(query, [mentor_id]);
  return result.rows;
};

exports.getSocialEnterprisesWithoutMentor = async () => {
  try {
    const query = `
      SELECT 
          se.se_id, 
          se.team_name
      FROM socialenterprises AS se
      LEFT JOIN mentorships AS ms ON se.se_id = ms.se_id
      WHERE ms.se_id IS NULL
    `;
    const result = await pgDatabase.query(query);
    return result.rows.length ? result.rows : [];
  } catch (error) {
    console.error("❌ Error fetching social enterprises without mentors:", error);
    return [];
  }
};

exports.updateSERowUpdate = async (se_id, updatedData) => {
  try {
    const { name, program_name, mentorshipStatus, mentors } = updatedData;
    console.log("[ctrl] updatingSE: ", se_id, "\n Data: ", updatedData);

    // Update the social enterprise name and program
    const updateSEQuery = `
      UPDATE socialenterprises
      SET team_name = $1
      WHERE se_id = $2
      RETURNING *;
    `;
    await pgDatabase.query(updateSEQuery, [name, se_id]);

    console.log("updatedData.mentors: ", updatedData.mentors);

    // Proceed only if there are mentors
    if (updatedData.mentors.toLowerCase() !== 'no mentors') {
      // Remove existing mentorships for this SE
      const deleteMentorshipQuery = `DELETE FROM mentorships WHERE se_id = $1;`;
      await pgDatabase.query(deleteMentorshipQuery, [se_id]);

      const selectMentorQuery = `
          SELECT mentor_id 
          FROM mentors 
          WHERE CONCAT(mentor_firstname, ' ', mentor_lastname) = $1;
      `;
      const selectedMentors = await pgDatabase.query(selectMentorQuery, [updatedData.mentors]);

      // console.log("updatedData.mentors: ", updatedData.mentors); // Debugging purposes only
      // console.log("selectedMentors: ", selectedMentors.rows[0]?.mentor_id, "rowCount: ", selectedMentors.rowCount); // Debugging purposes only

      // // Add new mentorships if there are mentors
      const mentorNamesArray = Array.isArray(mentors) ? mentors : [mentors];
      if (selectedMentors.rowCount > 0) {
          const mentorInsertQuery = `
              INSERT INTO mentorships (se_id, mentor_id)
              SELECT $1, mentor_id FROM mentors WHERE CONCAT(mentor_firstname, ' ', mentor_lastname) = ANY($2::text[]);
          `;
          await pgDatabase.query(mentorInsertQuery, [se_id, mentorNamesArray]);
      }
      console.log("[updateSERowUpdate] You have updated the mentorships table.")
    }
    
    return { success: true, message: "Social Enterprise updated successfully" };
  } catch (error) {
    console.error("❌ Error updating Social Enterprise:", error);
    return { success: false, message: "Failed to update Social Enterprise" };
  }
};

exports.updateSocialEnterpriseStatus = async (se_id, isActive) => {
  try {
    const query = `
      UPDATE socialenterprises
      SET isactive = $1
      WHERE se_id = $2
      RETURNING *;
    `;
    const result = await pgDatabase.query(query, [isActive, se_id]);
    return result.rows[0];
  } catch (error) {
    console.error("❌ Error updating social enterprise status:", error);
    throw error;
  }
};

exports.getTotalSECount = async (program = null) => {
  try {
      let programFilter = program ? `WHERE p.name = '${program}'` : '';

      const query = `
        SELECT COUNT(*) FROM socialenterprises AS s
        JOIN programs AS p ON p.program_id = s.program_id
        ${programFilter}
      `;

      const result = await pgDatabase.query(query);
      return result.rows;
  } catch (error) {
      console.error("❌ Error fetching se count:", error);
      return [];
  }
};

exports.addSocialEnterprise = async (socialEnterpriseData) => {
  try {
    const {
      name,
      sdg_ids,
      contactnum,
      program_id,
      isactive,
      abbr = null,
      number_of_members = 0,
      criticalAreas = [],
      description,
      preferred_mentoring_time = [],
      mentoring_time_note,
      accepted_application_id,
    } = socialEnterpriseData;

    if (!sdg_ids || !Array.isArray(sdg_ids) || sdg_ids.length === 0) {
      throw new Error("At least one SDG ID is required.");
    }
    if (!program_id) {
      throw new Error("Program ID is required but missing.");
    }

    const formatted_sdg_ids = `{${sdg_ids.join(",")}}`;

    const query = `
      INSERT INTO socialenterprises (
        team_name,
        sdg_id,
        contactnum,
        program_id,
        isactive,
        abbr,
        numMember,
        critical_areas,
        description,
        preferred_mentoring_time,
        mentoring_time_note,
        accepted_application_id
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING se_id;
    `;

    const values = [
      name,
      formatted_sdg_ids,
      contactnum,
      program_id,
      isactive,
      abbr,
      number_of_members,
      criticalAreas,
      description,
      preferred_mentoring_time,
      mentoring_time_note,
      accepted_application_id
    ];

    const result = await pgDatabase.query(query, values);
    const se_id = result.rows[0].se_id;

    return { se_id };
  } catch (error) {
    console.error("Error adding social enterprise:", error);
    throw error;
  }
};

exports.getPreviousTotalSECount = async (program = null) => {
  try {
      let programFilter = program ? `AND p.name = '${program}'` : '';

      const query = `
        SELECT COUNT(*) AS count 
        FROM socialenterprises AS s
        JOIN programs AS p ON p.program_id = s.program_id
        WHERE s.created_at < NOW() - INTERVAL '1 month'
        ${programFilter};
      `;
      const result = await pgDatabase.query(query);
      return result.rows;
  } catch (error) {
      console.error("❌ Error fetching se count:", error);
      return [];
  }
};

exports.getSEWithOutMentors = async (program = null) => {
  try {
      let programFilter = program ? `AND p.name = '${program}'` : '';

      const query = `
          SELECT COUNT(*) AS total_se_without_mentors
          FROM socialenterprises AS s
          JOIN programs AS p ON p.program_id = s.program_id
              WHERE s.se_id NOT IN (
                  SELECT DISTINCT se_id FROM mentorships WHERE status = 'Active'
              )
          ${programFilter};
      `;

      const result = await pgDatabase.query(query);
      return result.rows;
  } catch (error) {
      console.error("❌ Error fetching mentorships", error);
      return []; // Return an empty array in case of an error
  }
};

exports.getAreasOfFocus = async (se_id) => {
  try {
    const query = `
      SELECT unnest(critical_areas) AS area
      FROM socialenterprises
      WHERE se_id = $1;
    `;
    const values = [se_id];

    const result = await pgDatabase.query(query, values);
    return result.rows.map(row => row.area);
  } catch (error) {
    console.error("Error fetching areas of focus:", error);
    return null;
  }
};

exports.getSuggestedMentors = async (se_id) => {
  try {
    const query = `
      WITH se_areas AS (
        SELECT critical_areas
        FROM socialenterprises
        WHERE se_id = $1
      ),
      mentor_matches AS (
        SELECT
          m.mentor_id,
          m.mentor_firstname,
          m.mentor_lastname,
          m.critical_areas,
          m.is_available_for_assignment,  -- Include toggle column here
          ARRAY(
            SELECT UNNEST(m.critical_areas)
            INTERSECT
            SELECT UNNEST(se_areas.critical_areas)
          ) AS matched_areas,
          COALESCE(cardinality(
            ARRAY(
              SELECT UNNEST(m.critical_areas)
              INTERSECT
              SELECT UNNEST(se_areas.critical_areas)
            )
          ), 0) AS match_count,
          cardinality(se_areas.critical_areas) AS total_se_areas
        FROM mentors m, se_areas
      ),
      ranked_mentors AS (
        SELECT *,
          CASE
            WHEN match_count = total_se_areas AND total_se_areas > 0 THEN 3  -- Top match
            WHEN match_count >= CEIL(total_se_areas * 0.5) THEN 2           -- Good match
            ELSE 1                                                          -- Weak match
          END AS match_rank
        FROM mentor_matches
      )
      SELECT jsonb_build_object(
        'suggested', jsonb_agg(to_jsonb(ranked_mentors) ORDER BY match_rank DESC, match_count DESC)
          FILTER (WHERE match_rank > 1),
        'others', jsonb_agg(to_jsonb(ranked_mentors))
          FILTER (WHERE match_rank = 1)
      ) AS result
      FROM ranked_mentors;
    `;

    const result = await pgDatabase.query(query, [se_id]);

    // ✅ Correctly extract the JSON object from the result
    return result.rows[0]?.result || { suggested: [], others: [] };
  } catch (error) {
    console.error("❌ Error fetching mentor suggestions:", error);
    return { suggested: [], others: [] };
  }
};