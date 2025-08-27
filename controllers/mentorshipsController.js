const pgDatabase = require('../database.js'); // Import PostgreSQL client

exports.getMentorshipsByMentorId = async (mentor_id) => {
  try {
    const query = `
            SELECT 
                ms.mentoring_session_id, -- ✅ Add to GROUP BY
                m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name, -- ✅ Mentor assigned
                se.team_name AS social_enterprise_name, -- ✅ Social Enterprise assigned
                se.se_id,
                m.mentor_id,
                p."name" AS program_name, -- ✅ Program the SE belongs to
                STRING_AGG(sdg."name", ', ') AS SDGs, -- ✅ Aggregate multiple SDGs
                TO_CHAR(ms.start_time, 'HH24:MI') AS start_time, -- ✅ Formatted start time
                TO_CHAR(ms.end_time, 'HH24:MI') AS end_time, -- ✅ Formatted end time
                TO_CHAR(ms.mentoring_session_date, 'FMMonth DD, YYYY') AS mentoring_session_date
            FROM mentoring_session AS ms
            JOIN mentorships AS mt ON ms.mentorship_id = mt.mentorship_id -- ✅ Get mentorship details
            JOIN mentors AS m ON mt.mentor_id = m.mentor_id -- ✅ Get mentor details from mentorships
            JOIN socialenterprises AS se ON mt.se_id = se.se_id -- ✅ Get SE details from mentorships
            JOIN programs AS p ON se.program_id = p.program_id -- ✅ Get the program the SE belongs to
            JOIN sdg AS sdg ON sdg.sdg_id = ANY(se.sdg_id) -- ✅ Get the SDGs for the SE
            WHERE ms.status = 'Accepted' AND mt.mentor_id = $1
            GROUP BY 
                ms.mentoring_session_id, -- ✅ Added to GROUP BY
                m.mentor_firstname, 
                m.mentor_lastname, 
                se.team_name, 
                se.se_id,
                m.mentor_id,
                p."name", 
                ms.start_time, 
                ms.end_time, 
                mentoring_session_date
            ORDER BY ms.start_time DESC; -- ✅ Show most recent sessions first
        `;

    const values = [mentor_id];
    const result = await pgDatabase.query(query, values);

    return result.rows.length ? result.rows : [];
  } catch (error) {
    console.error("❌ Error fetching mentorships by mentor_id:", error);
    return []; // Return an empty array in case of an error
  }
};

exports.getCollaborators = async (mentor_id) => {
  try {
    const query = `
      SELECT 
        mt.mentorship_id,
        m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name,
        se.team_name AS social_enterprise_name,
        se.se_id,
        m.mentor_id,
        p."name" AS program_name,
        STRING_AGG(sdg."name", ', ') AS SDGs
      FROM mentorships AS mt
      JOIN mentors AS m ON mt.mentor_id = m.mentor_id
      JOIN socialenterprises AS se ON mt.se_id = se.se_id
      JOIN programs AS p ON se.program_id = p.program_id
      JOIN sdg AS sdg ON sdg.sdg_id = ANY(se.sdg_id)

      -- Exclude mentorships that are already involved in a collaboration
      LEFT JOIN mentorship_collaborations mc1 
        ON mc1.seeking_collaboration_mentorship_id = mt.mentorship_id
      LEFT JOIN mentorship_collaborations mc2 
        ON mc2.suggested_collaborator_mentorship_id = mt.mentorship_id

      WHERE mt.mentor_id = $1
        AND mc1.seeking_collaboration_mentorship_id IS NULL
        AND mc2.suggested_collaborator_mentorship_id IS NULL

      GROUP BY 
        mt.mentorship_id,
        m.mentor_firstname, 
        m.mentor_lastname, 
        se.team_name, 
        se.se_id,
        m.mentor_id,
        p."name"

      ORDER BY se.team_name;
    `;

    const values = [mentor_id];
    const result = await pgDatabase.query(query, values);

    return result.rows.length ? result.rows : [];
  } catch (error) {
    console.error("❌ Error fetching mentorships (excluding current mentor):", error);
    return [];
  }
};

exports.getMentorshipsForScheduling = async (mentor_id) => {
  try {
    const query = `
      SELECT
        ms.mentorship_id AS id,
        m.mentor_id,
        CONCAT(m.mentor_firstname, ' ', m.mentor_lastname) AS mentor,
        se.se_id,
        se.team_name AS se,
        p."name" AS program,
        STRING_AGG(sdg."name", ', ') AS sdgs,
        se.preferred_mentoring_time,
        se.mentoring_time_note
      FROM mentorships AS ms
      JOIN socialenterprises AS se ON ms.se_id = se.se_id
      JOIN mentors AS m ON m.mentor_id = ms.mentor_id
      JOIN programs AS p ON se.program_id = p.program_id
      JOIN sdg AS sdg ON sdg.sdg_id = ANY(se.sdg_id)
      WHERE m.mentor_id = $1
        AND NOT EXISTS (
          SELECT 1
          FROM mentoring_session AS s
          WHERE s.mentorship_id = ms.mentorship_id
            AND s.status IN ('Pending', 'Pending SE', 'Accepted', 'In Progress')
        )
      GROUP BY
        ms.mentorship_id,
        m.mentor_id,
        CONCAT(m.mentor_firstname, ' ', m.mentor_lastname),
        se.se_id,
        se.team_name,
        p."name",
        se.preferred_mentoring_time,
        se.mentoring_time_note
    `;

    const { rows } = await pgDatabase.query(query, [mentor_id]);
    return rows;
  } catch (error) {
    console.error("❌ Error fetching options for mentoring session by mentor_id:", error);
    return [];
  }
};

exports.getMentorBySEID = async (client, se_id) => {
  const { rows } = await client.query(
    `SELECT m.mentor_id,
            m.mentor_firstname,
            m.mentor_lastname,
            ms.mentorship_id
       FROM mentorships AS ms 
       JOIN mentors AS m ON ms.mentor_id = m.mentor_id 
      WHERE ms.se_id = $1
      ORDER BY (ms.status = 'Active') DESC
      LIMIT 1;`,
    [se_id]
  );

  if (!rows.length) {
    throw Object.assign(new Error(`MENTOR_NOT_FOUND_FOR_SE:${se_id}`), { status: 404 });
  }

  return {
    name: `${rows[0].mentor_firstname} ${rows[0].mentor_lastname}`,
    mentor_id: rows[0].mentor_id,
    mentorship_id: rows[0].mentorship_id,
  };
};

exports.getSEWithMentors = async (program = null) => {
  try {
    let programFilter = program ? `AND p.name = '${program}'` : '';


    const query = `
            SELECT COUNT(DISTINCT ms.se_id) AS total_se_with_mentors 
            FROM mentorships AS ms
			JOIN socialenterprises AS s ON s.se_id = ms.se_id
			JOIN programs AS p ON p.program_id = s.program_id
            WHERE ms.status = 'Active'
			${programFilter};
        `;

    const result = await pgDatabase.query(query);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching mentorships", error);
    return []; // Return an empty array in case of an error
  }
};

exports.getHandledSEsCountByMentor = async (mentor_id) => {
  try {
    const query = `
          SELECT 
              mentor_id, 
              COUNT(DISTINCT se_id) AS num_se_handled
          FROM mentorships
          WHERE mentor_id = $1
          GROUP BY mentor_id;
      `;

    const result = await pgDatabase.query(query, [mentor_id]); // Correctly passing mentor_id

    return result.rows[0]?.num_se_handled || 0; // Return the count or 0 if no data found
  } catch (error) {
    console.error("❌ Error fetching mentorships:", error);
    return 0; // Return 0 in case of an error
  }
};

exports.getMentorshipCount = async () => {
  try {
    const query = `
            SELECT COUNT(DISTINCT m.mentor_id)
            FROM mentors m
            JOIN mentorships ms ON ms.mentor_id = m.mentor_id;
        `;

    const result = await pgDatabase.query(query);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching mentorships", error);
    return []; // Return an empty array in case of an error
  }
};

exports.getMentorSchedules = async () => {
  try {
    const query = `
        SELECT 
          m.mentor_id,
          m.mentor_firstname || ' ' || m.mentor_lastname AS mentor_name,
          se.team_name AS social_enterprise,
          ARRAY(SELECT unnest(mentorship_date)) AS mentorship_dates, 
          ARRAY(SELECT unnest(mentorship_time)) AS mentorship_times
        FROM mentorships ms
        JOIN mentors m ON ms.mentor_id = m.mentor_id
        JOIN social_enterprises se ON ms.se_id = se.se_id
        WHERE ms.status = 'Active';
      `;

    const result = await pgDatabase.query(query);
    if (!result.rows.length) {
      console.log("⚠️ No active mentorships found.");
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching mentor schedules:", error);
    return [];
  }
};

exports.getPendingSchedules = async (program = null, mentor_id) => {
  try {
    let programFilter = "";
    let params = [mentor_id];

    if (program) {
      programFilter = " AND p.name = $2";
      params = [mentor_id, program];
    }

    const query = `
      SELECT 
        ms.mentoring_session_id,
        m.mentorship_id, 
        se.team_name, 
        CONCAT(mt.mentor_firstname, ' ', mt.mentor_lastname) AS mentor_name,
        p.name AS program_name,
        TO_CHAR(ms.mentoring_session_date, 'FMMonth DD, YYYY') AS mentoring_session_date,
        CONCAT(
          TO_CHAR(ms.start_time, 'HH24:MI'), ' - ', 
          TO_CHAR(ms.end_time, 'HH24:MI')
        ) AS mentoring_session_time,
        ms.status,
        ms.zoom_link
      FROM mentorships m
      JOIN mentoring_session ms ON m.mentorship_id = ms.mentorship_id
      JOIN mentors mt ON m.mentor_id = mt.mentor_id
      JOIN socialenterprises se ON m.se_id = se.se_id
      JOIN programs p ON p.program_id = se.program_id
      WHERE ms.status = 'Pending'
        AND m.mentor_id IS DISTINCT FROM $1
        ${programFilter}
      ORDER BY ms.mentoring_session_date, ms.start_time;
    `;

    const result = await pgDatabase.query(query, params);
    if (!result.rows.length) {
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching pending schedules:", error);
    return [];
  }
};

exports.getSchedulingHistory = async (program = null) => {
  try {

    let programFilter = program ? `AND p.name = '${program}'` : '';

    const query = `
            SELECT 
                ms.mentoring_session_id,
                m.mentorship_id, 
                se.team_name, 
                CONCAT(mt.mentor_firstname, ' ', mt.mentor_lastname) AS mentor_name,
                p.name AS program_name,
                TO_CHAR(ms.mentoring_session_date, 'FMMonth DD, YYYY') AS mentoring_session_date,  -- ✅ Proper month name without spaces
                CONCAT(
                    TO_CHAR(ms.start_time, 'HH24:MI'), ' - ', 
                    TO_CHAR(ms.end_time, 'HH24:MI')
                ) AS mentoring_session_time,
                ms.status,
                ms.zoom_link
            FROM mentorships m
            JOIN mentoring_session ms ON m.mentorship_id = ms.mentorship_id
            JOIN mentors mt ON m.mentor_id = mt.mentor_id
            JOIN socialenterprises se ON m.se_id = se.se_id
            JOIN programs p ON p.program_id = se.program_id
            WHERE ms.status <> 'Pending'  -- ❌ Exclude pending sessions
            ${programFilter}
            ORDER BY ms.mentoring_session_date DESC, ms.start_time DESC;
        `;

    const result = await pgDatabase.query(query);
    if (!result.rows.length) {
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching scheduling history schedules:", error);
    return [];
  }
};

exports.getSchedulingHistoryByMentorID = async (mentor_id) => {
  try {
    const query = `
          SELECT 
              ms.mentoring_session_id,
              m.mentorship_id, 
              se.team_name, 
              CONCAT(mt.mentor_firstname, ' ', mt.mentor_lastname) AS mentor_name,
              p.name AS program_name,
              TO_CHAR(ms.mentoring_session_date, 'FMMonth DD, YYYY') AS mentoring_session_date,  -- ✅ Proper month name without spaces
              CONCAT(
                  TO_CHAR(ms.start_time, 'HH24:MI'), ' - ', 
                  TO_CHAR(ms.end_time, 'HH24:MI')
              ) AS mentoring_session_time,
              ms.status,
              ms.zoom_link
          FROM mentorships m
          JOIN mentoring_session ms ON m.mentorship_id = ms.mentorship_id
          JOIN mentors mt ON m.mentor_id = mt.mentor_id
          JOIN socialenterprises se ON m.se_id = se.se_id
          JOIN programs p ON p.program_id = se.program_id
          WHERE ms.status <> 'Pending'  -- ❌ Exclude pending sessions
          AND mt.mentor_id = $1
          ORDER BY ms.mentoring_session_date, ms.start_time;
      `;


    const result = await pgDatabase.query(query, [mentor_id]);
    if (!result.rows.length) {
      console.log("No Schedules found.");
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching scheduling history by mentor schedules:", error);
    return [];
  }
};

exports.getPendingSchedulesForMentor = async (mentor_id) => {
  try {
    const query = `
        SELECT 
            ms.mentoring_session_id,
            m.mentorship_id, 
            se.team_name, 
            CONCAT(mt.mentor_firstname, ' ', mt.mentor_lastname) AS mentor_name,
            p.name AS program_name,
            TO_CHAR(ms.mentoring_session_date, 'FMMonth DD, YYYY') AS mentoring_session_date,
            CONCAT(
                TO_CHAR(ms.start_time, 'HH24:MI'), ' - ', 
                TO_CHAR(ms.end_time, 'HH24:MI')
            ) AS mentoring_session_time,
            ms.status,
            ms.zoom_link
        FROM mentorships m
        JOIN mentoring_session ms ON m.mentorship_id = ms.mentorship_id
        JOIN mentors mt ON m.mentor_id = mt.mentor_id
        JOIN socialenterprises se ON m.se_id = se.se_id
        JOIN programs p ON p.program_id = se.program_id
        WHERE ms.status IN ('Pending', 'Pending SE')
          AND mt.mentor_id = $1
        ORDER BY ms.mentoring_session_date DESC, ms.start_time DESC;
      `;


    const result = await pgDatabase.query(query, [mentor_id]);
    if (!result.rows.length) {
      console.log("No Schedules found.");
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching scheduling history by mentor schedules:", error);
    return [];
  }
};

exports.getMentorshipCountByMentorID = async (mentor_id) => {
  try {
    const query = `
        SELECT COUNT(DISTINCT se_id) AS mentorship_count
        FROM mentorships
        WHERE mentor_id = $1;
      `;
    const result = await pgDatabase.query(query, [mentor_id]);
    if (!result.rows.length) {
      console.log("No Schedules found.");
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching scheduling history by mentor schedules:", error);
    return [];
  }
};

exports.getProgramCoordinatorsByMentorshipID = async (mentorship_id) => {
  try {
    const query = `
      SELECT DISTINCT pa.user_id, p.name AS program_name
      FROM mentorships AS ms
      JOIN socialenterprises AS s ON s.se_id = ms.se_id
      JOIN programs AS p ON p.program_id = s.program_id
      JOIN program_assignment AS pa ON pa.program_id = p.program_id
      WHERE ms.mentorship_id = $1;
    `;
    const result = await pgDatabase.query(query, [mentorship_id]);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching program coordinators by mentorship ID:", error);
    return [];
  }
};

exports.getSuggestedCollaborations = async (mentor_id, mentorship_id) => {
  try {
    const query = `
      WITH mentorship_traits AS (
        SELECT 
          m.mentorship_id,
          m.mentor_id,
          ec.category_name,
          ROUND(AVG(ec.rating), 2) AS avg_rating
        FROM mentorships m
        JOIN evaluations e ON e.se_id = m.se_id
        JOIN evaluation_categories ec ON ec.evaluation_id = e.evaluation_id
        GROUP BY m.mentorship_id, m.mentor_id, ec.category_name
      ),

      strengths AS (
        SELECT mentorship_id, category_name FROM mentorship_traits WHERE avg_rating > 3
      ),

      weaknesses AS (
        SELECT mentorship_id, category_name FROM mentorship_traits WHERE avg_rating <= 3
      ),

      complementary_matches AS (
        SELECT 
          a.mentorship_id AS mentorship_a,
          b.mentorship_id AS mentorship_b,
          COUNT(*) AS match_count,
          ARRAY_AGG(a.category_name ORDER BY a.category_name) AS matched_categories,
          1 AS priority
        FROM weaknesses a
        JOIN strengths b ON a.category_name = b.category_name AND a.mentorship_id <> b.mentorship_id
        GROUP BY a.mentorship_id, b.mentorship_id
        HAVING COUNT(*) >= 3
      ),

      shared_strengths AS (
        SELECT 
          a.mentorship_id AS mentorship_a,
          b.mentorship_id AS mentorship_b,
          COUNT(*) AS match_count,
          ARRAY_AGG(a.category_name ORDER BY a.category_name) AS matched_categories,
          2 AS priority
        FROM strengths a
        JOIN strengths b ON a.category_name = b.category_name AND a.mentorship_id <> b.mentorship_id
        GROUP BY a.mentorship_id, b.mentorship_id
        HAVING COUNT(*) >= 3
      ),

      shared_weaknesses AS (
        SELECT 
          a.mentorship_id AS mentorship_a,
          b.mentorship_id AS mentorship_b,
          COUNT(*) AS match_count,
          ARRAY_AGG(a.category_name ORDER BY a.category_name) AS matched_categories,
          3 AS priority
        FROM weaknesses a
        JOIN weaknesses b ON a.category_name = b.category_name AND a.mentorship_id <> b.mentorship_id
        GROUP BY a.mentorship_id, b.mentorship_id
        HAVING COUNT(*) >= 3
      ),

      all_matches AS (
        SELECT * FROM complementary_matches
        UNION ALL
        SELECT * FROM shared_strengths
        UNION ALL
        SELECT * FROM shared_weaknesses
      ),

      ranked_recommendations AS (
        SELECT 
          am.priority,
          ma.mentorship_id,
          ma.mentor_id,
          ma.se_id AS seeking_collaboration_se_id,
          mna.mentor_firstname || ' ' || mna.mentor_lastname AS seeking_collaboration_mentor_name,
          se_a.team_name AS seeking_collaboration_se_name,
          se_a.abbr AS seeking_collaboration_se_abbreviation,

          mb.mentorship_id AS suggested_collaborator_mentorship_id,
          mb.mentor_id AS suggested_collaboration_mentor_id,
          mb.se_id AS suggested_collaboration_se_id,
          mnb.mentor_firstname || ' ' || mnb.mentor_lastname AS suggested_collaboration_mentor_name,
          se_b.team_name AS suggested_collaboration_se_name,
          se_b.abbr AS suggested_collaboration_se_abbreviation,

          am.match_count,
          am.matched_categories,
          ROW_NUMBER() OVER (
            PARTITION BY ma.mentorship_id, am.priority
            ORDER BY am.match_count DESC, mnb.mentor_firstname
          ) AS row_num
        FROM all_matches am
        JOIN mentorships ma ON ma.mentorship_id = am.mentorship_a
        JOIN mentors mna ON mna.mentor_id = ma.mentor_id
        JOIN socialenterprises se_a ON se_a.se_id = ma.se_id
        JOIN mentorships mb ON mb.mentorship_id = am.mentorship_b
        JOIN mentors mnb ON mnb.mentor_id = mb.mentor_id
        JOIN socialenterprises se_b ON se_b.se_id = mb.se_id
        WHERE ma.mentorship_id = $2
          AND ma.mentor_id = $1
          AND mb.mentor_id <> ma.mentor_id
          AND NOT EXISTS (
            SELECT 1 FROM mentorship_collaborations mc
            WHERE (mc.seeking_collaboration_mentorship_id = am.mentorship_a AND mc.suggested_collaborator_mentorship_id = am.mentorship_b)
              OR (mc.seeking_collaboration_mentorship_id = am.mentorship_b AND mc.suggested_collaborator_mentorship_id = am.mentorship_a)
          )
          AND NOT EXISTS (
            SELECT 1 FROM mentorship_collaboration_requests req
            WHERE req.collaboration_card_id = CAST(mb.se_id AS text) || '_' || CAST(ma.se_id AS text)
          )
          -- ✅ NEW: Prevent suggestion if a request already exists for same seeking SE + tier
          AND NOT EXISTS (
            SELECT 1 FROM mentorship_collaboration_requests req
            WHERE SPLIT_PART(req.collaboration_card_id, '_', 2)::UUID = ma.se_id
              AND req.tier = am.priority
          )
      ),

      fallback_options AS (
        SELECT 
          m.mentorship_id AS suggested_collaborator_mentorship_id,
          m.mentor_id AS suggested_collaboration_mentor_id,
          m.se_id AS suggested_collaboration_se_id,
          mentors.mentor_firstname || ' ' || mentors.mentor_lastname AS suggested_collaboration_mentor_name,
          se_b.team_name AS suggested_collaboration_se_name,
          se_b.abbr AS suggested_collaboration_se_abbreviation,
          4 AS priority,

          CASE
            WHEN EXISTS (
              SELECT 1 FROM weaknesses w1
              JOIN strengths s1 ON w1.category_name = s1.category_name
              WHERE w1.mentorship_id = $2 AND s1.mentorship_id = m.mentorship_id
            ) THEN 1
            WHEN EXISTS (
              SELECT 1 FROM strengths s2
              JOIN strengths s3 ON s2.category_name = s3.category_name
              WHERE s2.mentorship_id = $2 AND s3.mentorship_id = m.mentorship_id
            ) THEN 2
            WHEN EXISTS (
              SELECT 1 FROM weaknesses w2
              JOIN weaknesses w3 ON w2.category_name = w3.category_name
              WHERE w2.mentorship_id = $2 AND w3.mentorship_id = m.mentorship_id
            ) THEN 3
            ELSE NULL
          END AS subtier
        FROM mentorships m
        JOIN mentors ON mentors.mentor_id = m.mentor_id
        JOIN socialenterprises se_b ON se_b.se_id = m.se_id
        WHERE m.mentorship_id <> $2
          AND m.mentor_id <> $1
          AND m.mentorship_id NOT IN (
            SELECT suggested_collaborator_mentorship_id FROM mentorship_collaborations WHERE seeking_collaboration_mentorship_id = $2
            UNION
            SELECT seeking_collaboration_mentorship_id FROM mentorship_collaborations WHERE suggested_collaborator_mentorship_id = $2
          )
          AND CAST(m.se_id AS TEXT) || '_' || (
            SELECT CAST(se_id AS TEXT) FROM mentorships WHERE mentorship_id = $2
          ) NOT IN (
            SELECT collaboration_card_id FROM mentorship_collaboration_requests
          )
          AND m.mentorship_id NOT IN (
            SELECT suggested_collaborator_mentorship_id FROM ranked_recommendations WHERE row_num = 1
          )
      )

      -- Final output
      SELECT 
        rr.priority AS tier,
        NULL AS subtier,
        rr.seeking_collaboration_mentor_name,
        rr.seeking_collaboration_se_name,
        rr.seeking_collaboration_se_abbreviation,
        rr.seeking_collaboration_se_id,
        rr.suggested_collaboration_mentor_name,
        rr.suggested_collaboration_mentor_id,
        rr.suggested_collaboration_se_name,
        rr.suggested_collaboration_se_abbreviation,
        rr.suggested_collaboration_se_id,
        rr.matched_categories,

        sa.strengths AS seeking_collaboration_se_strengths,
        wa.weaknesses AS seeking_collaboration_se_weaknesses,
        sb.strengths AS suggested_collaboration_se_strengths,
        wb.weaknesses AS suggested_collaboration_se_weaknesses,

        NOW() AS created_at
      FROM ranked_recommendations rr
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS strengths
        FROM mentorship_traits WHERE mentorship_id = rr.mentorship_id AND avg_rating > 3
      ) sa ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS weaknesses
        FROM mentorship_traits WHERE mentorship_id = rr.mentorship_id AND avg_rating <= 3
      ) wa ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS strengths
        FROM mentorship_traits WHERE mentorship_id = rr.suggested_collaborator_mentorship_id AND avg_rating > 3
      ) sb ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS weaknesses
        FROM mentorship_traits WHERE mentorship_id = rr.suggested_collaborator_mentorship_id AND avg_rating <= 3
      ) wb ON true
      WHERE rr.row_num = 1

      UNION ALL

      SELECT 
        f.priority,
        f.subtier,
        mna.mentor_firstname || ' ' || mna.mentor_lastname,
        se_a.team_name,
        se_a.abbr,
        se_a.se_id,
        f.suggested_collaboration_mentor_name,
        f.suggested_collaboration_mentor_id,
        f.suggested_collaboration_se_name,
        f.suggested_collaboration_se_abbreviation,
        f.suggested_collaboration_se_id,

        CASE 
          WHEN f.subtier = 1 THEN (
            SELECT ARRAY_AGG(w1.category_name ORDER BY w1.category_name)
            FROM weaknesses w1
            JOIN strengths s1 ON w1.category_name = s1.category_name
            WHERE w1.mentorship_id = ma.mentorship_id AND s1.mentorship_id = f.suggested_collaborator_mentorship_id
          )
          WHEN f.subtier = 2 THEN (
            SELECT ARRAY_AGG(s2.category_name ORDER BY s2.category_name)
            FROM strengths s2
            JOIN strengths s3 ON s2.category_name = s3.category_name
            WHERE s2.mentorship_id = ma.mentorship_id AND s3.mentorship_id = f.suggested_collaborator_mentorship_id
          )
          WHEN f.subtier = 3 THEN (
            SELECT ARRAY_AGG(w2.category_name ORDER BY w2.category_name)
            FROM weaknesses w2
            JOIN weaknesses w3 ON w2.category_name = w3.category_name
            WHERE w2.mentorship_id = ma.mentorship_id AND w3.mentorship_id = f.suggested_collaborator_mentorship_id
          )
          ELSE ARRAY[]::text[]
        END AS matched_categories,

        sa.strengths,
        wa.weaknesses,
        sb.strengths,
        wb.weaknesses,

        NOW()
      FROM fallback_options f
      JOIN mentorships ma ON ma.mentorship_id = $2
      JOIN mentors mna ON mna.mentor_id = ma.mentor_id
      JOIN socialenterprises se_a ON se_a.se_id = ma.se_id
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS strengths
        FROM mentorship_traits WHERE mentorship_id = ma.mentorship_id AND avg_rating > 3
      ) sa ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS weaknesses
        FROM mentorship_traits WHERE mentorship_id = ma.mentorship_id AND avg_rating <= 3
      ) wa ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS strengths
        FROM mentorship_traits WHERE mentorship_id = f.suggested_collaborator_mentorship_id AND avg_rating > 3
      ) sb ON true
      LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(category_name ORDER BY category_name) AS weaknesses
        FROM mentorship_traits WHERE mentorship_id = f.suggested_collaborator_mentorship_id AND avg_rating <= 3
      ) wb ON true
      ORDER BY tier, subtier NULLS LAST;
    `;

    const result = await pgDatabase.query(query, [mentor_id, mentorship_id]);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching suggested collaborations:", error);
    return [];
  }
};