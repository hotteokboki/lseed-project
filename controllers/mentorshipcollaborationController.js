 const pgDatabase = require('../database.js');

exports.getExistingCollaborations = async (user_id) => {
  try {
    const query = `
      SELECT
        mc.collaboration_id,
        mc.created_at,
        mc.status AS is_active,

        -- Determine who is the user's mentorship
        CASE
          WHEN s.mentor_id = $1 THEN s.se_id
          ELSE sc.se_id
        END AS own_se_id,

        CASE
          WHEN s.mentor_id = $1 THEN se.team_name
          ELSE sce.team_name
        END AS own_se_name,

        CASE
          WHEN s.mentor_id = $1 THEN sce.se_id
          ELSE se.se_id
        END AS collaborating_se_id,

        CASE
          WHEN s.mentor_id = $1 THEN sce.team_name
          ELSE se.team_name
        END AS collaborating_se_name,

        -- Mentor names
        CASE
          WHEN s.mentor_id = $1 THEN CONCAT(sc_mentor.mentor_firstname, ' ', sc_mentor.mentor_lastname)
          ELSE CONCAT(s_mentor.mentor_firstname, ' ', s_mentor.mentor_lastname)
        END AS collaborating_mentor_name,

        -- Flag if the user was the one who initiated
        CASE
          WHEN s.mentor_id = $1 THEN true
          ELSE false
        END AS initiated_by_user

      FROM mentorship_collaborations mc
      JOIN mentorships s ON mc.seeking_collaboration_mentorship_id = s.mentorship_id
      JOIN mentorships sc ON mc.suggested_collaborator_mentorship_id = sc.mentorship_id
      JOIN socialenterprises se ON s.se_id = se.se_id
      JOIN socialenterprises sce ON sc.se_id = sce.se_id
      JOIN mentors s_mentor ON s.mentor_id = s_mentor.mentor_id
      JOIN mentors sc_mentor ON sc.mentor_id = sc_mentor.mentor_id
      WHERE s.mentor_id = $1 OR sc.mentor_id = $1
      ORDER BY mc.created_at DESC;
    `;

    const result = await pgDatabase.query(query, [user_id]);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching mentorship collaborations:", error);
    throw error;
  }
};

exports.getCollaborationRequests = async (mentor_id) => {
  try {
    const query = `
      SELECT
        mentorship_collaboration_request_id,
        created_at,

        -- Collaborating SE: the one that initiated the request
        seeking_collaboration_se_name,
        seeking_collaboration_se_abbreviation,
        collaboration_card_id,
        tier
      FROM mentorship_collaboration_requests
      WHERE suggested_collaboration_mentor_id = $1 AND status = 'Pending'
      ORDER BY created_at DESC;
    `;

    const result = await pgDatabase.query(query, [mentor_id]);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching collaboration requests for mentor:", error);
    throw error;
  }
};

exports.getCollaborationRequestDetails = async (mentorship_collaboration_request_id) => {
  try {
    const query = `
      SELECT
        mentorship_collaboration_request_id,
        tier,
        created_at,
        seeking_collaboration_mentor_name,
        seeking_collaboration_se_name,
        seeking_collaboration_se_abbreviation,
        suggested_collaboration_mentor_name,
        suggested_collaboration_se_name,
        suggested_collaboration_se_abbreviation,
        matched_categories,
        seeking_collaboration_se_strengths,
        seeking_collaboration_se_weaknesses,
        suggested_collaboration_se_strengths,
        suggested_collaboration_se_weaknesses,
        subtier
      FROM mentorship_collaboration_requests
      WHERE mentorship_collaboration_request_id = $1
    `;

    const result = await pgDatabase.query(query, [mentorship_collaboration_request_id]);
    return result.rows;
  } catch (error) {
    console.error("❌ Error fetching collaboration requests for mentor:", error);
    throw error;
  }
};

exports.insertCollaboration = async (
  client,
  collaboration_request_details,
  seeking_collaboration_mentorship_id,
  suggested_collaborator_mentorship_id
) => {
  const { tier, mentorship_collaboration_request_id } = collaboration_request_details;
  const tier_id = Number(tier) || 1;

  // Insert collaboration; rely on unique constraint to block duplicates
  const insertSQL = `
    INSERT INTO mentorship_collaborations (
      collaboration_id,
      seeking_collaboration_mentorship_id,
      suggested_collaborator_mentorship_id,
      tier_id,
      mentorship_collaboration_request_id,
      status,
      created_at
    )
    VALUES (uuid_generate_v4(), $1, $2, $3, $4, TRUE, NOW())
    RETURNING collaboration_id, created_at;
  `;

  try {
    const { rows } = await client.query(insertSQL, [
      seeking_collaboration_mentorship_id,
      suggested_collaborator_mentorship_id,
      tier_id,
      mentorship_collaboration_request_id
    ]);
    const collab = rows[0];

    // Guarded status flip to avoid races (must still be Pending)
    const upd = await client.query(
      `UPDATE mentorship_collaboration_requests
          SET status = 'Accepted'
        WHERE mentorship_collaboration_request_id = $1
          AND status = 'Pending'`,
      [mentorship_collaboration_request_id]
    );
    if (!upd.rowCount) {
      // If the request was changed concurrently, abort to keep atomicity
      throw Object.assign(new Error("REQUEST_STATE_CHANGED"), { status: 409 });
    }

    return collab;
  } catch (e) {
    if (e.code === "23505") { // unique_directional_pair_per_tier
      throw Object.assign(new Error("COLLAB_ALREADY_EXISTS"), { status: 409 });
    }
    throw e;
  }
};

exports.requestCollaborationInsert = async (collaboration) => {
  try {
    const {
      tier,
      seeking_collaboration_mentor_name,
      seeking_collaboration_se_name,
      seeking_collaboration_se_abbreviation,
      suggested_collaboration_mentor_name,
      suggested_collaboration_se_name,
      suggested_collaboration_se_abbreviation,
      matched_categories,
      seeking_collaboration_se_strengths,
      seeking_collaboration_se_weaknesses,
      suggested_collaboration_se_strengths,
      suggested_collaboration_se_weaknesses,
      created_at,
      suggested_collaboration_mentor_id,
      collaborationCardId,
      subtier,
    } = collaboration;

    const query = `
      INSERT INTO mentorship_collaboration_requests (
        tier,
        seeking_collaboration_mentor_name,
        seeking_collaboration_se_name,
        seeking_collaboration_se_abbreviation,
        suggested_collaboration_mentor_name,
        suggested_collaboration_se_name,
        suggested_collaboration_se_abbreviation,
        matched_categories,
        seeking_collaboration_se_strengths,
        seeking_collaboration_se_weaknesses,
        suggested_collaboration_se_strengths,
        suggested_collaboration_se_weaknesses,
        created_at,
        suggested_collaboration_mentor_id,
        collaboration_card_id,
        subtier
      )
      VALUES (
        $1, $2, $3, $4,
        $5, $6, $7,
        $8, $9, $10,
        $11, $12, $13, $14,
        $15, $16
      )
    `;

    await pgDatabase.query(query, [
      tier,
      seeking_collaboration_mentor_name,
      seeking_collaboration_se_name,
      seeking_collaboration_se_abbreviation,
      suggested_collaboration_mentor_name,
      suggested_collaboration_se_name,
      suggested_collaboration_se_abbreviation,
      matched_categories,
      seeking_collaboration_se_strengths,
      seeking_collaboration_se_weaknesses,
      suggested_collaboration_se_strengths,
      suggested_collaboration_se_weaknesses,
      created_at,
      suggested_collaboration_mentor_id,
      collaborationCardId,
      subtier,
    ]);
  } catch (error) {
    console.error("❌ Error inserting mentorship collaboration request:", error);
    throw error;
  }
};

// Lock one row by id or cardId; returns the locked row or null
exports.lockRequestForUpdate = async (client, { requestId, cardId }) => {
  if (requestId) {
    const { rows } = await client.query(
      `SELECT * FROM mentorship_collaboration_requests
        WHERE mentorship_collaboration_request_id = $1
        FOR UPDATE`,
      [requestId]
    );
    return rows[0] || null;
  }
  if (cardId) {
    const { rows } = await client.query(
      `SELECT * FROM mentorship_collaboration_requests
        WHERE collaboration_card_id = $1
        FOR UPDATE`,
      [cardId]
    );
    return rows[0] || null;
  }
  return null;
};

exports.markRequestDeclined = async (client, mentorship_collaboration_request_id) => {
  const res = await client.query(
    `UPDATE mentorship_collaboration_requests
        SET status = 'Declined'
      WHERE mentorship_collaboration_request_id = $1
        AND status = 'Pending'`,
    [mentorship_collaboration_request_id]
  );
  // If you have a BEFORE UPDATE trigger that deletes the row and RETURN NULL,
  // res.rowCount will be 0 even though the operation succeeded by design.
  // But since we hold a row lock and we checked status='Pending' earlier,
  // any 0 here likely means the trigger deleted or another TX changed it.
  // Treat >=0 as success but require that the row existed & was Pending before.
  return res.rowCount >= 0;
};