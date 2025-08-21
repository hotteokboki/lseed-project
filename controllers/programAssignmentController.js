const pgDatabase = require("../database.js"); // Import PostgreSQL client

// Existing function (keep as is if you still need it for programs with assignments)
exports.getProgramCoordinators = async () => {
  try {
    const query = `
        SELECT
            p.program_id,
            p.name AS program_name,
            p.description,
            u.user_id AS coordinator_id,
            COALESCE(CONCAT(u.first_name, ' ', u.last_name), NULL) AS program_coordinator,
            COALESCE(u.email, NULL) AS coordinator_email
        FROM
            programs p
        LEFT JOIN
            program_assignment pa ON p.program_id = pa.program_id
        LEFT JOIN
            users u ON pa.user_id = u.user_id
        WHERE
            pa.user_id IS NULL
            OR EXISTS (
                SELECT 1
                FROM user_has_roles ur
                WHERE ur.user_id = u.user_id
                AND ur.role_name = 'LSEED-Coordinator'
            );
    `;
    const result = await pgDatabase.query(query);

    if (result.rows.length === 0) {
      return [];
    }

    return result.rows;
  } catch (error) {
    console.error("Error fetching program coordinators:", error);
    return [];
  }
};

exports.assignProgramCoordinator = async (program_id, user_id) => {
    try {
      // 1. Check if an assignment already exists for this program_id
      const checkQuery = `
        SELECT * FROM program_assignment WHERE program_id = $1;
      `;
      const existingAssignment = await pgDatabase.query(checkQuery, [program_id]);

      // Scenario A: No user_id provided (meaning unassign the coordinator)
      if (user_id === null) {
        if (existingAssignment.rows.length > 0) {
          // If an assignment exists, delete it
          const deleteQuery = `
            DELETE FROM program_assignment
            WHERE program_id = $1;
          `;
          await pgDatabase.query(deleteQuery, [program_id]);
          console.log(`Unassigned coordinator from program_id: ${program_id}`);
          return { message: "Coordinator unassigned successfully." };
        } else {
          // No assignment to delete, just return success
          console.log(`No coordinator assigned to program_id: ${program_id} to unassign.`);
          return { message: "No assignment found to unassign." };
        }
      }
      // Scenario B: A user_id is provided (meaning assign or re-assign)
      else {
        if (existingAssignment.rows.length > 0) {
          // If an assignment exists, update it
          const updateQuery = `
            UPDATE program_assignment
            SET user_id = $1
            WHERE program_id = $2
            RETURNING program_id, user_id; -- Return updated values
          `;
          const result = await pgDatabase.query(updateQuery, [user_id, program_id]);
          console.log(`Updated program_id: ${program_id} with new coordinator user_id: ${user_id}`);
          return result.rows[0]; // Return the updated row
        } else {
          // If no assignment exists, insert a new one
          const insertQuery = `
            INSERT INTO program_assignment (program_id, user_id)
            VALUES ($1, $2)
            RETURNING program_id, user_id; -- Return inserted values
          `;
          const result = await pgDatabase.query(insertQuery, [program_id, user_id]);
          console.log(`Assigned program_id: ${program_id} to coordinator user_id: ${user_id}`);
          return result.rows[0]; // Return the newly inserted row
        }
      }
    } catch (error) {
      console.error(`Database error in assignProgramCoordinator for program_id ${program_id}, user_id ${user_id}:`, error);
      throw error; // Re-throw the error to be caught by the API endpoint in server.js
    }
  }

exports.getProgramAssignment = async (user_id) => {
  try {
    const query = `
      SELECT p.program_id, p.name FROM program_assignment AS pa
      JOIN users AS u ON u.user_id = pa.user_id
      JOIN programs AS p ON pa.program_id = p.program_id
      WHERE pa.user_id = $1;
    `;
    const result = await pgDatabase.query(query, [user_id]);

    if (result.rows.length === 0) {
      return null;
    }

    return result.rows;
  } catch (error) {
    console.error("Error fetching program coordinators:", error);
    return null;
  }
};