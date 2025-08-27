const pgDatabase = require('../database.js'); // Import PostgreSQL client

exports.createNotification = async (receiverId, title, message, targetRoute, client = null) => {
  // Basic validation (fail fast, keep DB consistent)
  if (!receiverId || !/^[0-9a-fA-F-]{36}$/.test(receiverId)) {
    const err = new Error("INVALID_RECEIVER_ID");
    err.status = 400;
    throw err;
  }
  if (!title || !message || !targetRoute) {
    const err = new Error("MISSING_FIELDS");
    err.status = 400;
    throw err;
  }

  // Optional normalization (avoid oversized payloads)
  const _title = String(title).trim().slice(0, 300);
  const _message = String(message).trim().slice(0, 5000);
  const _route = String(targetRoute).trim().slice(0, 1000);

  const ownClient = !client;
  if (ownClient) client = await pgDatabase.connect();

  // TX strategy: either BEGIN/COMMIT or SAVEPOINT/RELEASE to play nice with outer TX
  const savepoint = "sp_notify";

  try {
    if (ownClient) {
      await client.query("BEGIN");
      // Explicit for clarity; READ COMMITTED is default and adequate for a single insert.
      await client.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    } else {
      await client.query(`SAVEPOINT ${savepoint}`);
    }

    const insertSQL = `
      INSERT INTO notification (notification_id, receiver_id, title, message, target_route, created_at)
      VALUES (uuid_generate_v4(), $1, $2, $3, $4, NOW())
      RETURNING notification_id, receiver_id, title, message, target_route, created_at;
    `;
    const { rows } = await client.query(insertSQL, [receiverId, _title, _message, _route]);
    const inserted = rows[0];

    if (ownClient) {
      await client.query("COMMIT");
    } else {
      await client.query(`RELEASE SAVEPOINT ${savepoint}`);
    }
    return inserted;
  } catch (e) {
    // Rollback only our scope
    try {
      if (ownClient) {
        await client.query("ROLLBACK");
      } else {
        await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
      }
    } catch (_) {}

    // Integrity/consistency hints
    if (e.code === "23503") { // foreign_key_violation
      const err = new Error("INVALID_RECEIVER_ID");
      err.status = 400;
      throw err;
    }
    if (e.code === "23514") { // check_violation
      const err = new Error("CHECK_CONSTRAINT_VIOLATION");
      err.status = 400;
      throw err;
    }
    // Let the caller decide how to surface other errors
    throw e;
  } finally {
    if (ownClient) client.release();
  }
}