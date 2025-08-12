const express = require("express");
const bcrypt = require("bcrypt");
const speakeasy = require("speakeasy");
const QRCode = require("qrcode");

const router = express.Router();

const db = require("../database");
const { requireAuth } = require("./authRoutes");

// ------------------------
// Helpers (make this match the FE rules)
// ------------------------
const strongPassword = (pwd) => {
  if (typeof pwd !== "string") return false;
  const hasLen = pwd.length >= 8;          // 8 (not 12)
  const hasUpper = /[A-Z]/.test(pwd);
  const hasLower = /[a-z]/.test(pwd);
  const hasNum = /[0-9]/.test(pwd);
  const hasSpec = /[\W_]/.test(pwd);       // special char
  return hasLen && hasUpper && hasLower && hasNum && hasSpec;
};

// ------------------------
// 2FA
// ------------------------

/**
 * GET /api/security/2fa/status
 * -> { enabled: boolean }
 */
router.get("/2fa/status", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;

    const q = await db.query(
      `SELECT enabled FROM user_twofa WHERE user_id = $1`,
      [userId]
    );
    return res.json({ enabled: q.rows[0]?.enabled || false });
  } catch (err) {
    console.error("GET /2fa/status", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

/**
 * GET /api/security/2fa/setup
 * -> { qrCodeDataURL, secret }
 */
router.get("/2fa/setup", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;

    const u = await db.query(`SELECT email FROM users WHERE user_id = $1`, [userId]);
    const email = u.rows[0]?.email || `user-${userId}`;

    const secret = speakeasy.generateSecret({
      name: `LSEED (${email})`,
      length: 20,
    });

    // Upsert pending secret
    await db.query(
      `
      INSERT INTO user_twofa (user_id, pending_secret_base32, enabled)
      VALUES ($1, $2, false)
      ON CONFLICT (user_id)
      DO UPDATE SET pending_secret_base32 = EXCLUDED.pending_secret_base32
      `,
      [userId, secret.base32]
    );

    const qrCodeDataURL = await QRCode.toDataURL(secret.otpauth_url);

    res.json({
      qrCodeDataURL,
      secret: secret.base32, // optional to show
    });
  } catch (err) {
    console.error("GET /2fa/setup", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

/**
 * POST /api/security/2fa/enable
 * body: { code }
 */
router.post("/2fa/enable", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;
    const { code } = req.body;

    if (!code) return res.status(400).json({ message: "Missing code." });

    const row = await db.query(
      `SELECT pending_secret_base32 FROM user_twofa WHERE user_id = $1`,
      [userId]
    );
    const pending = row.rows[0]?.pending_secret_base32;
    if (!pending) return res.status(400).json({ message: "No 2FA setup in progress." });

    const ok = speakeasy.totp.verify({
      secret: pending,
      encoding: "base32",
      token: String(code),
      window: 1,
    });

    if (!ok) return res.status(400).json({ message: "Invalid 2FA code." });

    await db.query(
      `
      UPDATE user_twofa
      SET enabled = true,
          secret_base32 = $2,
          pending_secret_base32 = NULL
      WHERE user_id = $1
      `,
      [userId, pending]
    );

    res.json({ message: "2FA enabled." });
  } catch (err) {
    console.error("POST /2fa/enable", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

/**
 * POST /api/security/2fa/disable
 */
router.post("/2fa/disable", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;

    await db.query(
      `
      INSERT INTO user_twofa (user_id, enabled, secret_base32, pending_secret_base32)
      VALUES ($1, false, NULL, NULL)
      ON CONFLICT (user_id)
      DO UPDATE SET enabled=false, secret_base32=NULL, pending_secret_base32=NULL
      `,
      [userId]
    );

    res.json({ message: "2FA disabled." });
  } catch (err) {
    console.error("POST /2fa/disable", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

// ------------------------
// Security Questions
// ------------------------

/**
 * GET /api/security/security-questions
 * -> [{ question, answer: "" }, ...]
 * (Never return answers/hashes)
 */
router.get("/security-questions", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;

    const q = await db.query(
      `
      SELECT position, question
      FROM user_security_questions
      WHERE user_id = $1
      ORDER BY position ASC
      `,
      [userId]
    );

    res.json(q.rows.map((r) => ({ question: r.question, answer: "" })));
  } catch (err) {
    console.error("GET /security-questions", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

/**
 * PUT /api/security/security-questions
 * body: { questions: [{question, answer}, ...] }  (up to 3)
 * (Answers are hashed)
 */
router.put("/security-questions", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;
    const { questions } = req.body;

    if (!Array.isArray(questions) || questions.length === 0) {
      return res.status(400).json({ message: "Invalid questions payload." });
    }

    const trimmed = questions.slice(0, 3).map((q, i) => ({
      position: i + 1,
      question: String(q.question || "").trim(),
      answer: String(q.answer || "").trim(),
    }));

    if (trimmed.some((x) => !x.question || !x.answer)) {
      return res.status(400).json({ message: "Question and answer are required." });
    }

    // Hash answers
    const hashed = await Promise.all(
      trimmed.map(async (x) => ({
        position: x.position,
        question: x.question,
        answer_hash: await bcrypt.hash(x.answer, 12),
      }))
    );

    await db.query("BEGIN");
    try {
      for (const row of hashed) {
        await db.query(
          `
          INSERT INTO user_security_questions (user_id, position, question, answer_hash)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (user_id, position)
          DO UPDATE SET question = EXCLUDED.question,
                        answer_hash = EXCLUDED.answer_hash,
                        updated_at = now()
          `,
          [userId, row.position, row.question, row.answer_hash]
        );
      }
      await db.query("COMMIT");
    } catch (e) {
      await db.query("ROLLBACK");
      throw e;
    }

    res.json({ message: "Security questions updated." });
  } catch (err) {
    console.error("PUT /security-questions", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

// ------------------------
// Change Password
// ------------------------

/**
 * POST /api/security/change-password
 * body: { currentPassword, newPassword }
 */
router.post("/change-password", requireAuth, async (req, res) => {
  try {
    const userId = req.session?.user?.id;
    const { currentPassword, newPassword } = req.body || {};

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: "Missing fields." });
    }

    // Align with FE (8+ + upper + lower + number + special)
    if (!strongPassword(newPassword)) {
      return res.status(400).json({
        message:
          "Password must be at least 8 characters and include uppercase, lowercase, a number, and a special character.",
      });
    }

    // Fetch current hash
    const q = await db.query(`SELECT password FROM users WHERE user_id = $1`, [userId]);
    if (q.rows.length === 0) {
      return res.status(404).json({ message: "User not found." });
    }

    const currentHash = q.rows[0].password || "";
    const ok = await bcrypt.compare(currentPassword, currentHash);
    if (!ok) return res.status(401).json({ message: "Current password is incorrect." });

    const newHash = await bcrypt.hash(newPassword, 12);
    await db.query(`UPDATE users SET password = $2 WHERE user_id = $1`, [userId, newHash]);

    res.json({ message: "Password changed." });
  } catch (err) {
    console.error("POST /change-password", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

// POST /api/security/change-password/verify-current
router.post("/change-password/verify-current", async (req, res) => {
  try {
    const userId = req.session?.user?.id;
    const { currentPassword } = req.body || {};

    if (!currentPassword) {
      return res.status(400).json({ message: "Missing current password." });
    }

    const q = await db.query(`SELECT password FROM users WHERE user_id = $1`, [userId]);
    if (q.rows.length === 0) {
      return res.status(404).json({ message: "User not found." });
    }

    const ok = await bcrypt.compare(currentPassword, q.rows[0].password || "");
    if (!ok) return res.status(401).json({ message: "Current password is incorrect." });

    return res.json({ ok: true });
  } catch (err) {
    console.error("POST /change-password/verify-current", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});


module.exports = router;
