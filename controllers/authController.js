const bcrypt = require("bcryptjs");
const pgDatabase = require("../database.js"); // Import PostgreSQL client
const crypto = require("crypto"); // To generate session ID
const nodemailer = require('nodemailer');
const BASE_URL = 'http://localhost:3000';

// // Login route
// exports.login = async (req, res) => {
//   const { email, password } = req.body;

//   try {
//     // Query the Users table for the user based on username
//     const query = "SELECT * FROM users WHERE email = $1";
//     const values = [email];

//     const result = await pgDatabase.query(query, values);
//     const user = result.rows[0];

//     if (!user) {
//       return res.status(401).json({ error: "Invalid credentials" });
//     }

//     // Compare the provided password with the hashed password
//     const isPasswordValid = bcrypt.compareSync(password, user.password);
//     if (!isPasswordValid) {
//       return res.status(401).json({ error: "Invalid credentials" });
//     }

//     try {
//       // console.log('Inserting session into active_sessions');
//       // ✅ Insert the session ID into `active_sessions`
//       const sessionInsertQuery = `
//         INSERT INTO active_sessions (session_id, user_id) VALUES ($1, $2)
//       `;
//       await pgDatabase.query(sessionInsertQuery, [sessionId, user.user_id]);
//       // console.log('Session inserted');
//     } catch (error) {
//       console.error('Error inserting session:', error);
//     }

//     // ✅ Store session in `req.session`
//     req.session.user = {
//       id: user.user_id,
//       email: user.email,
//       role: user.roles,
//       sessionId: sessionId,  // Store session ID in session object
//     };

//     // ✅ Set session ID in a cookie (optional)
//     res.cookie("session_id", sessionId, { httpOnly: true, secure: false });

//     res.json({
//       message: "Login successful",
//       user: { id: user.user_id, email: user.email, role: user.roles },
//       session_id: sessionId
//     });
//   } catch (err) {
//     console.error(err);
//     res.status(500).json({ error: "Internal Server Error" });
//   }
// };

exports.forgotPassword = async (req, res) => {
  const { email, answers } = req.body; // answers is optional: [{position, answer}, ...]
  try {
    // 1) Find user
    const uRes = await pgDatabase.query('SELECT user_id, email FROM users WHERE email = $1', [email]);
    if (uRes.rows.length === 0) {
      return res.status(404).json({ message: 'No user with that email found.' });
    }
    const user = uRes.rows[0];

    // 2) Do they have security questions?
    const sqRes = await pgDatabase.query(
      `
        SELECT position, question, answer_hash
        FROM user_security_questions
        WHERE user_id = $1
        ORDER BY position ASC
      `,
      [user.user_id]
    );

    // 2a) If they have SQ and client hasn't answered yet, return the questions (no hashes)
    if (sqRes.rows.length > 0 && !Array.isArray(answers)) {
      return res.json({
        requiresSecurityQuestions: true,
        questions: sqRes.rows.map(r => ({ position: r.position, question: r.question }))
      });
    }

    // 2b) If they have SQ and answers were provided, verify
    if (sqRes.rows.length > 0 && Array.isArray(answers)) {
      // Build a quick lookup by position
      const byPos = new Map(sqRes.rows.map(r => [r.position, r]));
      for (const a of answers) {
        const rec = byPos.get(Number(a.position));
        if (!rec) {
          return res.status(400).json({ message: 'Invalid security question position.' });
        }
        const ok = await bcrypt.compare(String(a.answer || '').trim(), rec.answer_hash);
        if (!ok) {
          return res.status(400).json({ message: 'Security answer is incorrect.' });
        }
      }
      // All answers correct → continue to send email
    }

    // 3) Generate token + expiry
    const token = crypto.randomBytes(32).toString('hex');
    const expires = new Date(Date.now() + 15 * 60 * 1000); // 15 min

    await pgDatabase.query(
      `INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES ($1, $2, $3)`,
      [user.user_id, token, expires]
    );

    // 4) Send email with link
    const resetLink = `${BASE_URL}/reset-password?token=${token}`;
    const transporter = nodemailer.createTransport({
      service: 'Gmail',
      auth: { user: process.env.EMAIL_USER, pass: process.env.EMAIL_PASS },
    });

    await transporter.sendMail({
      from: `"LSEED Support" <${process.env.EMAIL_USER}>`,
      to: user.email,
      subject: 'Password Reset Request',
      html: `
        <div style="font-family: Arial, sans-serif; line-height:1.6; color:#000;">
          <p>Hi,</p>
          <p>You requested a password reset. Click the link below to reset your password:</p>
          <p><a href="${resetLink}" style="color:#1a73e8;">${resetLink}</a></p>
          <p>This link will expire in 15 minutes. Do not reply—this is an automated message.</p>
        </div>
      `,
    });

    return res.json({ message: 'Password reset link sent to your email.' });
  } catch (err) {
    console.error('Error in forgotPassword:', err);
    return res.status(500).json({ message: 'Something went wrong.' });
  }
};

// Protected route
exports.protectedRoute = (req, res) => {
  if (!req.session.user) {
    return res.status(403).json({ error: "Unauthorized" });
  }
  res.json({ message: `Welcome ${req.session.user.username}` });
};
