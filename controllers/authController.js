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
  const { email } = req.body;
  try {
    // 1. Find the user by email
    const userQuery = await pgDatabase.query('SELECT * FROM users WHERE email = $1', [email]);
    if (userQuery.rows.length === 0) {
      return res.status(404).json({ message: 'No user with that email found.' });
    }

    const user = userQuery.rows[0];
    console.log('User found:', user.email); // Added for better logging

    // 2. Generate a token
    const token = crypto.randomBytes(32).toString('hex');
    const expires = new Date(Date.now() + 1000 * 60 * 15); // 15 minutes

    // 3. Save the token and expiry
    await pgDatabase.query(
      'INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES ($1, $2, $3)',
      [user.user_id, token, expires]
    );

    // 4. Send the email
    const resetLink = `${BASE_URL}/reset-password?token=${token}`;
    const transporter = nodemailer.createTransport({
      service: 'Gmail',
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
      },
    });

    await transporter.sendMail({
      from: `"LSEED Support" <${process.env.EMAIL_USER}>`,
      to: user.email,
      subject: 'Password Reset Request',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.6; color: #000;">
          <p style="margin: 0 0 16px;">Hi,</p>

          <p style="margin: 0 0 16px;">
            You requested a password reset. Click the link below to reset your password:
          </p>

          <p style="margin: 0 0 16px;">
            <a href="${resetLink}" style="color: #1a73e8;">${resetLink}</a>
          </p>

          <p style="margin: 0;">
            This link will expire in 15 minutes. Do not reply—this is an automated message.
          </p>
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
