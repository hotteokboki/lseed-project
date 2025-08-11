const express = require("express");
const bcrypt = require('bcrypt'); // For password hashing
const session = require("express-session");
const cookieParser = require('cookie-parser');
const { login, logout, protectedRoute, forgotPassword } = require("../controllers/authController");
const pgDatabase = require("../database.js"); // Import PostgreSQL client

const router = express.Router();

router.use(cookieParser()); // Middleware to parse cookies

// router.post("/", login);
router.post('/forgot-password', forgotPassword);
// router.get("/logout", logout);
router.get("/protected", protectedRoute);

const saltRounds = 10;

// const sessionId = crypto.randomUUID();

const requireAuth = (req, res, next) => {
  if (!req.session || !req.session.user) {
    console.log("🚨 Unauthorized: No session found.");
    return res.status(401).json({ error: "Unauthorized: No session found." });
  }

  console.log("[authRoutes]✅ User is authenticated:", req.session.user);
  // Add additional validation, like checking the session ID in a database or store if needed.
  next();
};

router.post('/reset-password', async (req, res) => {
  const { token, newPassword } = req.body;
  console.log('POST /reset-password hit with token:', token); // Logging

  try {
    // Check if token exists and is not expired
    const result = await pgDatabase.query(
      `SELECT * FROM password_reset_tokens WHERE token = $1`,
      [token]
    );

    if (result.rows.length === 0) {
      console.log('Invalid or expired token:', token); // Logging
      return res.status(400).json({ message: 'Invalid or expired token' });
    }

    const tokenData = result.rows[0];
    const today = new Date();
    const tokenExpiry = new Date(tokenData.expires_at);

    if (today > tokenExpiry) {
      console.log('Token has expired for token:', token); // Logging
      return res.status(400).json({ message: 'Token has expired' });
    }

    // Hash the new password
    const hashedPassword = await bcrypt.hash(newPassword, saltRounds);
    console.log('New password hashed.'); // Logging

    // Update user password
    await pgDatabase.query(
      `UPDATE users SET password = $1 WHERE user_id = $2`,
      [hashedPassword, tokenData.user_id]
    );
    console.log('User password updated for user_id:', tokenData.user_id, ' password: ', hashedPassword); // Logging

    // Delete token to prevent reuse
    await pgDatabase.query(`DELETE FROM password_reset_tokens WHERE token = $1`, [token]);
    console.log('Password reset token deleted.'); // Logging

    return res.json({ message: 'Password has been reset successfully' });
  } catch (err) {
    console.error('Error resetting password:', err);
    // console.error('Full Error Object:', err); // Log full error for detailed debugging
    return res.status(500).json({ message: 'Internal server error' });
  }
});

// router.post('/login', async (req, res) => {
//   const { email, password } = req.body;

//   try {
//     // Query to fetch user data from PostgreSQL
//     const result = await pgDatabase.query('SELECT * FROM users WHERE email = $1', [email]);
//     const user = result.rows[0]; // Assuming email is unique, we take the first result

//     console.log("[Login Route] Fetched user from DB:", user);

//     if (!user) {
//       return res.status(401).json({ message: 'Invalid credentials' });
//     }

//     // Verify password
//     const isPasswordValid = await bcrypt.compare(password, user.password);
//     if (!isPasswordValid) {
//       return res.status(401).json({ message: 'Invalid credentials' });
//     }

//     console.log("[Login Route] User object before session storage:", user);

//     // Check if account is active
//     if (!user.isactive) {
//       return res.status(403).json({ message: 'Your account is pending verification. Please wait for LSEED to verify your account.' });
//     }

//     // ✅ Generate a unique session ID
//     const sessionId = crypto.randomUUID();

//     try {
//       console.log('[authRoutes] Inserting session into active_sessions');
//       // ✅ Insert the session ID into `active_sessions`
//       const sessionInsertQuery = `
//         INSERT INTO active_sessions (session_id, user_id) VALUES ($1, $2)
//       `;
//       await pgDatabase.query(sessionInsertQuery, [sessionId, user.user_id]);
      
//     } catch (error) {
//       console.error('Error inserting session:', error);
//     }
//     console.log("[authRoutes] Session ID (global):", sessionId);

//     // ✅ Store user details in the session
//     req.session.user = {
//       id: user.user_id,
//       email: user.email,
//       role: user.cleanedRoles,
//       firstName: user.first_name,
//       lastName: user.last_name,
//       sessionId: sessionId,
//     };

//     console.log("[authRoutes] Session after login:", req.session.user.sessionId); // Add this log

//     // ✅ Set session ID in a cookie
//     res.cookie("session_id", sessionId, { httpOnly: true, secure: false });

//     // Separate handling for admin and normal users
//     if (user.roles === "Administrator") {
//       return res.json({
//         message: "Admin login successful",
//         user: { id: user.user_id, email: user.email, role: user.roles },
//         session_id: sessionId,
//         redirect: "/admin",
//       });
//     } else {
//       return res.json({
//         message: "User login successful",
//         user: { id: user.user_id, email: user.email, role: user.roles, firstname: user.first_name, lastname: user.last_name },
//         session_id: sessionId,
//         redirect: "/dashboard", // Normal users go to their dashboard
//       });
//     }
//   } catch (error) {
//     console.error('Login Error:', error);
//     res.status(500).json({ message: 'Internal server error' });
//   }
// });

router.get("/users", async (req, res) => {
  try {
    const query = `
      SELECT
          u.user_id AS id,
          u.first_name || ' ' || u.last_name AS name,
          u.email,
          -- Aggregate roles into an array
          ARRAY_AGG(uhr.role_name) AS roles
      FROM
          users u
      LEFT JOIN
          user_has_roles uhr ON u.user_id = uhr.user_id
      GROUP BY
          u.user_id, u.first_name, u.last_name, u.email
      ORDER BY
          u.first_name, u.last_name;
    `;

    const result = await pool.query(query);

    // Clean up the roles array from [null] to [] for users with no roles
    const usersWithCleanRoles = result.rows.map(user => {
        // If roles is [null], change it to an empty array
        const cleanedRoles = user.roles && user.roles.length > 0 && user.roles[0] !== null
                             ? user.roles
                             : [];
        return {
            ...user,
            roles: cleanedRoles,
        };
    });

    res.json(usersWithCleanRoles);
  } catch (err) {
    console.error("Error fetching users:", err.message);
    res.status(500).json({ error: "Server error fetching users" });
  }
});

router.get("/mentors", async (req, res) => {
  try {
    const result = await pgDatabase.query(`SELECT mentor_id, mentor_firstname || ' ' || mentor_lastname AS name, calendarlink FROM mentors`);
    res.json(result.rows);
  } catch (err) {
    console.error("Database Error:", err);  // Improved logging
    res.status(500).json({ error: "Error fetching mentors" });
  }
});

module.exports = {
  router,
  requireAuth,
};