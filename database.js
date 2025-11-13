require('dotenv').config();
const { Pool } = require('pg');

// // Retrieve Database credentials from environment variables (Production [DLSU])
// const pool = new Pool({
// host: process.env.DATABASE_HOSTNAME,
//  user: process.env.DATABASE_USER,
// port: process.env.DATABASE_PORT,
//  password: process.env.DATABASE_KEY,
// database: process.env.DATABASE_NAME,
// max: 10, // Max connections in pool
// idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
// });

// Retrieve Database credentials from environment variables (Testing)
const pool = new Pool({
host: process.env.DATABASE_LOCALHOSTNAME,
user: process.env.DATABASE_LOCAL_USER,
port: process.env.DATABASE_PORT,
password: process.env.DATABASE_KEY,
database: process.env.DATABASE_LOCAL_NAME,
max: 10, // Max connections in pool
idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
});

pool.connect()
   .then(() => console.log("🔗 Connected to PostgreSQL"))
   .catch(err => console.error("❌ Connection error", err.stack));

module.exports = pool;
