// lib/db.js
const { Pool } = require('pg');

// Default database configuration
const POSTGRES_CONFIG = {
  host: "98.85.154.231",  // Hardcoded DB host
  port: 5432,             // Hardcoded DB port
  database: "chainbook_db", // Hardcoded database name
  user: "postgres",       // Hardcoded DB user
  password: "admin",      // Hardcoded DB password
};

// Create and export the pool based on the default configuration
const pool = new Pool(POSTGRES_CONFIG);

module.exports = pool;
