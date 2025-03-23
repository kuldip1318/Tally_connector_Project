require('dotenv').config({ path: '.env.local' });
const { Pool } = require('pg');

console.log('Database connection details:');
console.log('Host:', process.env.DB_HOST);
console.log('Port:', process.env.DB_PORT);
console.log('Database:', process.env.DB_NAME);
console.log('User:', process.env.DB_USER);
console.log('Password:', process.env.DB_PASSWORD ? '[REDACTED]' : 'Not set');

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function createSubscribersTable() {
  try {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Create subscribers table
      await client.query(`
        CREATE TABLE IF NOT EXISTS subscribers (
          id SERIAL PRIMARY KEY,
          user_id INTEGER NOT NULL,
          company_id INTEGER NOT NULL,
          tally_company VARCHAR(255) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Add this after the existing table creation code
      await client.query(`
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = 'users_tb' AND column_name = 'user_id'
          ) THEN
            ALTER TABLE users_tb ADD COLUMN user_id SERIAL PRIMARY KEY;
          END IF;
        END $$;
      `);
      console.log('users_tb table updated with user_id column');

      // Update existing tables to reference subscribers table
      const tables = ['tally_data', 'tally_groups', 'tally_ledgers', 'ledger_monthly_summary'];
      for (const table of tables) {
        await client.query(`
          ALTER TABLE ${table}
          DROP CONSTRAINT IF EXISTS ${table}_subscribe_id_fkey,
          ADD CONSTRAINT ${table}_subscribe_id_fkey
          FOREIGN KEY (subscribe_id) REFERENCES subscribers(id)
        `);
      }

      await client.query('COMMIT');
      console.log('Subscribers table created and existing tables updated successfully');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Error creating subscribers table:', error);
  } finally {
    await pool.end();
  }
}

createSubscribersTable();

