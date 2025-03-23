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

async function getPrimaryKeyColumn(client, tableName) {
  const result = await client.query(`
    SELECT column_name 
    FROM information_schema.key_column_usage 
    WHERE table_name = $1 AND constraint_name = $1 || '_pkey'
  `, [tableName]);
  return result.rows.length > 0 ? result.rows[0].column_name : null;
}

async function tableExists(client, tableName) {
  const result = await client.query(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_name = $1
    );
  `, [tableName]);
  return result.rows[0].exists;
}

async function columnExists(client, tableName, columnName) {
  const result = await client.query(`
    SELECT EXISTS (
      SELECT FROM information_schema.columns 
      WHERE table_name = $1 AND column_name = $2
    );
  `, [tableName, columnName]);
  return result.rows[0].exists;
}

async function createTables() {
  let client;
  try {
    client = await pool.connect();
    console.log('Connected to the database successfully.');

    await client.query('BEGIN');

    // Check and create/modify users_tb table
    if (await tableExists(client, 'users_tb')) {
      console.log('users_tb table already exists. Checking structure...');

      const primaryKeyColumn = await getPrimaryKeyColumn(client, 'users_tb');

      if (!primaryKeyColumn) {
        if (!(await columnExists(client, 'users_tb', 'user_id'))) {
          await client.query(`
        ALTER TABLE users_tb
        ADD COLUMN user_id SERIAL PRIMARY KEY;
      `);
          console.log('Added user_id column as primary key to users_tb table.');
        } else {
          await client.query(`
        ALTER TABLE users_tb
        ADD PRIMARY KEY (user_id);
      `);
          console.log('Set user_id as primary key for users_tb table.');
        }
      } else {
        console.log(`Primary key (${primaryKeyColumn}) already exists for users_tb table.`);
      }

      // Check and add other columns if necessary
      const columns = ['full_name', 'email', 'mobile_number', 'password_hash', 'created_at'];
      for (const column of columns) {
        if (!(await columnExists(client, 'users_tb', column))) {
          await client.query(`
        ALTER TABLE users_tb
        ADD COLUMN ${column} ${column === 'created_at' ? 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' : 'VARCHAR(255)'};
      `);
          console.log(`Added ${column} column to users_tb table.`);
        }
      }
    } else {
      await client.query(`
    CREATE TABLE users_tb (
      user_id SERIAL PRIMARY KEY,
      full_name VARCHAR(255) NOT NULL,
      email VARCHAR(255) UNIQUE NOT NULL,
      mobile_number VARCHAR(20) UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
      console.log('users_tb table created successfully.');
    }

    // Check and create/modify company_details table
    if (await tableExists(client, 'company_details')) {
      console.log('company_details table already exists. Checking structure...');

      const primaryKeyColumn = await getPrimaryKeyColumn(client, 'company_details');

      if (!primaryKeyColumn) {
        if (!(await columnExists(client, 'company_details', 'company_id'))) {
          await client.query(`
        ALTER TABLE company_details
        ADD COLUMN company_id SERIAL PRIMARY KEY;
      `);
          console.log('Added company_id column as primary key to company_details table.');
        } else {
          await client.query(`
        ALTER TABLE company_details
        ADD PRIMARY KEY (company_id);
      `);
          console.log('Set company_id as primary key for company_details table.');
        }
      } else {
        console.log(`Primary key (${primaryKeyColumn}) already exists for company_details table.`);
      }

      // Check and add other columns if necessary
      const columns = ['business_name', 'gst_number', 'created_at'];
      for (const column of columns) {
        if (!(await columnExists(client, 'company_details', column))) {
          await client.query(`
        ALTER TABLE company_details
        ADD COLUMN ${column} ${column === 'created_at' ? 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP' : 'VARCHAR(255)'};
      `);
          console.log(`Added ${column} column to company_details table.`);
        }
      }
    } else {
      await client.query(`
    CREATE TABLE company_details (
      company_id SERIAL PRIMARY KEY,
      business_name VARCHAR(255) NOT NULL,
      gst_number VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
      console.log('company_details table created successfully.');
    }

    // Check and create subscriber_db table
    if (!(await tableExists(client, 'subscriber_db'))) {
      const usersPrimaryKey = await getPrimaryKeyColumn(client, 'users_tb');
      const companyPrimaryKey = await getPrimaryKeyColumn(client, 'company_details');

      await client.query(`
    CREATE TABLE subscriber_db (
      subscribe_id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      company_id INTEGER NOT NULL,
      tally_company VARCHAR(255) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users_tb(${usersPrimaryKey}),
      FOREIGN KEY (company_id) REFERENCES company_details(${companyPrimaryKey})
    )
  `);
      console.log('subscriber_db table created successfully.');
    } else {
      console.log('subscriber_db table already exists.');
    }

    // Check and create other tables (tally_data, tally_groups, tally_ledgers, ledger_monthly_summary)
    const otherTables = ['tally_data', 'tally_groups', 'tally_ledgers', 'ledger_monthly_summary'];
    for (const tableName of otherTables) {
      if (!(await tableExists(client, tableName))) {
        const usersPrimaryKey = await getPrimaryKeyColumn(client, 'users_tb');
        const companyPrimaryKey = await getPrimaryKeyColumn(client, 'company_details');

        let createTableQuery = `
          CREATE TABLE ${tableName} (
            id SERIAL PRIMARY KEY,
            subscribe_id INTEGER REFERENCES subscriber_db(subscribe_id),
            user_id INTEGER REFERENCES users_tb(${usersPrimaryKey}),
            company_id INTEGER REFERENCES company_details(${companyPrimaryKey}),
        `;

        if (tableName === 'tally_data') {
          createTableQuery += `
            date DATE,
            voucher_type TEXT,
            voucher_number TEXT,
            party_name TEXT,
            voucher_category TEXT,
            narration TEXT,
            ledger TEXT,
            amount NUMERIC
          )`;
        } else if (tableName === 'tally_groups') {
          createTableQuery += `
            GUID TEXT UNIQUE,
            Name TEXT,
            Parent TEXT,
            PrimaryGroup TEXT,
            Nature TEXT,
            Sign TEXT,
            Gross_Net_Profit TEXT,
            SortPosition BIGINT
          )`;
        } else if (tableName === 'tally_ledgers') {
          createTableQuery += `
            GUID TEXT UNIQUE,
            Name TEXT,
            Parent TEXT,
            Nature TEXT,
            Sign TEXT,
            SortPosition BIGINT
          )`;
        } else if (tableName === 'ledger_monthly_summary') {
          createTableQuery += `
            ledger TEXT,
            financial_year TEXT,
            month TEXT,
            opening NUMERIC,
            debit NUMERIC,
            credit NUMERIC,
            closing NUMERIC,
            CONSTRAINT ${tableName}_unique_constraint 
            UNIQUE (subscribe_id, user_id, company_id, ledger, financial_year, month)
          )`;
        }

        await client.query(createTableQuery);
        console.log(`${tableName} table created successfully.`);
      } else {
        console.log(`${tableName} table already exists.`);
      }
    }

    await client.query('COMMIT');
    console.log('All tables checked/created successfully');
  } catch (error) {
    if (client) await client.query('ROLLBACK');
    console.error('Error creating tables:', error);
  } finally {
    if (client) client.release();
    await pool.end();
  }
}

createTables();

