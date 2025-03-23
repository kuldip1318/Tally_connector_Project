import { Pool, PoolConfig } from 'pg';

const POSTGRES_CONFIG: PoolConfig = {
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT || "5432"),
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  connectionTimeoutMillis: 5000, // 5 seconds timeout
};

// Remove undefined values and log configuration
Object.keys(POSTGRES_CONFIG).forEach(key => {
  if (POSTGRES_CONFIG[key as keyof PoolConfig] === undefined) {
    console.warn(`Warning: ${key} is undefined in POSTGRES_CONFIG`);
    delete POSTGRES_CONFIG[key as keyof PoolConfig];
  }
});

console.log('PostgreSQL Configuration:', {
  ...POSTGRES_CONFIG,
  password: POSTGRES_CONFIG.password ? '[REDACTED]' : undefined
});

const pool = new Pool(POSTGRES_CONFIG);

// Test the database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Error connecting to the database:', err);
  } else {
    console.log('Successfully connected to the database');
  }
});

export default pool;

