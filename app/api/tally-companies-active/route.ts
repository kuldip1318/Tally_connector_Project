import { NextResponse } from 'next/server';
import { Pool, PoolConfig } from 'pg';

// Function to dynamically construct the database connection string
function getDbConfig(userId: string): PoolConfig {
  const dbName = `user_${userId}_db`;  // Dynamically create the database name
  return {
    connectionString: `postgres://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${dbName}`,
    ssl: process.env.NODE_ENV === 'production' 
      ? { rejectUnauthorized: false } 
      : undefined,
  };
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const userId = searchParams.get('userId');

  if (!userId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 400 });
  }

  // Dynamically create dbConfig based on the userId
  const dbConfig = getDbConfig(userId);

  // Create the pool for the specific user
  const pool = new Pool(dbConfig);

  let client;
  try {
    client = await pool.connect();
    
    const query = `
      SELECT 
        tally_company_id,
        user_id,
        tally_company_name,
        user_companies_names,
        active_status,
        created_at
      FROM tally_companies
      WHERE active_status = 1 AND user_id = $1
      LIMIT 1;
    `;
    
    const result = await client.query(query, [userId]);
    return NextResponse.json(result.rows[0] || null);
  } catch (error) {
    console.error('Error details:', error);
    
    if (error instanceof Error) {
      return NextResponse.json({ 
        error: 'Failed to fetch active tally company', 
        details: error.message 
      }, { status: 500 });
    }
    
    return NextResponse.json({ 
      error: 'An unknown error occurred' 
    }, { status: 500 });
  } finally {
    if (client) {
      client.release();
    }
  }
}
