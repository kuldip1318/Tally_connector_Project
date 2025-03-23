import { NextResponse } from 'next/server';
import { Pool } from 'pg';
import axios from 'axios';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const userId = searchParams.get('userId');

  if (!userId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 400 });
  }

  try {
    const client = await pool.connect();
    try {
      // Fetch companies from database
      const dbQuery = `
        SELECT company_id, business_name, gst_number
        FROM company_details
        WHERE user_id = $1
      `;
      const dbResult = await client.query(dbQuery, [userId]);
      const dbCompanies = dbResult.rows;

      // Fetch companies from Tally
      const tallyResponse = await axios.get(`${process.env.APP_URL}/api/tally/companies`);
      const tallyCompanies = tallyResponse.data;

      // Merge and deduplicate companies
      const allCompanies = [...dbCompanies];
      for (const tallyCompany of tallyCompanies) {
        if (!dbCompanies.some(dbCompany => dbCompany.business_name === tallyCompany.name)) {
          // Insert new Tally company into database
          const insertQuery = `
            INSERT INTO company_details (user_id, business_name, gst_number)
            VALUES ($1, $2, $3)
            RETURNING company_id, business_name, gst_number
          `;
          const insertResult = await client.query(insertQuery, [userId, tallyCompany.name, '']);
          allCompanies.push(insertResult.rows[0]);
        }
      }

      return NextResponse.json(allCompanies);
    } finally {
      client.release();
    }
  } catch (error: unknown) {
    console.error('Error fetching companies:', error);
    if (error instanceof Error) {
      return NextResponse.json({ error: error.message }, { status: 500 });
    } else {
      return NextResponse.json({ error: 'An unknown error occurred while fetching companies' }, { status: 500 });
    }
  }
}

