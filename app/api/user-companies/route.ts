import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const userId = searchParams.get('userId');

  if (!userId) {
    return NextResponse.json({ error: 'User ID is required' }, { status: 400 });
  }

  let client;
  try {
    client = await pool.connect();
    
    // Query to get user companies
    const query = `
      SELECT 
        company_id as id,
        business_name as name,
        gst_number
      FROM company_details
      WHERE user_id = $1
    `;
    
    const result = await client.query(query, [userId]);
    
    // Ensure we always return an array
    const companies = result.rows.map(row => ({
      id: row.id?.toString() || '',
      name: row.name || 'Unnamed Company',
      gstNumber: row.gst_number || ''
    }));

    return NextResponse.json(companies);
  } catch (error) {
    console.error('Error fetching user companies:', error);
    return NextResponse.json({ error: 'Failed to fetch user companies' }, { status: 500 });
  } finally {
    if (client) {
      client.release();
    }
  }
}

