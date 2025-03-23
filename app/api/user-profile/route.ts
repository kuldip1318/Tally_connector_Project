import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const userId = searchParams.get('userId');

  if (!userId) {
    console.error('User ID is missing in the request');
    return NextResponse.json({ error: 'User ID is required' }, { status: 400 });
  }

  let client;
  try {
    client = await pool.connect();
    console.log('Database connection established');
    
    const query = `
      SELECT full_name, email, mobile_number
      FROM users_tb
      WHERE id = $1
    `;
    
    console.log('Executing query:', query);
    console.log('User ID:', userId);

    const result = await client.query(query, [userId]);
    console.log('Query executed, rows returned:', result.rows.length);
    
    if (result.rows.length === 0) {
      console.log('User not found for ID:', userId);
      return NextResponse.json({ error: 'User not found' }, { status: 404 });
    }

    console.log('User profile fetched successfully');
    return NextResponse.json(result.rows[0]);
  } catch (error) {
    console.error('Error fetching user profile:', error);
    if (error instanceof Error) {
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
    }
    return NextResponse.json({ error: 'Failed to fetch user profile', details: error instanceof Error ? error.message : 'Unknown error' }, { status: 500 });
  } finally {
    if (client) {
      client.release();
      console.log('Database connection released');
    }
  }
}

