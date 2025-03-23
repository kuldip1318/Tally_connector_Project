import { NextResponse } from 'next/server';
import {getPoolForUser} from '@/lib/userdb';

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const userId = searchParams.get('userId');
  const userCompanyId = searchParams.get('userCompanyId');

  if (!userId || !userCompanyId) {
    return NextResponse.json({ error: 'User ID and User Company ID are required' }, { status: 400 });
  }

  let client;
  try {
    const pool = getPoolForUser(userId);
    client = await pool.connect();
    const query = `
      SELECT subscribe_id, tally_company
      FROM subscriber_db
      WHERE user_id = $1 AND company_id = $2
    `;
    const result = await client.query(query, [userId, userCompanyId]);
    return NextResponse.json(result.rows);
  } catch (error) {
    console.error('Error fetching subscribed companies:', error);
    if (error instanceof Error) {
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
    }
    return NextResponse.json({ error: 'An error occurred while fetching subscribed companies' }, { status: 500 });
  } finally {
    if (client) {
      client.release();
    }
  }
}

