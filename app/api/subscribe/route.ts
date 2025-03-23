import { NextResponse } from 'next/server';
import { getPoolForUser } from '@/lib/userdb'; // Import the getPoolForUser function

export async function POST(req: Request) {
  let client;
  try {
    const { userId, userCompanyId, tallyCompany } = await req.json();

    if (!userId || !userCompanyId || !tallyCompany) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 });
    }

    // Get the pool for the specific user
    const pool = getPoolForUser(userId); // Get the correct pool based on userId
    client = await pool.connect(); // Connect to the database

    const query = `
      INSERT INTO subscriber_db (user_id, company_id, tally_company)
      VALUES ($1, $2, $3)
      RETURNING subscribe_id
    `;
    const result = await client.query(query, [userId, userCompanyId, tallyCompany]);

    return NextResponse.json({ subscribeId: result.rows[0].subscribe_id });
  } catch (error) {
    console.error('Error in subscribe API:', error);
    return NextResponse.json({ error: 'An error occurred while subscribing' }, { status: 500 });
  } finally {
    if (client) {
      client.release(); // Make sure to release the client after the operation
    }
  }
}
