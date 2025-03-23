import { NextResponse } from 'next/server';
import { TallyLedgerSync } from '../../../../lib/TallyLedgerSync';

export async function POST(req: Request) {
  try {
    // Parse the request body to get the user_id
    const body = await req.json();
    const userId = body.user_id; // Assume the client sends { user_id: 1 }

    if (!userId || isNaN(userId)) {
      return NextResponse.json(
        { message: 'Invalid or missing user_id' },
        { status: 400 }
      );
    }

    // Construct the database name dynamically
    const databaseName = `user_${userId}_db`;

    // Set up database parameters
    const dbParams = {
      host: process.env.DB_HOST || '',
      port: parseInt(process.env.DB_PORT || '5432'),
      database: databaseName, // Use the dynamically constructed database name
      user: process.env.DB_USER || '',
      password: process.env.DB_PASSWORD || '',
    };

    // Tally URL from environment variables
    const tallyUrl = process.env.TALLY_URL || 'http://localhost:9000';

    // Initialize TallyLedgerSync with the dynamic DB and Tally URL
    const tallySync = new TallyLedgerSync(dbParams, tallyUrl);

    // Perform the synchronization
    await tallySync.syncLedgers();

    // Return success response
    return NextResponse.json({
      message: 'Ledger synchronization completed successfully',
    });
  } catch (error: unknown) {
    console.error('Error during ledger synchronization:', error);

    // Handle errors
    if (error instanceof Error) {
      return NextResponse.json(
        { message: 'Ledger synchronization failed', error: error.message },
        { status: 500 }
      );
    } else {
      return NextResponse.json(
        { message: 'Ledger synchronization failed', error: 'An unknown error occurred' },
        { status: 500 }
      );
    }
  }
}
