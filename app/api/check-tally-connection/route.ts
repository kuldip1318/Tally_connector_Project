import { NextResponse } from 'next/server';
import axios from 'axios';

export async function GET() {
  try {
    const response = await axios.get(process.env.TALLY_URL || 'http://localhost:9000');
    if (response.status === 200) {
      return NextResponse.json({ message: 'Tally connection successful' });
    } else {
      throw new Error('Tally connection failed');
    }
  } catch (error) {
    console.error('Error checking Tally connection:', error);
    return NextResponse.json({ error: 'Tally connection failed' }, { status: 500 });
  }
}

