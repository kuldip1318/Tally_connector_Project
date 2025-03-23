import { NextResponse } from 'next/server';
import bcrypt from 'bcryptjs';
import pool from '@/lib/db';

export async function POST(req: Request) {
  let client;
  try {
    const { identifier, password } = await req.json();
    console.log('Login attempt for:', identifier);

    client = await pool.connect();
    console.log('Database connection established');

    const query = `
      SELECT id, full_name, email, mobile_number, password_hash
      FROM users_tb
      WHERE email = $1 OR mobile_number = $1
    `;
    const result = await client.query(query, [identifier]);
    console.log('Query executed, rows returned:', result.rows.length);

    if (result.rows.length === 0) {
      console.log('User not found');
      return NextResponse.json({ error: 'User not found' }, { status: 404 });
    }

    const user = result.rows[0];
    
    // Log the stored hash for debugging (remove in production)
    console.log('Stored password hash:', user.password_hash);
    
    // Ensure we're comparing with the correct field
    const isPasswordValid = await bcrypt.compare(password, user.password_hash);
    console.log('Password validation result:', isPasswordValid);

    if (!isPasswordValid) {
      console.log('Invalid credentials');
      return NextResponse.json({ error: 'Invalid credentials' }, { status: 401 });
    }

    console.log('Login successful for user:', user.id);
    return NextResponse.json({
      id: user.id,
      full_name: user.full_name,
      email: user.email,
      mobile_number: user.mobile_number
    });
  } catch (error: unknown) {
    console.error('Login error:', error);
    if (error instanceof Error) {
      return NextResponse.json({ 
        error: 'An error occurred during login', 
        details: error.message 
      }, { status: 500 });
    } else {
      return NextResponse.json({ 
        error: 'An unknown error occurred during login'
      }, { status: 500 });
    }
  } finally {
    if (client) {
      client.release();
      console.log('Database connection released');
    }
  }
}

