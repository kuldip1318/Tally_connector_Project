import { NextResponse } from 'next/server'
import bcrypt from 'bcryptjs'
import pool from '../../../../lib/db'

export async function POST(req: Request) {
  try {
    const { email, password } = await req.json()
    const hashedPassword = await bcrypt.hash(password, 10)
    
    const result = await pool.query(
      'INSERT INTO users_tb (email, password) VALUES ($1, $2) RETURNING id',
      [email, hashedPassword]
    )
    
    return NextResponse.json({ message: 'User created successfully', userId: result.rows[0].id }, { status: 201 })
  } catch (error) {
    console.error('Signup error:', error)
    return NextResponse.json({ message: 'An error occurred during signup' }, { status: 500 })
  }
}

