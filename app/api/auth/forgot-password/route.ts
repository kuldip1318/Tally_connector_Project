import { NextResponse } from 'next/server'
import pool from '../../../../lib/db'
import crypto from 'crypto'

export async function POST(req: Request) {
  try {
    const { email } = await req.json()
    const resetToken = crypto.randomBytes(20).toString('hex')
    const resetTokenExpiry = new Date(Date.now() + 3600000) // 1 hour from now
    
    const result = await pool.query(
      'UPDATE users SET reset_token = $1, reset_token_expiry = $2 WHERE email = $3 RETURNING id',
      [resetToken, resetTokenExpiry, email]
    )
    
    if (result.rowCount === 0) {
      // Don't reveal if the email exists or not
      return NextResponse.json({ message: 'If an account exists, a reset link has been sent.' }, { status: 200 })
    }
    
    // TODO: Send email with reset link
    // In a real application, you would integrate with an email service here
    // For now, we'll just log the reset token
    console.log(`Reset token for ${email}: ${resetToken}`)
    
    return NextResponse.json({ message: 'If an account exists, a reset link has been sent.' }, { status: 200 })
  } catch (error) {
    console.error('Forgot password error:', error)
    return NextResponse.json({ message: 'An error occurred' }, { status: 500 })
  }
}

