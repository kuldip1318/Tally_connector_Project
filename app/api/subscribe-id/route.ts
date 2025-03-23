import { NextResponse } from "next/server"
import { Pool } from "pg"

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url)
  const userId = searchParams.get("userId")
  const userCompanyId = searchParams.get("userCompanyId")
  const tallyCompany = searchParams.get("tallyCompany")

  if (!userId || !userCompanyId || !tallyCompany) {
    return NextResponse.json({ error: "Missing required parameters" }, { status: 400 })
  }

  const dbUrl = `postgres://postgres:admin@98.85.154.231:5432/user_${userId}_db`
  const pool = new Pool({
    connectionString: dbUrl,
    ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
  })

  try {
    const client = await pool.connect()
    try {
      const query = `
        SELECT subscribe_id
        FROM subscriber_db
        WHERE user_id = $1 AND company_id = $2 AND tally_company = $3
      `
      const result = await client.query(query, [userId, userCompanyId, tallyCompany])

      if (result.rows.length === 0) {
        return NextResponse.json({ error: "No subscription found for the given parameters" }, { status: 404 })
      }

      return NextResponse.json({ subscribeId: result.rows[0].subscribe_id })
    } finally {
      client.release()
    }
  } catch (error) {
    console.error("Error fetching subscribe_id:", error)
    return NextResponse.json({ error: "An error occurred while fetching subscribe_id" }, { status: 500 })
  } finally {
    await pool.end()
  }
}

