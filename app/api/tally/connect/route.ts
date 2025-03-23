import { NextResponse } from 'next/server'
import axios from 'axios'
import { parseStringPromise } from 'xml2js'
import pool from '@/lib/db'

export async function GET() {
  try {
    const xmlRequest = `
      <ENVELOPE>
        <HEADER>
          <VERSION>1</VERSION>
          <TALLYREQUEST>Export</TALLYREQUEST>
          <TYPE>Collection</TYPE>
          <ID>ListOfCompanies</ID>
        </HEADER>
        <BODY>
          <DESC>
            <STATICVARIABLES>
              <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
            </STATICVARIABLES>
            <TDL>
              <TDLMESSAGE>
                <COLLECTION NAME="ListOfCompanies">
                  <TYPE>Company</TYPE>
                  <NATIVEMETHOD>Name</NATIVEMETHOD>
                  <NATIVEMETHOD>CompanyNumber</NATIVEMETHOD>
                </COLLECTION>
              </TDLMESSAGE>
            </TDL>
          </DESC>
        </BODY>
      </ENVELOPE>
    `

    const response = await axios.post('http://localhost:9000', xmlRequest, {
      headers: { 'Content-Type': 'application/xml' },
    })

    const result = await parseStringPromise(response.data)
    console.log('Parsed XML result:', JSON.stringify(result, null, 2))

    if (!result.ENVELOPE || !result.ENVELOPE.BODY || !result.ENVELOPE.BODY[0] || !result.ENVELOPE.BODY[0].DATA || !result.ENVELOPE.BODY[0].DATA[0] || !result.ENVELOPE.BODY[0].DATA[0].COLLECTION || !result.ENVELOPE.BODY[0].DATA[0].COLLECTION[0] || !result.ENVELOPE.BODY[0].DATA[0].COLLECTION[0].COMPANY) {
      throw new Error('Unexpected XML structure')
    }

    const companies = result.ENVELOPE.BODY[0].DATA[0].COLLECTION[0].COMPANY.map((company: any) => ({
      id: company.COMPANYNUMBER[0]._.trim(),
      name: company.NAME[0]._
    }))

    // Store companies in the database
    await pool.query('BEGIN')
    for (const company of companies) {
      await pool.query(
        'INSERT INTO companies (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = $2',
        [company.id, company.name]
      )
    }
    await pool.query('COMMIT')

    console.log('Companies:', companies)
    return NextResponse.json({ companies })
  } catch (error: unknown) {
    console.error('Error connecting to Tally:', error)
    await pool.query('ROLLBACK')
    if (error instanceof Error) {
      return NextResponse.json({ message: 'Failed to connect to Tally', error: error.message }, { status: 500 })
    } else {
      return NextResponse.json({ message: 'Failed to connect to Tally', error: 'An unknown error occurred' }, { status: 500 })
    }
  }
}

