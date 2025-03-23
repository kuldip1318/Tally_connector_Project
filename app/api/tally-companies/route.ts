import { NextResponse } from 'next/server';
import axios from 'axios';
import { parseStringPromise } from 'xml2js';

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
          </DESC>
        </BODY>
      </ENVELOPE>
    `;

    const response = await axios.post(process.env.TALLY_URL || 'http://localhost:9000', xmlRequest, {
      headers: { 'Content-Type': 'application/xml' },
    });

    const result = await parseStringPromise(response.data);
    const companies = result.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY?.map((company: any, index: number) => ({
      name: company.NAME?.[0]?._ || company.NAME?.[0] || `Unknown Company ${index + 1}`,
      guid: company.GUID?.[0]?._ || company.GUID?.[0] || `unknown-guid-${index + 1}`,
    })) || [];

    return NextResponse.json(companies);
  } catch (error) {
    console.error('Error fetching companies from Tally:', error);
    if (axios.isAxiosError(error)) {
      console.error('Axios error:', error.response?.data);
    }
    return NextResponse.json({ error: 'Failed to fetch companies from Tally' }, { status: 500 });
  }
}

