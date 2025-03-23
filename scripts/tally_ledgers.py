import requests
import xml.etree.ElementTree as ET
import psycopg2
from decimal import Decimal
from typing import Dict, Any, Set
import re
import os
from dotenv import load_dotenv

load_dotenv()

class TallyIntegration:
    def __init__(self, session_data: Dict[str, str]):
        self.session_data = session_data
        self.db_params = {
            'dbname': f"user_{session_data['userId']}_db",
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        self.tally_url = os.getenv('TALLY_URL')
        self.existing_entries: Set[tuple] = set() # Updated: changed to Set[tuple]
        self.xml_request = self.construct_xml_request()

    def construct_xml_request(self):
        company = self.session_data['tallyCompanyId'].strip().strip('"').strip("'")
        return f"""
        <ENVELOPE>
            <HEADER>
                <VERSION>1</VERSION>
                <TALLYREQUEST>EXPORT</TALLYREQUEST>
                <TYPE>COLLECTION</TYPE>
                <ID>Ledger Details</ID>
            </HEADER>
            <BODY>
                <DESC>
                    <STATICVARIABLES>
                        <SVEXPORTFORMAT>XML (Data Interchange)</SVEXPORTFORMAT>
                        <SVCURRENTCOMPANY>{company}</SVCURRENTCOMPANY>
                    </STATICVARIABLES>
                    <TDL>
                        <TDLMESSAGE>
                            <COLLECTION NAME="Ledger Details" ISINITIALIZE="Yes">
                                <TYPE>LEDGER</TYPE>
                                <NATIVEMETHOD>MASTER</NATIVEMETHOD>
                                <FETCH>NAME,PARENT,ADDRESS,LEDSTATENAME,PINCODE,LEDGERMOBILE,
                                OPENINGBALANCE,CLOSINGBALANCE,BILLBYBILL,ISBILLWISEON,CREDITDAYS,
                                COUNTRYOFRESIDENCE,GSTREGISTRATIONTYPE,PARTYGSTIN,BANKDETAILS,
                                IFSCODE,BANKNAME,ACCOUNTNUMBER,INCOMETAXNUMBER,REGISTRATIONTYPE,
                                VATTINNUMBER,INTERSTATESTNUMBER</FETCH>
                            </COLLECTION>
                        </TDLMESSAGE>
                    </TDL>
                </DESC>
            </BODY>
        </ENVELOPE>
        """

    def ensure_table_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tally_ledgers (
            id SERIAL PRIMARY KEY,
            subscribe_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            parent TEXT,
            address TEXT,
            led_state_name TEXT,
            pincode VARCHAR(10),
            ledger_mobile VARCHAR(20),
            opening_balance NUMERIC(15,2),
            closing_balance NUMERIC(15,2),
            bill_by_bill TEXT,
            is_bill_wise_on TEXT,
            credit_days INTEGER,
            country_of_residence TEXT,
            gst_registration_type TEXT,
            party_gstin VARCHAR(20),
            bank_details TEXT,
            ifsc_code VARCHAR(15),
            bank_name TEXT,
            account_number VARCHAR(30),
            income_tax_number VARCHAR(20),
            registration_type TEXT,
            vattin_number VARCHAR(20),
            interstate_st_number VARCHAR(20),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_tally_ledgers_name_subscribe_id ON tally_ledgers(name, subscribe_id);

        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';

        DROP TRIGGER IF EXISTS update_tally_ledgers_updated_at ON tally_ledgers;
        CREATE TRIGGER update_tally_ledgers_updated_at
            BEFORE UPDATE ON tally_ledgers
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)

    def load_existing_entries(self):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, subscribe_id FROM tally_ledgers")
                self.existing_entries = {(row[0], row[1]) for row in cur.fetchall()}

    def clean_xml_content(self, content: bytes) -> str:
        # First, try to decode as UTF-8, fallback to Latin-1
        try:
            xml_str = content.decode('utf-8')
        except UnicodeDecodeError:
            xml_str = content.decode('latin-1')

        # Remove invalid XML characters
        xml_str = re.sub(r'&#x[0-9a-fA-F]+;', '', xml_str)
        xml_str = re.sub(r'&#\d+;', '', xml_str)
        
        # Fix common XML entities
        xml_str = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&', xml_str)
        
        # Replace invalid characters with space
        xml_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', ' ', xml_str)
        
        return xml_str.strip()

    def fetch_tally_data(self) -> ET.Element:
        try:
            print(f"Sending request to Tally server: {self.tally_url}")
            print(f"XML Request:\n{self.xml_request}")
            response = requests.post(
                self.tally_url,
                data=self.xml_request,
                headers={'Content-Type': 'text/xml'},
                timeout=30
            )
            response.raise_for_status()
            cleaned_xml = self.clean_xml_content(response.content)
            return ET.fromstring(cleaned_xml)
        except ET.ParseError as e:
            print(f"XML Parsing error: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Failed to connect to Tally: {str(e)}")
            raise

    def parse_amount(self, amount_str: str) -> Decimal:
        if not amount_str:
            return Decimal('0.00')
        try:
            # Remove any non-numeric characters except decimal point and minus sign
            cleaned = ''.join(c for c in str(amount_str) if c.isdigit() or c in '.-')
            # Handle special cases
            if cleaned in ['', '.', '-', '-.']:
                return Decimal('0.00')
            return Decimal(cleaned).quantize(Decimal('0.01'))
        except:
            return Decimal('0.00')

    def parse_ledger(self, ledger: ET.Element) -> Dict[str, Any]:
        def get_text(element: ET.Element, tag: str) -> str:
            elem = element.find(tag)
            return str(elem.text).strip() if elem is not None and elem.text else ''

        def get_address(element: ET.Element) -> str:
            addr_list = element.find('ADDRESS.LIST')
            if addr_list is not None:
                return ', '.join(
                    addr.text.strip() 
                    for addr in addr_list.findall('ADDRESS') 
                    if addr is not None and addr.text
                )
            return ''

        name = str(ledger.get('NAME', '')).strip()
        if not name:  # Skip ledgers without names
            return {}

        return {
            'subscribe_id': int(self.session_data['subscribeId']),
            'name': name,
            'parent': get_text(ledger, 'PARENT'),
            'address': get_address(ledger),
            'led_state_name': get_text(ledger, 'LEDSTATENAME'),
            'pincode': get_text(ledger, 'PINCODE')[:10],
            'ledger_mobile': get_text(ledger, 'LEDGERMOBILE')[:20],
            'opening_balance': self.parse_amount(get_text(ledger, 'OPENINGBALANCE')),
            'closing_balance': self.parse_amount(get_text(ledger, 'CLOSINGBALANCE')),
            'bill_by_bill': get_text(ledger, 'BILLBYBILL'),
            'is_bill_wise_on': get_text(ledger, 'ISBILLWISEON'),
            'credit_days': int(get_text(ledger, 'CREDITDAYS') or '0'),
            'country_of_residence': get_text(ledger, 'COUNTRYOFRESIDENCE'),
            'gst_registration_type': get_text(ledger, 'GSTREGISTRATIONTYPE'),
            'party_gstin': get_text(ledger, 'PARTYGSTIN')[:20],
            'bank_details': get_text(ledger, 'BANKDETAILS'),
            'ifsc_code': get_text(ledger, 'IFSCODE')[:15],
            'bank_name': get_text(ledger, 'BANKNAME'),
            'account_number': get_text(ledger, 'ACCOUNTNUMBER')[:30],
            'income_tax_number': get_text(ledger, 'INCOMETAXNUMBER')[:20],
            'registration_type': get_text(ledger, 'REGISTRATIONTYPE'),
            'vattin_number': get_text(ledger, 'VATTINNUMBER')[:20],
            'interstate_st_number': get_text(ledger, 'INTERSTATESTNUMBER')[:20]
        }

    def upsert_data(self, ledger_data: Dict[str, Any]) -> bool:
        if not ledger_data or not ledger_data.get('name'):
            return False

        params = (
            ledger_data['parent'], ledger_data['address'],
            ledger_data['led_state_name'], ledger_data['pincode'],
            ledger_data['ledger_mobile'], ledger_data['opening_balance'],
            ledger_data['closing_balance'], ledger_data['bill_by_bill'],
            ledger_data['is_bill_wise_on'], ledger_data['credit_days'],
            ledger_data['country_of_residence'], ledger_data['gst_registration_type'],
            ledger_data['party_gstin'], ledger_data['bank_details'],
            ledger_data['ifsc_code'], ledger_data['bank_name'],
            ledger_data['account_number'], ledger_data['income_tax_number'],
            ledger_data['registration_type'], ledger_data['vattin_number'],
            ledger_data['interstate_st_number'], ledger_data['name'],
            ledger_data['subscribe_id']
        )

        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                if (ledger_data['name'], ledger_data['subscribe_id']) in self.existing_entries:
                    cur.execute("""
                        UPDATE tally_ledgers SET
                            parent = %s, address = %s, led_state_name = %s, pincode = %s,
                            ledger_mobile = %s, opening_balance = %s, closing_balance = %s,
                            bill_by_bill = %s, is_bill_wise_on = %s, credit_days = %s,
                            country_of_residence = %s, gst_registration_type = %s,
                            party_gstin = %s, bank_details = %s, ifsc_code = %s,
                            bank_name = %s, account_number = %s, income_tax_number = %s,
                            registration_type = %s, vattin_number = %s, interstate_st_number = %s
                        WHERE name = %s AND subscribe_id = %s
                    """, params)
                    return False
                else:
                    cur.execute("""
                        INSERT INTO tally_ledgers (
                            parent, address, led_state_name, pincode, ledger_mobile,
                            opening_balance, closing_balance, bill_by_bill, is_bill_wise_on,
                            credit_days, country_of_residence, gst_registration_type,
                            party_gstin, bank_details, ifsc_code, bank_name, account_number,
                            income_tax_number, registration_type, vattin_number,
                            interstate_st_number, name, subscribe_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, params)
                    self.existing_entries.add((ledger_data['name'], ledger_data['subscribe_id']))
                    return True

def main(session_data):
    print("Starting Tally integration...")
    tally = TallyIntegration(session_data)
    
    print("Setting up database...")
    tally.ensure_table_exists()
    tally.load_existing_entries()
    
    print("Fetching data from Tally...")
    root = tally.fetch_tally_data()
    
    print("Processing ledgers...")
    new_count = update_count = 0
    
    for ledger in root.findall('.//LEDGER'):
        ledger_data = tally.parse_ledger(ledger)
        if ledger_data:  # Skip empty ledger data
            if tally.upsert_data(ledger_data):
                new_count += 1
            else:
                update_count += 1
    
    print(f"\nSync completed successfully!")
    print(f"New entries: {new_count}")
    print(f"Updated entries: {update_count}")
    print(f"Total processed: {new_count + update_count}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python tally_ledgers.py key1=value1 key2=value2 ...")
        sys.exit(1)
    session_data = {}
    for arg in sys.argv[1:]:
        key, value = arg.split('=')
        session_data[key] = value
    main(session_data)

