import requests
import xml.etree.ElementTree as ET
import psycopg2
import pandas as pd
from io import StringIO
import sys
import logging
from typing import List, Dict
from dotenv import load_dotenv
import os
import xml.etree.ElementTree as ET
import requests
import time
from contextlib import contextmanager
import psycopg2.extras

load_dotenv()

class TallyDataSync:
    def __init__(self, session_data: Dict[str, str], tally_url: str = None):
        self.session_data = session_data
        user_id = session_data.get('userId')
        
        dynamic_db_name = f'user_{user_id}_db'

        self.db_params = {
            'dbname': dynamic_db_name,
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
            
        self.tally_url = tally_url or os.getenv('TALLY_URL')
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            filename='tally_integration.log',
            level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
            format='%(asctime)s %(levelname)s:%(message)s'
        )
        self.logger = logging.getLogger(__name__)
        # Add a stream handler to print logs to console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(console_handler)

    @contextmanager
    def db_cursor(self):
        """Context manager for database operations."""
        conn = None
        try:
            self.logger.debug(f"Connecting to database with params: {self.db_params}")
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_or_create_subscriber(self) -> int:
        try:
            user_id = self.session_data.get('userId')
            company_id = self.session_data.get('userCompanyId')
            tally_company = self.session_data.get('tallyCompanyId')

            with psycopg2.connect(**self.db_params) as conn:
                cur = conn.cursor()
                cur.execute("""
    INSERT INTO subscriber_db (user_id, company_id, tally_company)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id, company_id, tally_company) DO UPDATE
    SET created_at = CURRENT_TIMESTAMP
    RETURNING subscribe_id
""", (user_id, company_id, tally_company))
                subscribe_id = cur.fetchone()[0]
                conn.commit()
                self.logger.info(f"Subscriber ID obtained: {subscribe_id}")
                return subscribe_id
        except Exception as e:
            self.logger.error(f"Error getting or creating subscriber: {e}")
            raise

    def run_copy_insert(self, df: pd.DataFrame, table_name: str, columns: List[str]):
        """Generic COPY insert method for tables without unique constraints."""
        if df.empty:
            self.logger.info(f"No data to insert into {table_name}.")
            return

        try:
            with psycopg2.connect(**self.db_params) as conn:
                # Prepare the DataFrame for insertion
                buffer = StringIO()
                # Use a tab delimiter and a known null representation
                df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
                buffer.seek(0)

                # Use copy_from to insert data
                with conn.cursor() as cursor:
                    cursor.copy_from(buffer, table_name, null='\\N', columns=columns)
                conn.commit()

            self.logger.info(f"Data inserted into PostgreSQL table '{table_name}' successfully.")
        except Exception as e:
            self.logger.error(f"Error inserting data into {table_name}: {str(e)}")
            raise

    def get_tally_voucher_type(self, document_type):
            """Map document types to Tally voucher types."""
            document_type = document_type.lower() if document_type else ''
            
            voucher_map = {
                'invoice': 'Purchase',
                'bill': 'Purchase',
                'tax invoice': 'Purchase',
                'credit note': 'Credit Note',
                'debit note': 'Debit Note',
                'payment': 'Payment',
                'receipt': 'Receipt',
                'journal': 'Journal',
                'gst invoice': 'Purchase'
            }
            return voucher_map.get(document_type, 'Purchase')

    def fetch_and_convert_to_xml(self, transaction_ids, subscribe_id, tally_company):
        try:
            with self.db_cursor() as cursor:
                # First, get all ledger entries with their credit/debit status
                cursor.execute("""
                    SELECT 
                        l.transaction_id,
                        l.ledger_name,
                        l.ledger_amount,
                        l.amount_status,
                        t.document_number,
                        t.date,
                        t.narration,
                        COALESCE(b2b.name, b2c.name) as vendor_name,
                        b2b.GST_number as vendor_gst,
                        COALESCE(b2b.state, b2c.state) as vendor_state
                    FROM ledgerentries l
                    JOIN transactions t ON l.transaction_id = t.transaction_id
                    LEFT JOIN B2B_vendor b2b ON t.masterkeyids = b2b.masterkeyids
                    LEFT JOIN B2C_vendor b2c ON t.masterkeyids = b2c.masterkeyids
                    WHERE l.transaction_id = ANY(%s)
                    AND t.file_status = 'success'
                    ORDER BY l.transaction_id, l.entry_id
                """, (transaction_ids,))
                
                entries = cursor.fetchall()
                if not entries:
                    print("No ledger entries found")
                    return None

                # Group entries by transaction for processing
                entries_by_transaction = {}
                for entry in entries:
                    trans_id = entry['transaction_id']
                    if trans_id not in entries_by_transaction:
                        entries_by_transaction[trans_id] = []
                    entries_by_transaction[trans_id].append(entry)

                # Debug: Print transaction details
                print("\nTransaction Details:")
                for trans_id, trans_entries in entries_by_transaction.items():
                    print(f"\nTransaction {trans_id}:")
                    print(f"Document Number: {trans_entries[0]['document_number']}")
                    print("Ledger Entries:")
                    for entry in trans_entries:
                        print(f"  {entry['ledger_name']}: {entry['ledger_amount']} ({entry.get('amount_status', 'unknown')})")

                # Create XML
                root = ET.Element("ENVELOPE")
                header = ET.SubElement(root, "HEADER")
                ET.SubElement(header, "VERSION").text = "1"
                ET.SubElement(header, "TALLYREQUEST").text = "Import"
                ET.SubElement(header, "TYPE").text = "Data"
                ET.SubElement(header, "ID").text = "Vouchers"

                body = ET.SubElement(root, "BODY")
                desc = ET.SubElement(body, "DESC")
                static_vars = ET.SubElement(desc, "STATICVARIABLES")
                ET.SubElement(static_vars, "SVCURRENTCOMPANY").text = tally_company.strip('"').strip("'") if tally_company else ''

                data = ET.SubElement(body, "DATA")

                # Create ledgers first
                unique_ledgers = {entry['ledger_name'] for entry in entries}
                for ledger_name in unique_ledgers:
                    msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
                    ledger = ET.SubElement(msg, "LEDGER", NAME=ledger_name, Action="Create")
                    ET.SubElement(ledger, "NAME").text = ledger_name
                    if any(tax in ledger_name.upper() for tax in ['CGST', 'SGST', 'IGST']):
                        ET.SubElement(ledger, "PARENT").text = "Duties & Taxes"
                    else:
                        ET.SubElement(ledger, "PARENT").text = "Purchase Accounts"

                # Create vouchers
                for trans_id, trans_entries in entries_by_transaction.items():
                    print(f"\nProcessing voucher for transaction {trans_id}")
                    
                    # Get transaction details from first entry
                    first_entry = trans_entries[0]
                    
                    # Create vendor ledger
                    if first_entry['vendor_name']:
                        msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
                        vendor_ledger = ET.SubElement(msg, "LEDGER", NAME=first_entry['vendor_name'], Action="Create")
                        ET.SubElement(vendor_ledger, "NAME").text = first_entry['vendor_name']
                        ET.SubElement(vendor_ledger, "PARENT").text = "Sundry Creditors"
                        if first_entry['vendor_gst']:
                            ET.SubElement(vendor_ledger, "GSTREGISTRATIONTYPE").text = "Regular"
                            ET.SubElement(vendor_ledger, "PARTYGSTIN").text = first_entry['vendor_gst']
                            ET.SubElement(vendor_ledger, "STATENAME").text = first_entry['vendor_state']

                    # Create voucher
                    msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
                    voucher = ET.SubElement(msg, "VOUCHER", 
                                        VCHTYPE="Purchase", 
                                        ACTION="Create",
                                        OBJVIEW="Accounting Voucher View")

                    ET.SubElement(voucher, "DATE").text = first_entry['date'].strftime("%Y%m%d")
                    ET.SubElement(voucher, "NARRATION").text = first_entry['narration'] or "Purchase Entry"
                    ET.SubElement(voucher, "VOUCHERTYPENAME").text = "Purchase"
                    ET.SubElement(voucher, "VOUCHERNUMBER").text = first_entry['document_number']
                    ET.SubElement(voucher, "REFERENCE").text = first_entry['document_number']
                    ET.SubElement(voucher, "PARTYLEDGERNAME").text = first_entry['vendor_name']

                    if first_entry['vendor_gst']:
                        ET.SubElement(voucher, "PARTYGSTIN").text = first_entry['vendor_gst']

                    # Process ledger entries using amount_status from database
                    print(f"Processing {len(trans_entries)} ledger entries")
                    
                    # Add all ledger entries as per their amount_status
                    for entry in trans_entries:
                        amount = float(entry['ledger_amount'])
                        if amount == 0:
                            continue  # Skip zero amount entries
                            
                        ledger_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
                        ET.SubElement(ledger_entry, "LEDGERNAME").text = entry['ledger_name']
                        
                        # Determine if debit or credit based on amount_status
                        status = entry.get('amount_status', '').strip().lower()
                        if status == 'debit':
                            ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes"
                            ET.SubElement(ledger_entry, "AMOUNT").text = f"-{abs(amount):.2f}"
                            print(f"Added {entry['ledger_name']} as DEBIT: {amount:.2f}")
                        elif status == 'credit':
                            ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "No"
                            ET.SubElement(ledger_entry, "AMOUNT").text = f"{abs(amount):.2f}"
                            print(f"Added {entry['ledger_name']} as CREDIT: {amount:.2f}")
                        else:
                            # If status is missing, default based on amount sign
                            if amount < 0:
                                ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "No"
                                ET.SubElement(ledger_entry, "AMOUNT").text = f"{abs(amount):.2f}"
                                print(f"Added {entry['ledger_name']} as CREDIT (default): {amount:.2f}")
                            else:
                                ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes"
                                ET.SubElement(ledger_entry, "AMOUNT").text = f"-{abs(amount):.2f}"
                                print(f"Added {entry['ledger_name']} as DEBIT (default): {amount:.2f}")

                xml_str = ET.tostring(root, encoding="utf-8", method="xml", short_empty_elements=True).decode("utf-8")
                print("\nXML generation completed")
                return xml_str

        except Exception as e:
            self.logger.exception("Error generating XML")
            print(f"Error: {str(e)}")
            raise

    def send_data_to_tally(self, xml_data):
            """Send XML data to Tally."""
            try:
                headers = {"Content-Type": "text/xml;charset=utf-16"}
                
                # Log XML being sent
                self.logger.info("Sending XML data to Tally")
                self.logger.debug(f"XML Content: {xml_data}")

                for attempt in range(3):  # Retry mechanism
                    try:
                        response = requests.post(
                            self.tally_url, 
                            data=xml_data.encode('utf-16'), 
                            headers=headers,
                            timeout=30
                        )
                        response.raise_for_status()
                        self.logger.info("Successfully sent data to Tally")
                        return response.content
                    except requests.exceptions.RequestException as e:
                        self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                        if attempt < 2:  # Don't sleep on last attempt
                            time.sleep(2)  # Wait before retry
                        
                self.logger.error("Failed to send data to Tally after 3 attempts")
                raise Exception("Failed to send data to Tally")

            except Exception as e:
                self.logger.exception("Error sending data to Tally")
                raise

    def export_to_tally(self, transaction_ids):
        """Export specified transactions to Tally using session data."""
        try:
            self.logger.info(f"Starting export to Tally for {len(transaction_ids)} transactions")
            tally_company = self.session_data.get('tallyCompanyId')
            subscribe_id = self.session_data.get('subscribeId')
            
            self.logger.debug(f"Tally Company: {tally_company}, Subscribe ID: {subscribe_id}")
            
            # Generate XML with subscribe_id
            xml_data = self.fetch_and_convert_to_xml(transaction_ids, subscribe_id, tally_company)
            if not xml_data:
                self.logger.warning("No XML data generated")
                return None
            
            # Send to Tally
            response = self.send_data_to_tally(xml_data)
            
            self.logger.info(f"Successfully exported {len(transaction_ids)} transactions to Tally")
            return response
        except Exception as e:
            self.logger.exception("Error exporting to Tally")
            raise

def main():
    if len(sys.argv) < 2:
        print("Usage: python tally-data-sync.py key1=value1 key2=value2 ...")
        sys.exit(1)

    session_data = {}
    for arg in sys.argv[1:]:
        key, value = arg.split('=')
        session_data[key] = value

    required_keys = ['userId', 'userCompanyId', 'tallyCompanyId', 'subscribeId']
    if not all(key in session_data for key in required_keys):
        print(f"Error: Missing required session data. Required keys: {', '.join(required_keys)}")
        sys.exit(1)

    tally_url = "http://localhost:9000"
    sync = TallyDataSync(session_data, tally_url)

    sync.logger.info("Script started.")
    print("Script started.")

    try:
        # Find transactions to process
        with sync.db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    transaction_id,
                    document_number,
                    file_status,
                    push_status,
                    date
                FROM transactions 
                WHERE push_status = 1
                AND file_status = 'success'
                ORDER BY date DESC
                LIMIT 5
            """)
            
            transactions = cursor.fetchall()
            
            if not transactions:
                print("\nNo transactions found to export to Tally")
                sync.logger.info("No transactions found to export to Tally")
                return

            transaction_ids = [t['transaction_id'] for t in transactions]
            print(f"\nFound {len(transaction_ids)} transactions to export:")
            sync.logger.info(f"Found {len(transaction_ids)} transactions to export")
            for t in transactions:
                print(f"ID: {t['transaction_id']}, Doc: {t['document_number']}, "
                      f"Status: {t['file_status']}, Push Status: {t['push_status']}")
                sync.logger.debug(f"Transaction: ID: {t['transaction_id']}, Doc: {t['document_number']}, "
                                  f"Status: {t['file_status']}, Push Status: {t['push_status']}")

            # Export transactions to Tally
            try:
                response = sync.export_to_tally(transaction_ids)
                if response:
                    # Update push status on success
                    cursor.execute("""
                        UPDATE transactions 
                        SET push_status = 2, pushed_at = CURRENT_TIMESTAMP 
                        WHERE transaction_id = ANY(%s)
                    """, (transaction_ids,))

                    cursor.execute("""
                        UPDATE ledgerentries 
                        SET push_status = 2, pushed_at = CURRENT_TIMESTAMP 
                        WHERE transaction_id = ANY(%s)
                    """, (transaction_ids,))

                    print(f"\nSuccessfully exported and updated {len(transaction_ids)} transactions to push_status 3")
                    sync.logger.info(f"Successfully exported and updated {len(transaction_ids)} transactions to push_status 3")
            except Exception as e:
                sync.logger.error(f"Failed to export to Tally: {str(e)}")
                raise   

        print("\nScript completed successfully.")
        sync.logger.info("Script completed successfully.")
        
    except Exception as e:
        sync.logger.error(f"An unexpected error occurred: {e}")
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# import requests
# import xml.etree.ElementTree as ET
# import psycopg2
# import pandas as pd
# from io import StringIO
# import sys
# import logging
# from typing import List, Dict
# from dotenv import load_dotenv
# import os
# import time
# from contextlib import contextmanager
# import psycopg2.extras
# import re  # Added for GST ledger formatting

# load_dotenv()

# class TallyDataSync:
#     def __init__(self, session_data: Dict[str, str], tally_url: str = None):
#         self.session_data = session_data
#         user_id = session_data.get('userId')
#         dynamic_db_name = f'user_{user_id}_db'

#         self.db_params = {
#             'dbname': dynamic_db_name,
#             'user': os.getenv('DB_USER'),
#             'password': os.getenv('DB_PASSWORD'),
#             'host': os.getenv('DB_HOST'),
#             'port': os.getenv('DB_PORT')
#         }
            
#         self.tally_url = tally_url or os.getenv('TALLY_URL')
#         self.setup_logging()

#     def setup_logging(self):
#         logging.basicConfig(
#             filename='tally_integration.log',
#             level=logging.DEBUG,  # Detailed logging enabled
#             format='%(asctime)s %(levelname)s:%(message)s'
#         )
#         self.logger = logging.getLogger(__name__)
#         # Also log to console
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.DEBUG)
#         self.logger.addHandler(console_handler)

#     @contextmanager
#     def db_cursor(self):
#         """Context manager for database operations."""
#         conn = None
#         try:
#             self.logger.debug(f"Connecting to database with params: {self.db_params}")
#             conn = psycopg2.connect(**self.db_params)
#             cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#             yield cursor
#             conn.commit()
#         except Exception as e:
#             if conn:
#                 conn.rollback()
#             self.logger.error(f"Database error: {e}")
#             raise
#         finally:
#             if conn:
#                 conn.close()

#     def get_or_create_subscriber(self) -> int:
#         try:
#             user_id = self.session_data.get('userId')
#             company_id = self.session_data.get('userCompanyId')
#             tally_company = self.session_data.get('tallyCompanyId')

#             with psycopg2.connect(**self.db_params) as conn:
#                 cur = conn.cursor()
#                 cur.execute("""
#                     INSERT INTO subscriber_db (user_id, company_id, tally_company)
#                     VALUES (%s, %s, %s)
#                     ON CONFLICT (user_id, company_id, tally_company) DO UPDATE
#                     SET created_at = CURRENT_TIMESTAMP
#                     RETURNING subscribe_id
#                 """, (user_id, company_id, tally_company))
#                 subscribe_id = cur.fetchone()[0]
#                 conn.commit()
#                 self.logger.info(f"Subscriber ID obtained: {subscribe_id}")
#                 return subscribe_id
#         except Exception as e:
#             self.logger.error(f"Error getting or creating subscriber: {e}")
#             raise

#     def run_copy_insert(self, df: pd.DataFrame, table_name: str, columns: List[str]):
#         """Generic COPY insert method for tables without unique constraints."""
#         if df.empty:
#             self.logger.info(f"No data to insert into {table_name}.")
#             return

#         try:
#             with psycopg2.connect(**self.db_params) as conn:
#                 buffer = StringIO()
#                 df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
#                 buffer.seek(0)
#                 with conn.cursor() as cursor:
#                     cursor.copy_from(buffer, table_name, null='\\N', columns=columns)
#                 conn.commit()
#             self.logger.info(f"Data inserted into PostgreSQL table '{table_name}' successfully.")
#         except Exception as e:
#             self.logger.error(f"Error inserting data into {table_name}: {str(e)}")
#             raise

#     def get_tally_voucher_type(self, document_type):
#         """Map document types to Tally voucher types."""
#         document_type = document_type.lower() if document_type else ''
#         voucher_map = {
#             'invoice': 'Purchase',
#             'bill': 'Purchase',
#             'tax invoice': 'Purchase',
#             'credit note': 'Credit Note',
#             'debit note': 'Debit Note',
#             'payment': 'Payment',
#             'receipt': 'Receipt',
#             'journal': 'Journal',
#             'gst invoice': 'Purchase'
#         }
#         return voucher_map.get(document_type, 'Purchase')

#     def get_existing_ledgers(self) -> set:
#         query_xml = """
# <ENVELOPE>
#     <HEADER>
#         <VERSION>1</VERSION>
#         <TALLYREQUEST>Export</TALLYREQUEST>
#         <TYPE>Data</TYPE>
#         <ID>Ledger List</ID>
#     </HEADER>
#     <BODY>
#         <DESC>
#             <STATICVARIABLES>
#                 <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
#             </STATICVARIABLES>
#             <FETCHLIST>
#                 <FETCH>NAME</FETCH>
#             </FETCHLIST>
#             <REPORTNAME>Ledger</REPORTNAME>
#         </DESC>
#     </BODY>
# </ENVELOPE>
# """
#         try:
#             response = requests.post(
#                 self.tally_url, 
#                 data=query_xml.encode('utf-16'),
#                 headers={"Content-Type": "text/xml;charset=utf-16"},
#                 timeout=30
#             )
#             response.encoding = 'utf-8'
#             root = ET.fromstring(response.text)
#             ledger_names = set()
#             for ledger in root.findall(".//LEDGER"):
#                 name_elem = ledger.find("NAME")
#                 if name_elem is not None and name_elem.text:
#                     ledger_names.add(name_elem.text)
#             self.logger.debug(f"Existing ledgers from Tally: {ledger_names}")
#             return ledger_names
#         except Exception as e:
#             self.logger.error("Error fetching existing ledgers: " + str(e))
#             return set()

#     def get_existing_groups(self) -> set:
#         query_xml = """
# <ENVELOPE>
#     <HEADER>
#         <VERSION>1</VERSION>
#         <TALLYREQUEST>Export</TALLYREQUEST>
#         <TYPE>Data</TYPE>
#         <ID>Group List</ID>
#     </HEADER>
#     <BODY>
#         <DESC>
#             <STATICVARIABLES>
#                 <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
#             </STATICVARIABLES>
#             <FETCHLIST>
#                 <FETCH>NAME</FETCH>
#             </FETCHLIST>
#             <REPORTNAME>Group</REPORTNAME>
#         </DESC>
#     </BODY>
# </ENVELOPE>
# """
#         try:
#             response = requests.post(
#                 self.tally_url, 
#                 data=query_xml.encode('utf-16'),
#                 headers={"Content-Type": "text/xml;charset=utf-16"},
#                 timeout=30
#             )
#             response.encoding = 'utf-8'
#             root = ET.fromstring(response.text)
#             group_names = set()
#             for group in root.findall(".//GROUP"):
#                 name_elem = group.find("NAME")
#                 if name_elem is not None and name_elem.text:
#                     group_names.add(name_elem.text)
#             self.logger.debug(f"Existing groups from Tally: {group_names}")
#             return group_names
#         except Exception as e:
#             self.logger.error("Error fetching existing groups: " + str(e))
#             return set()

#     def get_ledger_group(self, ledger_name: str, is_vendor: bool = False) -> str:
#         """
#         Determine the parent group for a given ledger based on its name.
#         - Vendor ledgers are assigned to 'Sundry Creditors'
#         - Tax ledgers (e.g., with 'CGST', 'SGST', 'IGST', or 'tax') go to 'Duties & Taxes'
#         - Ledgers with 'discount' or 'round off' go to 'Indirect Expenses'
#         - Ledgers with 'direct' go to 'Direct Expenses'
#         - Ledgers with 'purchase' default to 'Purchase Accounts'
#         """
#         ledger_name_lower = ledger_name.lower()
#         if is_vendor:
#             return "Sundry Creditors"
#         if any(keyword in ledger_name_lower for keyword in ["cgst", "sgst", "igst", "tax"]):
#             return "Duties & Taxes"
#         if any(keyword in ledger_name_lower for keyword in ["discount", "round off"]):
#             return "Indirect Expenses"
#         if "direct expense" in ledger_name_lower or "direct" in ledger_name_lower:
#             return "Direct Expenses"
#         if "purchase" in ledger_name_lower:
#             return "Purchase Accounts"
#         return "Purchase Accounts"

#     def standardize_gst_ledger_name(self, ledger_name: str) -> str:
#         """
#         Standardize GST ledger names to follow the format:
#         'Input CGST 18%', 'Input SGST 18%', 'Input IGST 18%'
#         If the ledger already includes a percentage (e.g. '18%') or the 'Input' prefix,
#         it will ensure the prefix is present. Otherwise, it appends a default percentage of 18%.
#         """
#         ledger_name_upper = ledger_name.upper()
#         # Define a helper to add the 'Input' prefix if missing
#         def add_input_prefix(name: str) -> str:
#             return name if name.startswith("INPUT") else "Input " + name

#         if 'CGST' in ledger_name_upper:
#             if not re.search(r'\d+%', ledger_name_upper):
#                 return "Input CGST 18%"
#             else:
#                 return add_input_prefix(ledger_name_upper)
#         if 'SGST' in ledger_name_upper:
#             if not re.search(r'\d+%', ledger_name_upper):
#                 return "Input SGST 18%"
#             else:
#                 return add_input_prefix(ledger_name_upper)
#         if 'IGST' in ledger_name_upper:
#             if not re.search(r'\d+%', ledger_name_upper):
#                 return "Input IGST 18%"
#             else:
#                 return add_input_prefix(ledger_name_upper)
#         return ledger_name

#     def fetch_and_convert_to_xml(self, transaction_ids, subscribe_id, tally_company):
#         try:

#                         # First, fetch total_amount from transactions table for validation
#             cursor.execute("""
#                 SELECT 
#                     transaction_id,
#                     total_amount,
#                     document_number
#                 FROM transactions
#                 WHERE transaction_id = ANY(%s)
#                 AND file_status = 'success'
#             """, (transaction_ids,))
            
#             transaction_totals = {row['transaction_id']: float(row['total_amount']) 
#                                  for row in cursor.fetchall()}
            

#             if not transaction_totals:
#                 print("No transactions found")
#                 return None
            

#             with self.db_cursor() as cursor:


#                 cursor.execute("""
#                     SELECT 
#                         l.transaction_id,
#                         l.ledger_name,
#                         l.ledger_amount,
#                         l.amount_status,
#                         t.document_number,
#                         t.date,
#                         t.narration,
#                         COALESCE(b2b.name, b2c.name) as vendor_name,
#                         b2b.GST_number as vendor_gst,
#                         COALESCE(b2b.state, b2c.state) as vendor_state
#                     FROM ledgerentries l
#                     JOIN transactions t ON l.transaction_id = t.transaction_id
#                     LEFT JOIN B2B_vendor b2b ON t.masterkeyids = b2b.masterkeyids
#                     LEFT JOIN B2C_vendor b2c ON t.masterkeyids = b2c.masterkeyids
#                     WHERE l.transaction_id = ANY(%s)
#                     AND t.file_status = 'success'
#                     ORDER BY l.transaction_id, l.entry_id
#                 """, (transaction_ids,))
                
#                 entries = cursor.fetchall()
#                 if not entries:
#                     print("No ledger entries found")
#                     return None

#                 # Group entries by transaction and perform balancing check
#                 entries_by_transaction = {}
#                 for entry in entries:
#                     trans_id = entry['transaction_id']
#                     entries_by_transaction.setdefault(trans_id, []).append(entry)

#                 print("\nTransaction Balancing Check:")
#                 for trans_id, trans_entries in entries_by_transaction.items():
#                     total_amount = sum(float(entry['ledger_amount']) for entry in trans_entries)
#                     print(f"\nTransaction {trans_id}:")
#                     print(f"Document Number: {trans_entries[0]['document_number']}")
#                     print("Ledger Entries:")
#                     for entry in trans_entries:
#                         print(f"  {entry['ledger_name']}: {entry['ledger_amount']} ({entry.get('amount_status', 'unknown')})")
#                     print(f"Total Amount: {total_amount}")

#                 # Begin XML generation
#                 root = ET.Element("ENVELOPE")
#                 header = ET.SubElement(root, "HEADER")
#                 ET.SubElement(header, "VERSION").text = "1"
#                 ET.SubElement(header, "TALLYREQUEST").text = "Import"
#                 ET.SubElement(header, "TYPE").text = "Data"
#                 ET.SubElement(header, "ID").text = "Vouchers"

#                 body = ET.SubElement(root, "BODY")
#                 desc = ET.SubElement(body, "DESC")
#                 static_vars = ET.SubElement(desc, "STATICVARIABLES")
#                 ET.SubElement(static_vars, "SVCURRENTCOMPANY").text = tally_company.strip('"').strip("'") if tally_company else ''

#                 data = ET.SubElement(body, "DATA")

#                 # Ensure all required groups exist in Tally
#                 required_groups = {"Duties & Taxes", "Purchase Accounts", "Sundry Creditors", "Indirect Expenses", "Direct Expenses"}
#                 existing_groups = self.get_existing_groups()
#                 for group in required_groups:
#                     if group not in existing_groups:
#                         self.logger.info(f"Group '{group}' does not exist in Tally. Creating it.")
#                         msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
#                         group_elem = ET.SubElement(msg, "GROUP", NAME=group, Action="Create")
#                         ET.SubElement(group_elem, "NAME").text = group

#                 # Create non-vendor ledgers if not present in Tally
#                 existing_ledgers = self.get_existing_ledgers()
#                 unique_ledgers = {entry['ledger_name'] for entry in entries}
#                 for ledger_name in unique_ledgers:
#                     standardized_name = self.standardize_gst_ledger_name(ledger_name)
#                     if standardized_name in existing_ledgers:
#                         self.logger.info(f"Ledger '{standardized_name}' already exists in Tally, skipping creation.")
#                         continue
#                     msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
#                     ledger = ET.SubElement(msg, "LEDGER", NAME=standardized_name, Action="Create")
#                     ET.SubElement(ledger, "NAME").text = standardized_name
#                     parent_group = self.get_ledger_group(ledger_name, is_vendor=False)
#                     ET.SubElement(ledger, "PARENT").text = parent_group

#                 # Process transactions and create vouchers
#                 for trans_id, trans_entries in entries_by_transaction.items():
#                     print(f"\nProcessing voucher for transaction {trans_id}")
#                     first_entry = trans_entries[0]

#                     # Create vendor ledger if not already present and standardize GST names if needed
#                     if first_entry['vendor_name']:
#                         vendor_name = first_entry['vendor_name']
#                         standardized_vendor_name = vendor_name
#                         if standardized_vendor_name not in existing_ledgers:
#                             msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
#                             vendor_ledger = ET.SubElement(msg, "LEDGER", NAME=standardized_vendor_name, Action="Create")
#                             ET.SubElement(vendor_ledger, "NAME").text = standardized_vendor_name
#                             ET.SubElement(vendor_ledger, "PARENT").text = "Sundry Creditors"
#                             if first_entry['vendor_gst']:
#                                 ET.SubElement(vendor_ledger, "GSTREGISTRATIONTYPE").text = "Regular"
#                                 ET.SubElement(vendor_ledger, "PARTYGSTIN").text = first_entry['vendor_gst']
#                                 ET.SubElement(vendor_ledger, "STATENAME").text = first_entry['vendor_state']
#                         else:
#                             self.logger.info(f"Vendor ledger '{standardized_vendor_name}' already exists in Tally, skipping creation.")

#                     # Build voucher XML using standardized vendor ledger name if applicable
#                     msg = ET.SubElement(data, "TALLYMESSAGE", xmlns_UDF="TallyUDF")
#                     voucher = ET.SubElement(msg, "VOUCHER", 
#                                             VCHTYPE="Purchase", 
#                                             ACTION="Create",
#                                             OBJVIEW="Accounting Voucher View")

#                     ET.SubElement(voucher, "DATE").text = first_entry['date'].strftime("%Y%m%d")
#                     ET.SubElement(voucher, "NARRATION").text = first_entry['narration'] or "Purchase Entry"
#                     ET.SubElement(voucher, "VOUCHERTYPENAME").text = "Purchase"
#                     ET.SubElement(voucher, "VOUCHERNUMBER").text = first_entry['document_number']
#                     ET.SubElement(voucher, "REFERENCE").text = first_entry['document_number']
#                     ET.SubElement(voucher, "PARTYLEDGERNAME").text = first_entry['vendor_name'] if first_entry['vendor_name'] else ""

#                     if first_entry['vendor_gst']:
#                         ET.SubElement(voucher, "PARTYGSTIN").text = first_entry['vendor_gst']

#                     print(f"Processing {len(trans_entries)} ledger entries")
                    
#                     # Process entries with their proper credit/debit status
#                     for entry in trans_entries:
#                         amount = float(entry['ledger_amount'])
#                         if amount != 0:
#                             ledger_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
#                             ET.SubElement(ledger_entry, "LEDGERNAME").text = self.standardize_gst_ledger_name(entry['ledger_name'])
                            
#                             # Determine the proper ISDEEMEDPOSITIVE and AMOUNT values based on amount_status
#                             status = entry.get('amount_status', '').strip().lower()
#                             if status == 'debit':
#                                 ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes"
#                                 ET.SubElement(ledger_entry, "AMOUNT").text = f"-{abs(amount):.2f}"
#                             elif status == 'credit':
#                                 ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "No"
#                                 ET.SubElement(ledger_entry, "AMOUNT").text = f"{abs(amount):.2f}"
#                             else:
#                                 # Default to debit if status is missing or invalid
#                                 self.logger.warning(f"Invalid or missing amount_status: {entry.get('amount_status')}. Defaulting to debit.")
#                                 ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes"
#                                 ET.SubElement(ledger_entry, "AMOUNT").text = f"-{abs(amount):.2f}"

#                     # Check if debits and credits are balanced
#                     debit_sum = sum(float(entry['ledger_amount']) for entry in trans_entries 
#                                     if entry.get('amount_status', '').strip().lower() == 'debit')
#                     credit_sum = sum(float(entry['ledger_amount']) for entry in trans_entries 
#                                     if entry.get('amount_status', '').strip().lower() == 'credit')

#                     # If not balanced, log a warning and add balancing entry
#                     if abs(debit_sum - credit_sum) > 0.01:
#                         self.logger.warning(f"Transaction {trans_id} is not balanced: Debit sum={debit_sum}, Credit sum={credit_sum}")
                        
#                         # Add a balancing entry to the vendor
#                         if debit_sum > credit_sum:
#                             # Need to add a credit entry to balance
#                             balance_amount = debit_sum - credit_sum
#                             party_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
#                             ET.SubElement(party_entry, "LEDGERNAME").text = first_entry['vendor_name']
#                             ET.SubElement(party_entry, "ISDEEMEDPOSITIVE").text = "No"
#                             ET.SubElement(party_entry, "AMOUNT").text = f"{abs(balance_amount):.2f}"
#                             self.logger.info(f"Added balancing credit entry: {abs(balance_amount):.2f}")
#                             print(f"Added balancing credit entry: {abs(balance_amount):.2f}")
#                         elif credit_sum > debit_sum:
#                             # Need to add a debit entry to balance
#                             balance_amount = credit_sum - debit_sum
#                             party_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
#                             ET.SubElement(party_entry, "LEDGERNAME").text = first_entry['vendor_name']
#                             ET.SubElement(party_entry, "ISDEEMEDPOSITIVE").text = "Yes"
#                             ET.SubElement(party_entry, "AMOUNT").text = f"-{abs(balance_amount):.2f}"
#                             self.logger.info(f"Added balancing debit entry: {abs(balance_amount):.2f}")
#                             print(f"Added balancing debit entry: {abs(balance_amount):.2f}")
#                     else:
#                         self.logger.info(f"Transaction {trans_id} is balanced: Debit sum={debit_sum}, Credit sum={credit_sum}")
#                         print(f"Transaction {trans_id} is balanced: Debit sum={debit_sum}, Credit sum={credit_sum}")

#                 xml_str = ET.tostring(root, encoding="utf-8", method="xml", short_empty_elements=True).decode("utf-8")
#                 print("\nXML generation completed")
#                 print("Generated XML:")
#                 print(xml_str)
#                 return xml_str

#         except Exception as e:
#             self.logger.exception("Error generating XML")
#             print(f"Error: {str(e)}")
#             raise

#     def send_data_to_tally(self, xml_data):
#         """Send XML data to Tally with retry logic."""
#         try:
#             headers = {"Content-Type": "text/xml;charset=utf-16"}
#             self.logger.info("Sending XML data to Tally")
#             self.logger.debug(f"XML Content: {xml_data}")
#             for attempt in range(3):
#                 try:
#                     response = requests.post(
#                         self.tally_url, 
#                         data=xml_data.encode('utf-16'), 
#                         headers=headers,
#                         timeout=30
#                     )
#                     response.raise_for_status()
#                     self.logger.info("Successfully sent data to Tally")
#                     return response.content
#                 except requests.exceptions.RequestException as e:
#                     self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
#                     if attempt < 2:
#                         time.sleep(2)
#             self.logger.error("Failed to send data to Tally after 3 attempts")
#             raise Exception("Failed to send data to Tally")
#         except Exception as e:
#             self.logger.exception("Error sending data to Tally")
#             raise

#     def export_to_tally(self, transaction_ids):
#         """Export specified transactions to Tally using session data."""
#         try:
#             self.logger.info(f"Starting export to Tally for {len(transaction_ids)} transactions")
#             tally_company = self.session_data.get('tallyCompanyId')
#             subscribe_id = self.session_data.get('subscribeId')
#             self.logger.debug(f"Tally Company: {tally_company}, Subscribe ID: {subscribe_id}")
            
#             xml_data = self.fetch_and_convert_to_xml(transaction_ids, subscribe_id, tally_company)
#             if not xml_data:
#                 self.logger.warning("No XML data generated")
#                 return None
            
#             response = self.send_data_to_tally(xml_data)
#             self.logger.info(f"Successfully exported {len(transaction_ids)} transactions to Tally")
#             return response
#         except Exception as e:
#             self.logger.exception("Error exporting to Tally")
#             raise

# def main():
#     if len(sys.argv) < 2:
#         print("Usage: python tally-data-sync.py key1=value1 key2=value2 ...")
#         sys.exit(1)

#     session_data = {}
#     for arg in sys.argv[1:]:
#         key, value = arg.split('=')
#         session_data[key] = value

#     required_keys = ['userId', 'userCompanyId', 'tallyCompanyId', 'subscribeId']
#     if not all(key in session_data for key in required_keys):
#         print(f"Error: Missing required session data. Required keys: {', '.join(required_keys)}")
#         sys.exit(1)

#     tally_url = "http://localhost:9000"
#     sync = TallyDataSync(session_data, tally_url)

#     sync.logger.info("Script started.")
#     print("Script started.")

#     try:
#         with sync.db_cursor() as cursor:
#             cursor.execute("""
#                 SELECT 
#                     transaction_id,
#                     document_number,
#                     file_status,
#                     push_status,
#                     date
#                 FROM transactions 
#                 WHERE push_status = 1
#                 AND file_status = 'success'
#                 ORDER BY date DESC
#                 LIMIT 5
#             """)
            
#             transactions = cursor.fetchall()
            
#             if not transactions:
#                 print("\nNo transactions found to export to Tally")
#                 sync.logger.info("No transactions found to export to Tally")
#                 return

#             transaction_ids = [t['transaction_id'] for t in transactions]
#             print(f"\nFound {len(transaction_ids)} transactions to export:")
#             sync.logger.info(f"Found {len(transaction_ids)} transactions to export")
#             for t in transactions:
#                 print(f"ID: {t['transaction_id']}, Doc: {t['document_number']}, "
#                       f"Status: {t['file_status']}, Push Status: {t['push_status']}")
#                 sync.logger.debug(f"Transaction: ID: {t['transaction_id']}, Doc: {t['document_number']}, "
#                                   f"Status: {t['file_status']}, Push Status: {t['push_status']}")

#             try:
#                 response = sync.export_to_tally(transaction_ids)
#                 if response:
#                     cursor.execute("""
#                         UPDATE transactions 
#                         SET push_status = 2, pushed_at = CURRENT_TIMESTAMP 
#                         WHERE transaction_id = ANY(%s)
#                     """, (transaction_ids,))
#                     cursor.execute("""
#                         UPDATE ledgerentries 
#                         SET push_status = 2, pushed_at = CURRENT_TIMESTAMP 
#                         WHERE transaction_id = ANY(%s)
#                     """, (transaction_ids,))
#                     print(f"\nSuccessfully exported and updated {len(transaction_ids)} transactions to push_status 3")
#                     sync.logger.info(f"Successfully exported and updated {len(transaction_ids)} transactions to push_status 3")
#             except Exception as e:
#                 sync.logger.error(f"Failed to export to Tally: {str(e)}")
#                 raise   

#         print("\nScript completed successfully.")
#         sync.logger.info("Script completed successfully.")
        
#     except Exception as e:
#         sync.logger.error(f"An unexpected error occurred: {e}")
#         print(f"An unexpected error occurred: {e}")
#         sys.exit(1)

# if __name__ == "__main__":
#     main()



