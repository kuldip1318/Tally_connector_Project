# import os
# import sys
# import logging
# import datetime
# import pandas as pd
# import psycopg2
# import requests
# import xml.etree.ElementTree as ET
# import uuid
# from typing import Dict, List, Tuple, Generator
# from tqdm import tqdm
# from contextlib import contextmanager
# from psycopg2 import sql
# from psycopg2.extras import execute_values, DictCursor
# from dotenv import load_dotenv
# import time

# load_dotenv()

# class TallyIntegration:
#     def __init__(self, session_data: Dict[str, str]):
#         self.session_data = session_data
#         self.db_params = {
#             'dbname': f"user_{session_data['userId']}_db",
#             'user': os.getenv('DB_USER'),
#             'password': os.getenv('DB_PASSWORD'),
#             'host': os.getenv('DB_HOST'),
#             'port': os.getenv('DB_PORT')
#         }
#         self.tally_url = os.getenv('TALLY_URL')

#         # Register UUID adapter for psycopg2
#         import psycopg2.extensions
#         psycopg2.extensions.register_adapter(uuid.UUID, lambda u: psycopg2.extensions.AsIs(f"'{u}'"))
#         self.setup_logging()


    

        
#     def setup_logging(self):
#         logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#         self.logger = logging.getLogger(__name__)

#         self.tally_data_config = {
#             "from_date": datetime.datetime.strptime(self.session_data['start_date'], '%Y-%m-%d').date(),
#             "to_date": datetime.datetime.strptime(self.session_data['end_date'], '%Y-%m-%d').date(),
#             "company_name": self.session_data['tallyCompanyId'],
#             "table_name": "transactions"  # Changed from tally_data to transactions
#         }

#     @contextmanager
#     def db_cursor(self):
#         """Context manager for database operations."""
#         conn = None
#         try:
#             conn = psycopg2.connect(**self.db_params)
#             cursor = conn.cursor(cursor_factory=DictCursor)
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

#     def get_next_transaction_id(self) -> str:
#         """Get the next transaction ID (TRN00001, TRN00002, etc.)"""
#         try:
#             with self.db_cursor() as cursor:
#                 cursor.execute("SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT 1")
#                 result = cursor.fetchone()
                
#                 if result is None or not result[0]:
#                     # No existing transactions, start with TRN00001
#                     return "TRN00001"
                
#                 last_id = result[0]
#                 # Extract the numeric part
#                 numeric_part = int(last_id[3:])
#                 # Increment and format
#                 next_numeric = numeric_part + 1
#                 return f"TRN{next_numeric:05d}"
#         except Exception as e:
#             self.logger.error(f"Error getting next transaction ID: {str(e)}")
#             # Fallback to a default if needed
#             return "TRN00001"

#     def construct_tally_data_payload(self, from_date, to_date, company):
#         """Constructs XML payload for Tally request with GUID."""

#         company = company.strip().strip('"').strip("'") if company else ""

#         payload_xml = f"""<?xml version="1.0" encoding="utf-8"?>
#     <ENVELOPE>
#         <HEADER>
#             <VERSION>1</VERSION>
#             <TALLYREQUEST>Export</TALLYREQUEST>
#             <TYPE>Data</TYPE>
#             <ID>MyReportAccountingVoucherTable</ID>
#         </HEADER>
#         <BODY>
#             <DESC>
#                 <STATICVARIABLES>
#                     <SVEXPORTFORMAT>XML (Data Interchange)</SVEXPORTFORMAT>
#                     <SVFROMDATE>{from_date.strftime('%d-%b-%Y')}</SVFROMDATE>
#                     <SVTODATE>{to_date.strftime('%d-%b-%Y')}</SVTODATE>
#                     {"<SVCURRENTCOMPANY>" + company + "</SVCURRENTCOMPANY>" if company else ""}
#                     <SVEXPORTFORBALANCINGLEDGERS>Yes</SVEXPORTFORBALANCINGLEDGERS>
#                     <SVEXPORTFORMAT>XML (Data Interchange)</SVEXPORTFORMAT>
#                     <SVINVENTORYENTRIES>All Items</SVINVENTORYENTRIES>
#                     <SVINVENTORYVALUES>Yes</SVINVENTORYVALUES>
#                     <SVINVOICEDETAILS>Yes</SVINVOICEDETAILS>
#                     <SVSHOWBILLALLOCATIONS>Yes</SVSHOWBILLALLOCATIONS>
#                 </STATICVARIABLES>
#                 <TDL>
#                     <TDLMESSAGE>
#                         <REPORT NAME="MyReportAccountingVoucherTable">
#                             <FORMS>MyForm</FORMS>
#                         </REPORT>
#                         <FORM NAME="MyForm">
#                             <PARTS>MyPart01</PARTS>
#                             <XMLTAG>DATA</XMLTAG>
#                         </FORM>
#                         <PART NAME="MyPart01">
#                             <LINES>MyLine01</LINES>
#                             <REPEAT>MyLine01 : MyCollection</REPEAT>
#                             <SCROLLED>Vertical</SCROLLED>
#                         </PART>
#                         <PART NAME="MyPart02">
#                             <LINES>MyLine02</LINES>
#                             <REPEAT>MyLine02 : AllLedgerEntries</REPEAT>
#                             <SCROLLED>Vertical</SCROLLED>
#                         </PART>
#                         <LINE NAME="MyLine01">
#                             <FIELDS>FldGUID,FldDate,FldVoucherType,FldVoucherNumber,FldPartyName,FldVoucherCategory,FldNarration</FIELDS>
#                             <EXPLODE>MyPart02</EXPLODE>
#                             <XMLTAG>VOUCHER</XMLTAG>
#                         </LINE>
#                         <LINE NAME="MyLine02">
#                             <FIELDS>FldLedgerName,FldLedgerAmount,FldLedgerMasterId,FldLedgerType,FldIsDrCr</FIELDS>
#                             <XMLTAG>ACCOUNTING_ALLOCATION</XMLTAG>
#                         </LINE>
#                         <FIELD NAME="FldGUID">
#                             <SET>$GUID</SET>
#                             <XMLTAG>GUID</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldDate">
#                             <SET>$Date</SET>
#                             <XMLTAG>DATE</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldVoucherType">
#                             <SET>$VoucherTypeName</SET>
#                             <XMLTAG>VOUCHERTYPE</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldVoucherNumber">
#                             <SET>
#                                 if not $$IsEmpty:$Reference then $Reference else $VoucherNumber
#                             </SET>
#                             <XMLTAG>VOUCHERNUMBER</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldPartyName">
#                             <SET>if $$IsEmpty:$PartyLedgerName then $$StrByCharCode:245 else $PartyLedgerName</SET>
#                             <XMLTAG>PARTYNAME</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldVoucherCategory">
#                             <SET>$Parent:VoucherType:$VoucherTypeName</SET>
#                             <XMLTAG>VOUCHERCATEGORY</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldNarration">
#                             <SET>if $$IsEmpty:$Narration then $$StrByCharCode:245 else $Narration</SET>
#                             <XMLTAG>Narration</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldLedgerName">
#                             <SET>$LedgerName</SET>
#                             <XMLTAG>LEDGER</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldLedgerAmount">
#                             <SET>$$StringFindAndReplace:(if $$IsDebit:$Amount then -$$NumValue:$Amount else $$NumValue:$Amount):"(-)":"-"</SET>
#                             <XMLTAG>AMOUNT</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldLedgerMasterId">
#                             <SET>$MasterId:Ledger:$LedgerName</SET>
#                             <XMLTAG>LEDGER_MASTERID</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldLedgerType">
#                             <SET>$Parent:Ledger:$LedgerName</SET>
#                             <XMLTAG>LEDGER_GROUP</XMLTAG>
#                         </FIELD>
#                         <FIELD NAME="FldIsDrCr">
#                             <SET>if $$IsDebit:$Amount then "Dr" else "Cr"</SET>
#                             <XMLTAG>DRCR</XMLTAG>
#                         </FIELD>
#                         <COLLECTION NAME="MyCollection">
#                             <TYPE>Voucher</TYPE>
#                             <FETCH>AllLedgerEntries</FETCH>
#                             <FETCH>Narration</FETCH>
#                             <FETCH>PartyLedgerName</FETCH>
#                             <FETCH>GUID</FETCH>
#                             <FETCH>Reference</FETCH>
#                             <FETCH>AlterID</FETCH>
#                             <FETCH>MasterID</FETCH>
#                             <FILTER>Fltr01,Fltr02</FILTER>
#                         </COLLECTION>
#                         <SYSTEM TYPE="Formulae" NAME="Fltr01">NOT $IsCancelled</SYSTEM>
#                         <SYSTEM TYPE="Formulae" NAME="Fltr02">NOT $IsOptional</SYSTEM>
#                     </TDLMESSAGE>
#                 </TDL>
#             </DESC>
#         </BODY>
#     </ENVELOPE>
#         """
#         self.logger.info(f"Constructed payload with company: '{company}'")
#         self.logger.info(f"Payload XML: {payload_xml}...")
#         return payload_xml

#     def fetch_tally_data(self, payload):
#         """Fetch data from Tally server."""
#         max_retries = 3
#         retry_delay = 5  # seconds

#         for attempt in range(max_retries):
#             try:
#                 self.logger.info(f"Sending request to Tally server: {self.tally_url}")
#                 response = requests.post(self.tally_url, data=payload, timeout=60)  # Increased timeout
                
#                 self.logger.info(f"Received response from Tally. Status code: {response.status_code}")
#                 response.raise_for_status()
                
#                 if not response.text:
#                     raise ValueError("Received empty response from Tally server")
                    
#                 return response.text
                
#             except requests.exceptions.Timeout:
#                 self.logger.warning(f"Timeout while connecting to Tally server (Attempt {attempt + 1}/{max_retries})")
#                 if attempt < max_retries - 1:
#                     time.sleep(retry_delay)
#                 else:
#                     raise
#             except requests.exceptions.ConnectionError:
#                 self.logger.warning(f"Could not connect to Tally server at {self.tally_url} (Attempt {attempt + 1}/{max_retries})")
#                 if attempt < max_retries - 1:
#                     time.sleep(retry_delay)
#                 else:
#                     raise
#             except requests.exceptions.RequestException as e:
#                 self.logger.error(f"Error fetching data from Tally: {str(e)}")
#                 raise
#             except Exception as e:
#                 self.logger.error(f"Unexpected error while fetching Tally data: {str(e)}")
#                 raise

#     def parse_tally_data_xml(self, xml_data, subscribe_id) -> Generator[List[Dict], None, None]:
#         """Parse Tally XML response for transactions table."""
#         try:
#             root = ET.fromstring(xml_data)

#             self.logger.info(f"XML Response (first 500 chars): {xml_data[:500]}...")
#         except ET.ParseError as e:
#             self.logger.error(f"transactions: Error parsing XML: {e}")
#             sys.exit(1)

#         # Get the first transaction ID to use for this batch
#         next_transaction_id = self.get_next_transaction_id()
#         transaction_counter = int(next_transaction_id[3:])
        
#         processed_vouchers = {}  # To track unique vouchers by their number
#         data = []
        
#             # Log the total number of vouchers found
#         vouchers = root.findall(".//VOUCHER")
#         self.logger.info(f"Total vouchers found in XML: {len(vouchers)}")
#         for voucher in root.findall(".//VOUCHER"):
#             guid_text = voucher.findtext("GUID")
#             date_text = voucher.findtext("DATE")
#             voucher_type = voucher.findtext("VOUCHERTYPE") or ""
#             voucher_number = voucher.findtext("VOUCHERNUMBER") or ""
#             party_name = voucher.findtext("PARTYNAME") or ""
#             # Use correct case for narration tag - Note capital 'N' in XML
#             narration = voucher.findtext("Narration") or ""
            
#             # Debug logging to check what's coming from Tally
#             self.logger.info(f"Processing Voucher - Type: {voucher_type}, Number: {voucher_number}, GUID: {guid_text}")

#             self.logger.info(f"GUID from XML: {guid_text}, Narration: {narration}")
            
#             # Additional search for GUID if it's missing
#             if not guid_text:
#                 # Try looking with different cases or in attributes
#                 for elem in voucher:
#                     if elem.tag.upper() == "GUID" and elem.text:
#                         guid_text = elem.text
#                         self.logger.info(f"Found GUID using alternative method: {guid_text}")
#                         break
            
#             # Convert date from Tally format (e.g., "1-Apr-23") to database format
#             try:
#                 date_obj = datetime.datetime.strptime(date_text.strip(), '%d-%b-%y').date()
#             except (ValueError, AttributeError):
#                 self.logger.warning(f"Invalid date format: {date_text}")
#                 continue


#             voucher_xml = ET.tostring(voucher, encoding='unicode')
#             self.logger.info(f"Voucher XML: {voucher_xml[:200]}...")     
#             # Skip if we've already processed this voucher
#             voucher_key = f"{date_obj}_{voucher_type}_{voucher_number}"
#             if voucher_key in processed_vouchers:
#                 continue
                
#             # Calculate total amount for this voucher
#             total_amount = 0.0
#             allocations = voucher.findall(".//ACCOUNTING_ALLOCATION")
#             for allocation in allocations:
#                 amount_text = allocation.findtext("AMOUNT") or "0"
#                 amount_text = amount_text.replace("(-)", "-")
#                 try:
#                     amount = float(amount_text)
#                     # We're only interested in positive amounts for the total
#                     total_amount += abs(amount)
#                 except ValueError:
#                     pass
            
#             # More robust GUID handling
#             guid_uuid = None
#             if guid_text:
#                 # Clean up the GUID text
#                 guid_text = guid_text.strip()
#                 self.logger.info(f"Attempting to convert GUID: {guid_text}")
                
#                 try:
#                     # Try direct conversion
#                     guid_uuid = uuid.UUID(guid_text)
#                 except (ValueError, TypeError):
#                     try:
#                         # If the GUID is in a non-standard format, try to clean it
#                         # Remove any non-hex characters
#                         clean_guid = ''.join(c for c in guid_text if c.isalnum())
                        
#                         # If it's too long, truncate; if too short, pad with zeros
#                         if len(clean_guid) > 32:
#                             clean_guid = clean_guid[:32]
#                         elif len(clean_guid) < 32:
#                             clean_guid = clean_guid.ljust(32, '0')
                            
#                         # Try to create a UUID from the cleaned string
#                         if len(clean_guid) == 32:
#                             formatted_guid = f"{clean_guid[:8]}-{clean_guid[8:12]}-{clean_guid[12:16]}-{clean_guid[16:20]}-{clean_guid[20:]}"
#                             guid_uuid = uuid.UUID(formatted_guid)
#                             self.logger.info(f"Converted non-standard GUID to: {guid_uuid}")
#                     except (ValueError, TypeError):
#                         self.logger.warning(f"Could not convert GUID: {guid_text}")
#                         # Generate a new UUID if conversion fails
#                         guid_uuid = uuid.uuid4()
#                         self.logger.info(f"Generated new UUID: {guid_uuid}")
#             else:
#                 # If GUID is missing, generate a new one
#                 guid_uuid = uuid.uuid4()
#                 self.logger.info(f"No GUID found, generated: {guid_uuid}")
                
#             # Generate the transaction ID
#             transaction_id = f"TRN{transaction_counter:05d}"
#             transaction_counter += 1
            
#             row = {
#                 "transaction_id": transaction_id,
#                 "batch_id": None,  # As specified, this should be NULL
#                 "transaction_batch_id": None,  # As specified, this should be NULL
#                 "GUID": guid_uuid,
#                 "subscribe_id": subscribe_id,
#                 "file_status": None,  # As specified, this should be NULL
#                 "original_filename": None,  # As specified, this should be NULL
#                 "masterkeyids": None,  # As specified, this should be NULL
#                 "date": date_obj,
#                 "document_type": voucher_type,  # document_type == voucher_type
#                 "document_number": voucher_number,  # document_number == voucher_number
#                 "narration": narration,
#                 "party_name": party_name,
#                 "total_amount": total_amount,
#                 "user_id": self.session_data.get('userId', None),
#                 "filepath": None,  # As specified, this should be NULL
#                 "push_status": 0,
#                 "pushed_at": None,
#                 "created_by": self.session_data.get('userId', None),
#                 "updated_by": self.session_data.get('userId', None)
#             }
            
#             data.append(row)
#             processed_vouchers[voucher_key] = True
            
#             # Process data in chunks
#             if len(data) >= 1000:
#                 yield data
#                 data = []

#         # Yield any remaining data
#         if data:
#             yield data

#         self.logger.info(f"transactions: Parsed all records.")

#     def insert_transactions_into_postgres(self, data: List[Dict], table_name: str):
#         """Insert data into transactions table."""
#         if not data:
#             self.logger.info("No data to insert.")
#             return

#         # Log basic info about the data being processed
#         self.logger.info(f"Processing {len(data)} records for insertion/update")
        
#         # Debug first record
#         if data:
#             self.logger.info(f"Sample record before insertion: {data[0]}")
            
#             if 'GUID' in data[0]:
#                 self.logger.info(f"GUID value: {data[0]['GUID']}, Type: {type(data[0]['GUID'])}")
#             if 'narration' in data[0]:
#                 self.logger.info(f"Narration value: {data[0]['narration']}")
#             if 'document_number' in data[0]:
#                 self.logger.info(f"Document number (voucher number) value: {data[0]['document_number']}")

#         try:
#             with self.db_cursor() as cursor:
#                 for idx, record in enumerate(data):
#                     # Log each record being processed
#                     self.logger.info(f"Processing record {idx+1}/{len(data)}: document_number={record['document_number']}, document_type={record['document_type']}")
                    
#                     # Convert UUID to string to avoid adaptation issues
#                     if 'GUID' in record and isinstance(record['GUID'], uuid.UUID):
#                         guid_str = str(record['GUID'])
#                     else:
#                         guid_str = None
                    
#                     # Check if document_number already exists in transactions table
#                     check_query = f"""
#                         SELECT transaction_id FROM {table_name} 
#                         WHERE document_number = %s
#                     """
#                     self.logger.info(f"Checking if document_number '{record['document_number']}' exists in database")
#                     cursor.execute(check_query, (record['document_number'],))
#                     existing = cursor.fetchone()
                    
#                     if existing:
#                         # If document_number exists, update only GUID and subscribe_id
#                         self.logger.info(f"FOUND: Document number '{record['document_number']}' already exists with transaction_id {existing[0]}, updating GUID and subscribe_id")
#                         update_query = f"""
#                             UPDATE {table_name}
#                             SET GUID = %s, 
#                                 subscribe_id = %s,
#                                 updated_at = CURRENT_TIMESTAMP
#                             WHERE document_number = %s
#                         """
#                         cursor.execute(update_query, (guid_str, record['subscribe_id'], record['document_number']))
#                         self.logger.info(f"Updated record with document_number '{record['document_number']}'")
#                     else:
#                         # If document_number doesn't exist, insert new record
#                         self.logger.info(f"NOT FOUND: Document number '{record['document_number']}' not found in database, inserting new record")
                        
#                         # Convert UUID to string for all records to avoid adaptation issues
#                         insert_record = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in record.items()}
                        
#                         columns = ', '.join(insert_record.keys())
#                         placeholders = ', '.join(['%s'] * len(insert_record))
#                         insert_query = f"""
#                             INSERT INTO {table_name} ({columns})
#                             VALUES ({placeholders})
#                         """
#                         cursor.execute(insert_query, tuple(insert_record.values()))
#                         self.logger.info(f"Inserted new record with document_number '{record['document_number']}'")
                
#                 self.logger.info(f"Data chunk processed successfully.")
            
#         except Exception as e:
#             self.logger.error(f"Error processing data: {str(e)}")
#             self.logger.error(f"Error detail: {e}")
#             raise

#     ############# TRANSACTION DETAILS INSERTION #############  
#     def sync_transaction_details(self, subscribe_id: str):
#         """Sync transaction details data with the updated XML format"""
#         start_date = self.tally_data_config['from_date']
#         end_date = self.tally_data_config['to_date']
        
#         try:
#             self.logger.info(f"Starting transaction details sync from {start_date} to {end_date}")
            
#             # Construct and send payload with enhanced XML
#             payload = self.construct_tally_data_payload(start_date, end_date, self.tally_data_config["company_name"])
            
#             # Fetch data from Tally
#             xml_data = self.fetch_tally_data(payload)
            
#             # Verify and update transaction details
#             success_count, failure_count = self.verify_and_update_transaction_details(xml_data, subscribe_id)
            
#             self.logger.info(f"Transaction details sync completed. Success: {success_count}, Failures: {failure_count}")
            
#             if failure_count > 0:
#                 self.logger.warning("There were verification failures. Check logs for details.")
            
#             return success_count, failure_count
            
#         except Exception as e:
#             self.logger.error(f"Error in transaction details sync: {str(e)}")
#             return 0, 0  

#     def get_transaction_details_by_guid(self, guid_str):

#         try:
#             with self.db_cursor() as cursor:
#                 cursor.execute("""
#                     SELECT entry_id, ledger_name, ledger_amount 
#                     FROM transaction_details 
#                     WHERE GUID = %s
#                 """, (guid_str,))
#                 return cursor.fetchall()
#         except Exception as e:
#             self.logger.error(f"Error getting transaction details by GUID: {str(e)}")
#             return []

#     def parse_ledger_entries_from_voucher(self, voucher, guid_uuid, subscribe_id):
#         """Parse ledger entries from a voucher element and return structured data"""
#         ledger_entries = []
#         allocations = voucher.findall(".//ACCOUNTING_ALLOCATION")
        
#         for allocation in allocations:
#             ledger_name = allocation.findtext("LEDGER") or ""
#             amount_text = allocation.findtext("AMOUNT") or "0"
            
#             # Get Dr/Cr status from the XML
#             amount_status = allocation.findtext("DRCR") or ""
#             if not amount_status:
#                 # If DRCR is not available, determine from amount sign
#                 try:
#                     amount = float(amount_text.replace("(-)", "-"))
#                     amount_status = "Dr" if amount < 0 else "Cr"
#                 except ValueError:
#                     self.logger.warning(f"Invalid amount format for ledger {ledger_name}: {amount_text}")
#                     amount_status = "Unknown"
            
#             # Normalize the amount (store as absolute value)
#             try:
#                 ledger_amount = abs(float(amount_text.replace("(-)", "-")))
#             except ValueError:
#                 self.logger.warning(f"Invalid amount format for ledger {ledger_name}: {amount_text}")
#                 ledger_amount = 0.0
            
#             ledger_entries.append({
#                 "ledger_name": ledger_name,
#                 "ledger_amount": ledger_amount,
#                 "amount_status": amount_status,
#                 "GUID": guid_uuid,
#                 "subscribe_id": subscribe_id
#             })
        
#         return ledger_entries

#     def verify_and_update_transaction_details(self, xml_data, subscribe_id):
#         """
#         Parse Tally XML, find matching transaction details by GUID, 
#         verify ledger entries and update specified columns
#         """
#         root = ET.fromstring(xml_data)
#         success_count = 0
#         failure_count = 0
        
#         for voucher in root.findall(".//VOUCHER"):
#             guid_text = voucher.findtext("GUID") or ""
#             if not guid_text:
#                 self.logger.warning("Voucher without GUID found, skipping.")
#                 continue
            
#             # Convert GUID for comparison
#             try:
#                 guid_uuid = uuid.UUID(guid_text.strip())
#                 guid_str = str(guid_uuid)
#             except (ValueError, TypeError):
#                 self.logger.warning(f"Invalid GUID format: {guid_text}")
#                 continue
            
#             # Get ledger entries from API response
#             api_ledger_entries = self.parse_ledger_entries_from_voucher(voucher, guid_uuid, subscribe_id)
#             if not api_ledger_entries:
#                 self.logger.warning(f"No ledger entries found for voucher with GUID {guid_str}")
#                 continue
            
#             # Get existing entries from database
#             db_entries = self.get_transaction_details_by_guid(guid_str)
#             if not db_entries:
#                 self.logger.warning(f"No transaction_details found with GUID {guid_str}")
#                 continue
            
#             # Map existing DB entries by ledger name for comparison
#             db_entries_map = {row['ledger_name']: {
#                 'entry_id': row['entry_id'], 
#                 'amount': float(row['ledger_amount'])
#             } for row in db_entries}
            
#             # Map API entries by ledger name for comparison
#             api_entries_map = {entry['ledger_name']: entry for entry in api_ledger_entries}
            
#             # Verify all DB ledgers exist in API response
#             missing_in_api = set(db_entries_map.keys()) - set(api_entries_map.keys())
#             if missing_in_api:
#                 self.logger.error(f"Ledgers in DB not found in API response: {missing_in_api}")
#                 failure_count += 1
#                 continue
            
#             # Verify all API ledgers exist in DB
#             missing_in_db = set(api_entries_map.keys()) - set(db_entries_map.keys())
#             if missing_in_db:
#                 self.logger.warning(f"New ledgers in API not in DB: {missing_in_db}")
            
#             # Verify amounts match (with small tolerance for rounding errors)
#             amount_mismatches = []
#             for ledger_name in set(db_entries_map.keys()) & set(api_entries_map.keys()):
#                 db_amount = db_entries_map[ledger_name]['amount']
#                 api_amount = float(api_entries_map[ledger_name]['ledger_amount'])
                
#                 if abs(db_amount - api_amount) > 0.01:  # Small tolerance
#                     amount_mismatches.append(f"{ledger_name}: DB={db_amount}, API={api_amount}")
            
#             if amount_mismatches:
#                 self.logger.error(f"Amount mismatches for GUID {guid_str}: {amount_mismatches}")
#                 failure_count += 1
#                 continue
            
#             # If verification passes, update the transaction_details
#             try:
#                 with self.db_cursor() as cursor:
#                     for ledger_name, entry in db_entries_map.items():
#                         if ledger_name in api_entries_map:
#                             api_entry = api_entries_map[ledger_name]
#                             entry_id = entry['entry_id']
                            
#                             cursor.execute("""
#                                 UPDATE transaction_details
#                                 SET GUID = %s,
#                                     subscribe_id = %s,
#                                     amount_status = %s,
#                                     updated_at = CURRENT_TIMESTAMP,
#                                     updated_by = %s
#                                 WHERE entry_id = %s
#                             """, (
#                                 guid_str,
#                                 subscribe_id,
#                                 api_entry['amount_status'],
#                                 self.session_data.get('userId'),
#                                 entry_id
#                             ))
#                 success_count += 1
#                 self.logger.info(f"Updated transaction_details for GUID {guid_str}")
#             except Exception as e:
#                 self.logger.error(f"Error updating transaction_details: {str(e)}")
#                 failure_count += 1
        
#         return success_count, failure_count

#     def sync_data_by_year(self, start_date: datetime.date, end_date: datetime.date, overall_pbar, subscribe_id: str) -> bool:
#         """Sync data for a specific year range with progress tracking."""
#         try:
#             year_pbar = tqdm(
#                 total=100,
#                 desc=f"Processing {start_date.year}-{end_date.year}",
#                 leave=True,
#                 position=1
#             )
            
#             self.logger.info(f"Processing data from {start_date} to {end_date}")
            
#             # Construct and send payload
#             year_pbar.set_description(f"Constructing payload")
#             payload = self.construct_tally_data_payload(start_date, end_date, self.tally_data_config["company_name"])
#             year_pbar.update(20)
            
#             # Fetch data
#             year_pbar.set_description(f"Fetching data")
#             xml_data = self.fetch_tally_data(payload)
#             year_pbar.update(30)
            
#             # Parse XML and insert data in chunks
#             year_pbar.set_description(f"Parsing and inserting data")
#             total_records = 0
#             for chunk in self.parse_tally_data_xml(xml_data, subscribe_id):
#                 self.insert_transactions_into_postgres(chunk, self.tally_data_config["table_name"])
#                 total_records += len(chunk)
#                 year_pbar.update(50 * (len(chunk) / 100000))  # Assuming max 100,000 records per year
            
#             self.logger.info(f"Total records processed: {total_records}")
            
#             year_pbar.close()
#             overall_pbar.update(1)
#             return True
            
#         except Exception as e:
#             self.logger.error(f"Error processing data: {str(e)}")
#             return False

#     def sync_tally_data(self, subscribe_id: str):
#         """Sync all tally data with progress tracking."""
#         start_date = self.tally_data_config['from_date']
#         end_date = self.tally_data_config['to_date']
        
#         # Calculate number of years between dates
#         years_diff = end_date.year - start_date.year
#         if end_date.month < start_date.month or (end_date.month == start_date.month and end_date.day < start_date.day):
#             years_diff -= 1
        
#         total_chunks = max(1, years_diff + 1)  # At least 1 chunk even if within same year
        
#         overall_pbar = tqdm(
#             total=total_chunks,
#             desc="Overall Progress",
#             leave=True,
#             position=0
#         )
        
#         success_chunks = []
#         failed_chunks = []
        
#         print(f"\nProcessing data in {total_chunks} {'chunk' if total_chunks == 1 else 'chunks'}...")
        
#         if total_chunks == 1:
#             # If dates are within one year or less, process all at once
#             if self.sync_data_by_year(start_date, end_date, overall_pbar, subscribe_id):
#                 success_chunks.append(f"{start_date} to {end_date}")
#             else:
#                 failed_chunks.append(f"{start_date} to {end_date}")
#         else:
#             # Process year by year
#             current_date = start_date
#             while current_date < end_date:
#                 year_end = min(
#                     datetime.date(current_date.year + 1, current_date.month, current_date.day) - datetime.timedelta(days=1),
#                     end_date
#                 )
                
#                 if self.sync_data_by_year(current_date, year_end, overall_pbar, subscribe_id):
#                     success_chunks.append(f"{current_date} to {year_end}")
#                 else:
#                     failed_chunks.append(f"{current_date} to {year_end}")
                
#                 current_date = year_end + datetime.timedelta(days=1)
        
#         overall_pbar.close()
        
#         # Print summary
#         print("\nSync Summary:")
#         print(f"Total Chunks Processed: {total_chunks}")
#         print(f"Successfully Processed: {len(success_chunks)}")
#         print(f"Failed: {len(failed_chunks)}")
        
#         if failed_chunks:
#             print("\nFailed Chunks:")
#             for chunk in failed_chunks:
#                 print(f"- {chunk}")
        
#         self.logger.info("Tally data sync completed")
#         return success_chunks, failed_chunks

# def main():
#     # Check command line arguments
#     if len(sys.argv) < 2:
#         print("Usage: python tally_data.py key1=value1 key2=value2 ...")
#         sys.exit(1)
#     session_data = {}
#     for arg in sys.argv[1:]:
#         key, value = arg.split('=')
#         session_data[key] = value

#     required_keys = ['userId', 'userCompanyId', 'tallyCompanyId', 'subscribeId', 'start_date', 'end_date']
#     if not all(key in session_data for key in required_keys):
#         print(f"Error: Missing required session data. Required keys: {', '.join(required_keys)}")
#         sys.exit(1)

#     tally = TallyIntegration(session_data)
#     subscribe_id = session_data['subscribeId']
    
#     # First sync transactions (your existing code)
#     success_months, failed_months = tally.sync_tally_data(subscribe_id)
#     print(f"Transactions sync completed. Success months: {success_months}, Failed months: {failed_months}")
    
#     # Now sync transaction details (new code)
#     success_count, failure_count = tally.sync_transaction_details(subscribe_id)
#     print(f"Transaction details sync completed. Success: {success_count}, Failures: {failure_count}")

# if __name__ == "__main__":
#     main()

import os
import sys
import logging
import datetime
import pandas as pd
import psycopg2
import requests
import xml.etree.ElementTree as ET
import uuid
from typing import Dict, List, Tuple, Generator
from tqdm import tqdm
from contextlib import contextmanager
from psycopg2 import sql
from psycopg2.extras import execute_values, DictCursor
from dotenv import load_dotenv
import time

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

        # Register UUID adapter for psycopg2
        import psycopg2.extensions
        psycopg2.extensions.register_adapter(uuid.UUID, lambda u: psycopg2.extensions.AsIs(f"'{u}'"))
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.tally_data_config = {
            "from_date": datetime.datetime.strptime(self.session_data['start_date'], '%Y-%m-%d').date(),
            "to_date": datetime.datetime.strptime(self.session_data['end_date'], '%Y-%m-%d').date(),
            "company_name": self.session_data['tallyCompanyId'],
            "table_name": "transactions"  # Changed from tally_data to transactions
        }

    @contextmanager
    def db_cursor(self):
        """Context manager for database operations."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor(cursor_factory=DictCursor)
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

    def get_next_transaction_id(self) -> str:
        """Get the next transaction ID (TRN00001, TRN00002, etc.)"""
        try:
            with self.db_cursor() as cursor:
                cursor.execute("SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT 1")
                result = cursor.fetchone()
                if result is None or not result[0]:
                    return "TRN00001"
                last_id = result[0]
                numeric_part = int(last_id[3:])
                next_numeric = numeric_part + 1
                return f"TRN{next_numeric:05d}"
        except Exception as e:
            self.logger.error(f"Error getting next transaction ID: {str(e)}")
            return "TRN00001"

    def convert_guid(self, guid_text: str) -> uuid.UUID:
        """
        Convert the given GUID text into a valid UUID.
        First, attempt a direct conversion. If that fails,
        use uuid.uuid5 with a fixed namespace for a deterministic result.
        """
        try:
            # Attempt to convert directly
            return uuid.UUID(guid_text)
        except (ValueError, TypeError):
            new_uuid = uuid.uuid5(uuid.NAMESPACE_OID, guid_text)
            self.logger.info(f"Converted non-standard GUID using uuid5 to: {new_uuid}")
            return new_uuid

    def construct_tally_data_payload(self, from_date, to_date, company):
        """Constructs XML payload for Tally request with GUID."""
        company = company.strip().strip('"').strip("'") if company else ""
        payload_xml = f"""<?xml version="1.0" encoding="utf-8"?>
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>MyReportAccountingVoucherTable</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>XML (Data Interchange)</SVEXPORTFORMAT>
                <SVFROMDATE>{from_date.strftime('%d-%b-%Y')}</SVFROMDATE>
                <SVTODATE>{to_date.strftime('%d-%b-%Y')}</SVTODATE>
                {"<SVCURRENTCOMPANY>" + company + "</SVCURRENTCOMPANY>" if company else ""}
                <SVEXPORTFORBALANCINGLEDGERS>Yes</SVEXPORTFORBALANCINGLEDGERS>
                <SVEXPORTFORMAT>XML (Data Interchange)</SVEXPORTFORMAT>
                <SVINVENTORYENTRIES>All Items</SVINVENTORYENTRIES>
                <SVINVENTORYVALUES>Yes</SVINVENTORYVALUES>
                <SVINVOICEDETAILS>Yes</SVINVOICEDETAILS>
                <SVSHOWBILLALLOCATIONS>Yes</SVSHOWBILLALLOCATIONS>
            </STATICVARIABLES>
            <TDL>
                <TDLMESSAGE>
                    <REPORT NAME="MyReportAccountingVoucherTable">
                        <FORMS>MyForm</FORMS>
                    </REPORT>
                    <FORM NAME="MyForm">
                        <PARTS>MyPart01</PARTS>
                        <XMLTAG>DATA</XMLTAG>
                    </FORM>
                    <PART NAME="MyPart01">
                        <LINES>MyLine01</LINES>
                        <REPEAT>MyLine01 : MyCollection</REPEAT>
                        <SCROLLED>Vertical</SCROLLED>
                    </PART>
                    <PART NAME="MyPart02">
                        <LINES>MyLine02</LINES>
                        <REPEAT>MyLine02 : AllLedgerEntries</REPEAT>
                        <SCROLLED>Vertical</SCROLLED>
                    </PART>
                    <LINE NAME="MyLine01">
                        <FIELDS>FldGUID,FldDate,FldVoucherType,FldVoucherNumber,FldPartyName,FldVoucherCategory,FldNarration</FIELDS>
                        <EXPLODE>MyPart02</EXPLODE>
                        <XMLTAG>VOUCHER</XMLTAG>
                    </LINE>
                    <LINE NAME="MyLine02">
                        <FIELDS>FldLedgerName,FldLedgerAmount,FldLedgerMasterId,FldLedgerType,FldIsDrCr</FIELDS>
                        <XMLTAG>ACCOUNTING_ALLOCATION</XMLTAG>
                    </LINE>
                    <FIELD NAME="FldGUID">
                        <SET>$GUID</SET>
                        <XMLTAG>GUID</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldDate">
                        <SET>$Date</SET>
                        <XMLTAG>DATE</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldVoucherType">
                        <SET>$VoucherTypeName</SET>
                        <XMLTAG>VOUCHERTYPE</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldVoucherNumber">
                        <SET>
                            if not $$IsEmpty:$Reference then $Reference else $VoucherNumber
                        </SET>
                        <XMLTAG>VOUCHERNUMBER</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldPartyName">
                        <SET>if $$IsEmpty:$PartyLedgerName then $$StrByCharCode:245 else $PartyLedgerName</SET>
                        <XMLTAG>PARTYNAME</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldVoucherCategory">
                        <SET>$Parent:VoucherType:$VoucherTypeName</SET>
                        <XMLTAG>VOUCHERCATEGORY</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldNarration">
                        <SET>if $$IsEmpty:$Narration then $$StrByCharCode:245 else $Narration</SET>
                        <XMLTAG>Narration</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldLedgerName">
                        <SET>$LedgerName</SET>
                        <XMLTAG>LEDGER</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldLedgerAmount">
                        <SET>$$StringFindAndReplace:(if $$IsDebit:$Amount then -$$NumValue:$Amount else $$NumValue:$Amount):"(-)":"-"</SET>
                        <XMLTAG>AMOUNT</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldLedgerMasterId">
                        <SET>$MasterId:Ledger:$LedgerName</SET>
                        <XMLTAG>LEDGER_MASTERID</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldLedgerType">
                        <SET>$Parent:Ledger:$LedgerName</SET>
                        <XMLTAG>LEDGER_GROUP</XMLTAG>
                    </FIELD>
                    <FIELD NAME="FldIsDrCr">
                        <SET>if $$IsDebit:$Amount then "Dr" else "Cr"</SET>
                        <XMLTAG>DRCR</XMLTAG>
                    </FIELD>
                    <COLLECTION NAME="MyCollection">
                        <TYPE>Voucher</TYPE>
                        <FETCH>AllLedgerEntries</FETCH>
                        <FETCH>Narration</FETCH>
                        <FETCH>PartyLedgerName</FETCH>
                        <FETCH>GUID</FETCH>
                        <FETCH>Reference</FETCH>
                        <FETCH>AlterID</FETCH>
                        <FETCH>MasterID</FETCH>
                        <FILTER>Fltr01,Fltr02</FILTER>
                    </COLLECTION>
                    <SYSTEM TYPE="Formulae" NAME="Fltr01">NOT $IsCancelled</SYSTEM>
                    <SYSTEM TYPE="Formulae" NAME="Fltr02">NOT $IsOptional</SYSTEM>
                </TDLMESSAGE>
            </TDL>
        </DESC>
    </BODY>
</ENVELOPE>
"""
        self.logger.info(f"Constructed payload with company: '{company}'")
        self.logger.info(f"Payload XML: {payload_xml[:200]}...")
        return payload_xml

    def fetch_tally_data(self, payload):
        """Fetch data from Tally server."""
        max_retries = 3
        retry_delay = 5  # seconds
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Sending request to Tally server: {self.tally_url}")
                response = requests.post(self.tally_url, data=payload, timeout=60)
                self.logger.info(f"Received response from Tally. Status code: {response.status_code}")
                response.raise_for_status()
                if not response.text:
                    raise ValueError("Received empty response from Tally server")
                return response.text
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout while connecting to Tally server (Attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"Could not connect to Tally server at {self.tally_url} (Attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching data from Tally: {str(e)}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error while fetching Tally data: {str(e)}")
                raise

    def parse_tally_data_xml(self, xml_data, subscribe_id) -> Generator[List[Dict], None, None]:
        """Parse Tally XML response for transactions table."""
        try:
            root = ET.fromstring(xml_data)
            self.logger.info(f"XML Response (first 500 chars): {xml_data[:500]}...")
        except ET.ParseError as e:
            self.logger.error(f"transactions: Error parsing XML: {e}")
            sys.exit(1)

        next_transaction_id = self.get_next_transaction_id()
        transaction_counter = int(next_transaction_id[3:])
        
        processed_vouchers = {}  # To track unique vouchers by their number
        data = []
        
        vouchers = root.findall(".//VOUCHER")
        self.logger.info(f"Total vouchers found in XML: {len(vouchers)}")
        for voucher in vouchers:
            guid_text = voucher.findtext("GUID")
            date_text = voucher.findtext("DATE")
            voucher_type = voucher.findtext("VOUCHERTYPE") or ""
            voucher_number = voucher.findtext("VOUCHERNUMBER") or ""
            party_name = voucher.findtext("PARTYNAME") or ""
            narration = voucher.findtext("Narration") or ""
            
            self.logger.info(f"Processing Voucher - Type: {voucher_type}, Number: {voucher_number}, GUID: {guid_text}")
            self.logger.info(f"GUID from XML: {guid_text}, Narration: {narration}")
            
            # Use our helper to convert GUID
            if guid_text:
                guid_text = guid_text.strip()
                self.logger.info(f"Attempting to convert GUID: {guid_text}")
                guid_uuid = self.convert_guid(guid_text)
            else:
                guid_uuid = uuid.uuid4()
                self.logger.info(f"No GUID found, generated: {guid_uuid}")
            
            try:
                date_obj = datetime.datetime.strptime(date_text.strip(), '%d-%b-%y').date()
            except (ValueError, AttributeError):
                self.logger.warning(f"Invalid date format: {date_text}")
                continue

            voucher_key = f"{date_obj}_{voucher_type}_{voucher_number}"
            if voucher_key in processed_vouchers:
                continue
                
            total_amount = 0.0
            allocations = voucher.findall(".//ACCOUNTING_ALLOCATION")
            for allocation in allocations:
                amount_text = allocation.findtext("AMOUNT") or "0"
                amount_text = amount_text.replace("(-)", "-")
                try:
                    amount = float(amount_text)
                    total_amount += abs(amount)
                except ValueError:
                    pass
            
            transaction_id = f"TRN{transaction_counter:05d}"
            transaction_counter += 1
            
            row = {
                "transaction_id": transaction_id,
                "batch_id": None,
                "transaction_batch_id": None,
                "GUID": guid_uuid,
                "subscribe_id": subscribe_id,
                "file_status": None,
                "original_filename": None,
                "masterkeyids": None,
                "date": date_obj,
                "document_type": voucher_type,
                "document_number": voucher_number,
                "narration": narration,
                "party_name": party_name,
                "total_amount": total_amount,
                "user_id": self.session_data.get('userId', None),
                "filepath": None,
                "push_status": 0,
                "pushed_at": None,
                "created_by": self.session_data.get('userId', None),
                "updated_by": self.session_data.get('userId', None)
            }
            
            data.append(row)
            processed_vouchers[voucher_key] = True
            
            if len(data) >= 1000:
                yield data
                data = []
        if data:
            yield data
        self.logger.info(f"transactions: Parsed all records.")

    def insert_transactions_into_postgres(self, data: List[Dict], table_name: str):
        """Insert data into transactions table."""
        if not data:
            self.logger.info("No data to insert.")
            return
        self.logger.info(f"Processing {len(data)} records for insertion/update")
        if data:
            self.logger.info(f"Sample record before insertion: {data[0]}")
            if 'GUID' in data[0]:
                self.logger.info(f"GUID value: {data[0]['GUID']}, Type: {type(data[0]['GUID'])}")
            if 'narration' in data[0]:
                self.logger.info(f"Narration value: {data[0]['narration']}")
            if 'document_number' in data[0]:
                self.logger.info(f"Document number (voucher number) value: {data[0]['document_number']}")
        try:
            with self.db_cursor() as cursor:
                for idx, record in enumerate(data):
                    self.logger.info(f"Processing record {idx+1}/{len(data)}: document_number={record['document_number']}, document_type={record['document_type']}")
                    guid_str = str(record['GUID']) if isinstance(record.get('GUID'), uuid.UUID) else None
                    check_query = f"""
                        SELECT transaction_id FROM {table_name} 
                        WHERE document_number = %s
                    """
                    self.logger.info(f"Checking if document_number '{record['document_number']}' exists in database")
                    cursor.execute(check_query, (record['document_number'],))
                    existing = cursor.fetchone()
                    if existing:
                        self.logger.info(f"FOUND: Document number '{record['document_number']}' exists with transaction_id {existing[0]}, updating GUID and subscribe_id")
                        update_query = f"""
                            UPDATE {table_name}
                            SET GUID = %s, 
                                subscribe_id = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE document_number = %s
                        """
                        cursor.execute(update_query, (guid_str, record['subscribe_id'], record['document_number']))
                        self.logger.info(f"Updated record with document_number '{record['document_number']}'")
                    else:
                        self.logger.info(f"NOT FOUND: Document number '{record['document_number']}' not found, inserting new record")
                        insert_record = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in record.items()}
                        columns = ', '.join(insert_record.keys())
                        placeholders = ', '.join(['%s'] * len(insert_record))
                        insert_query = f"""
                            INSERT INTO {table_name} ({columns})
                            VALUES ({placeholders})
                        """
                        cursor.execute(insert_query, tuple(insert_record.values()))
                        self.logger.info(f"Inserted new record with document_number '{record['document_number']}'")
                self.logger.info("Data chunk processed successfully.")
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            raise

    def insert_transaction_details(self, ledger_entries: List[Dict]):
        """Insert ledger entries into transaction_details table."""
        if not ledger_entries:
            self.logger.info("No ledger entries provided for insertion.")
            return
        try:
            with self.db_cursor() as cursor:
                values = []
                for entry in ledger_entries:
                    values.append((
                        entry['ledger_name'],
                        entry['ledger_amount'],
                        str(entry['GUID']),
                        entry['subscribe_id'],
                        entry['amount_status'],
                        self.session_data.get('userId'),
                        self.session_data.get('userId')
                    ))
                insert_query = """
                    INSERT INTO transaction_details 
                    (ledger_name, ledger_amount, GUID, subscribe_id, amount_status, created_by, updated_by)
                    VALUES %s
                """
                execute_values(cursor, insert_query, values)
            self.logger.info("Inserted new transaction_details records.")
        except Exception as e:
            self.logger.error(f"Error inserting transaction_details: {str(e)}")
            raise

    def sync_transaction_details(self, subscribe_id: str):
        """Sync transaction details data with the updated XML format"""
        start_date = self.tally_data_config['from_date']
        end_date = self.tally_data_config['to_date']
        try:
            self.logger.info(f"Starting transaction details sync from {start_date} to {end_date}")
            payload = self.construct_tally_data_payload(start_date, end_date, self.tally_data_config["company_name"])
            xml_data = self.fetch_tally_data(payload)
            success_count, failure_count = self.verify_and_update_transaction_details(xml_data, subscribe_id)
            self.logger.info(f"Transaction details sync completed. Success: {success_count}, Failures: {failure_count}")
            if failure_count > 0:
                self.logger.warning("There were verification failures. Check logs for details.")
            return success_count, failure_count
        except Exception as e:
            self.logger.error(f"Error in transaction details sync: {str(e)}")
            return 0, 0  

    def get_transaction_details_by_guid(self, guid_str):
        try:
            with self.db_cursor() as cursor:
                cursor.execute("""
                    SELECT entry_id, ledger_name, ledger_amount 
                    FROM transaction_details 
                    WHERE GUID = %s
                """, (guid_str,))
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error getting transaction details by GUID: {str(e)}")
            return []

    def parse_ledger_entries_from_voucher(self, voucher, guid_uuid, subscribe_id):
        """Parse ledger entries from a voucher element and return structured data"""
        ledger_entries = []
        allocations = voucher.findall(".//ACCOUNTING_ALLOCATION")
        for allocation in allocations:
            ledger_name = allocation.findtext("LEDGER") or ""
            amount_text = allocation.findtext("AMOUNT") or "0"
            amount_status = allocation.findtext("DRCR") or ""
            if not amount_status:
                try:
                    amount = float(amount_text.replace("(-)", "-"))
                    amount_status = "Dr" if amount < 0 else "Cr"
                except ValueError:
                    self.logger.warning(f"Invalid amount format for ledger {ledger_name}: {amount_text}")
                    amount_status = "Unknown"
            try:
                ledger_amount = abs(float(amount_text.replace("(-)", "-")))
            except ValueError:
                self.logger.warning(f"Invalid amount format for ledger {ledger_name}: {amount_text}")
                ledger_amount = 0.0
            ledger_entries.append({
                "ledger_name": ledger_name,
                "ledger_amount": ledger_amount,
                "amount_status": amount_status,
                "GUID": guid_uuid,
                "subscribe_id": subscribe_id
            })
        return ledger_entries

    def verify_and_update_transaction_details(self, xml_data, subscribe_id):
        """
        Parse Tally XML, verify transaction details for each voucher, and either insert
        new details if missing or update existing details if matching ledger entries are found.
        If ledger amounts or names do not match, a warning is raised.
        """
        root = ET.fromstring(xml_data)
        success_count = 0
        failure_count = 0
        for voucher in root.findall(".//VOUCHER"):
            guid_text = voucher.findtext("GUID") or ""
            if not guid_text:
                self.logger.warning("Voucher without GUID found, skipping.")
                continue
            # Use our helper to convert GUID
            guid_text = guid_text.strip()
            guid_uuid = self.convert_guid(guid_text)
            guid_str = str(guid_uuid)
            
            api_ledger_entries = self.parse_ledger_entries_from_voucher(voucher, guid_uuid, subscribe_id)
            if not api_ledger_entries:
                self.logger.warning(f"No ledger entries found for voucher with GUID {guid_str}")
                continue
            
            db_entries = self.get_transaction_details_by_guid(guid_str)
            if not db_entries:
                self.logger.info(f"No transaction_details found with GUID {guid_str}, inserting new transaction_details.")
                try:
                    self.insert_transaction_details(api_ledger_entries)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"Error inserting transaction_details for GUID {guid_str}: {e}")
                    failure_count += 1
                continue
            
            db_entries_map = {row['ledger_name']: {
                'entry_id': row['entry_id'], 
                'amount': float(row['ledger_amount'])
            } for row in db_entries}
            api_entries_map = {entry['ledger_name']: entry for entry in api_ledger_entries}
            
            missing_in_api = set(db_entries_map.keys()) - set(api_entries_map.keys())
            if missing_in_api:
                self.logger.error(f"Ledgers in DB not found in API response for GUID {guid_str}: {missing_in_api}")
                failure_count += 1
                continue
            
            missing_in_db = set(api_entries_map.keys()) - set(db_entries_map.keys())
            if missing_in_db:
                self.logger.warning(f"New ledgers in API not in DB for GUID {guid_str}: {missing_in_db}")
            
            amount_mismatches = []
            for ledger_name in set(db_entries_map.keys()) & set(api_entries_map.keys()):
                db_amount = db_entries_map[ledger_name]['amount']
                api_amount = float(api_entries_map[ledger_name]['ledger_amount'])
                if abs(db_amount - api_amount) > 0.01:
                    amount_mismatches.append(f"{ledger_name}: DB={db_amount}, API={api_amount}")
            if amount_mismatches:
                self.logger.error(f"Amount mismatches for GUID {guid_str}: {amount_mismatches}")
                failure_count += 1
                continue
            
            try:
                with self.db_cursor() as cursor:
                    for ledger_name, entry in db_entries_map.items():
                        if ledger_name in api_entries_map:
                            api_entry = api_entries_map[ledger_name]
                            entry_id = entry['entry_id']
                            cursor.execute("""
                                UPDATE transaction_details
                                SET GUID = %s,
                                    subscribe_id = %s,
                                    amount_status = %s,
                                    updated_at = CURRENT_TIMESTAMP,
                                    updated_by = %s
                                WHERE entry_id = %s
                            """, (
                                guid_str,
                                subscribe_id,
                                api_entry['amount_status'],
                                self.session_data.get('userId'),
                                entry_id
                            ))
                success_count += 1
                self.logger.info(f"Updated transaction_details for GUID {guid_str}")
            except Exception as e:
                self.logger.error(f"Error updating transaction_details for GUID {guid_str}: {str(e)}")
                failure_count += 1
        return success_count, failure_count

    def sync_data_by_year(self, start_date: datetime.date, end_date: datetime.date, overall_pbar, subscribe_id: str) -> bool:
        """Sync data for a specific year range with progress tracking."""
        try:
            year_pbar = tqdm(
                total=100,
                desc=f"Processing {start_date.year}-{end_date.year}",
                leave=True,
                position=1
            )
            self.logger.info(f"Processing data from {start_date} to {end_date}")
            year_pbar.set_description("Constructing payload")
            payload = self.construct_tally_data_payload(start_date, end_date, self.tally_data_config["company_name"])
            year_pbar.update(20)
            year_pbar.set_description("Fetching data")
            xml_data = self.fetch_tally_data(payload)
            year_pbar.update(30)
            year_pbar.set_description("Parsing and inserting data")
            total_records = 0
            for chunk in self.parse_tally_data_xml(xml_data, subscribe_id):
                self.insert_transactions_into_postgres(chunk, self.tally_data_config["table_name"])
                total_records += len(chunk)
                year_pbar.update(50 * (len(chunk) / 100000))
            self.logger.info(f"Total records processed: {total_records}")
            year_pbar.close()
            overall_pbar.update(1)
            return True
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return False

    def sync_tally_data(self, subscribe_id: str):
        """Sync all tally data with progress tracking."""
        start_date = self.tally_data_config['from_date']
        end_date = self.tally_data_config['to_date']
        years_diff = end_date.year - start_date.year
        if end_date.month < start_date.month or (end_date.month == start_date.month and end_date.day < start_date.day):
            years_diff -= 1
        total_chunks = max(1, years_diff + 1)
        overall_pbar = tqdm(
            total=total_chunks,
            desc="Overall Progress",
            leave=True,
            position=0
        )
        success_chunks = []
        failed_chunks = []
        print(f"\nProcessing data in {total_chunks} {'chunk' if total_chunks == 1 else 'chunks'}...")
        if total_chunks == 1:
            if self.sync_data_by_year(start_date, end_date, overall_pbar, subscribe_id):
                success_chunks.append(f"{start_date} to {end_date}")
            else:
                failed_chunks.append(f"{start_date} to {end_date}")
        else:
            current_date = start_date
            while current_date < end_date:
                year_end = min(
                    datetime.date(current_date.year + 1, current_date.month, current_date.day) - datetime.timedelta(days=1),
                    end_date
                )
                if self.sync_data_by_year(current_date, year_end, overall_pbar, subscribe_id):
                    success_chunks.append(f"{current_date} to {year_end}")
                else:
                    failed_chunks.append(f"{current_date} to {year_end}")
                current_date = year_end + datetime.timedelta(days=1)
        overall_pbar.close()
        print("\nSync Summary:")
        print(f"Total Chunks Processed: {total_chunks}")
        print(f"Successfully Processed: {len(success_chunks)}")
        print(f"Failed: {len(failed_chunks)}")
        if failed_chunks:
            print("\nFailed Chunks:")
            for chunk in failed_chunks:
                print(f"- {chunk}")
        self.logger.info("Tally data sync completed")
        return success_chunks, failed_chunks

def main():
    if len(sys.argv) < 2:
        print("Usage: python tally_data.py key1=value1 key2=value2 ...")
        sys.exit(1)
    session_data = {}
    for arg in sys.argv[1:]:
        key, value = arg.split('=')
        session_data[key] = value
    required_keys = ['userId', 'userCompanyId', 'tallyCompanyId', 'subscribeId', 'start_date', 'end_date']
    if not all(key in session_data for key in required_keys):
        print(f"Error: Missing required session data. Required keys: {', '.join(required_keys)}")
        sys.exit(1)
    tally = TallyIntegration(session_data)
    subscribe_id = session_data['subscribeId']
    # Sync transactions
    success_chunks, failed_chunks = tally.sync_tally_data(subscribe_id)
    print(f"Transactions sync completed. Success chunks: {success_chunks}, Failed chunks: {failed_chunks}")
    # Sync transaction details (upsert or insert ledger entries)
    success_count, failure_count = tally.sync_transaction_details(subscribe_id)
    print(f"Transaction details sync completed. Success: {success_count}, Failures: {failure_count}")

if __name__ == "__main__":
    main()

