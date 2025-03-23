# correct the code two table mergere in one code 
import requests
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime
import xml.etree.ElementTree as ET
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict
import re
import html
import csv
from io import StringIO
import sys
import pandas as pd
import numpy as np  # To handle numerical data types
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
handler = RotatingFileHandler('ledgergroups.log', maxBytes=5*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Tally's HTTP interface URL from environment variables
TALLY_URL = os.getenv("TALLY_URL", "http://localhost:9000")


# Table names
GROUP_TABLE_NAME = "group_data"
LEDGER_TABLE_NAME = "ledger_table"

class TallyLedgerSync:
    def __init__(self, session_data: Dict[str, str], tally_url: str):
        self.session_data = session_data
        self.db_params = {
            'dbname': f"user_{session_data['userId']}_db",
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        self.tally_url = tally_url
        self.logger = logging.getLogger(__name__)
        self.group_cache = {}  # Initialize the group_cache   

    def setup_logging(self):
        self.logger = logging.getLogger('TallyLedgerSync')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = RotatingFileHandler('tally_sync.log', maxBytes=5*1024*1024, backupCount=5)
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def load_group_cache(self, conn):
        """Load all tally groups into cache for efficient lookups."""
        self.logger.info("Loading group cache")
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT name, primary_group, parent_group, group_name, 
                           is_revenue, is_deemed_positive, affects_gross_profit, 
                           sort_position 
                    FROM group_data
                """)
                for row in cursor.fetchall():
                    self.group_cache[row[0]] = {
                        'primary_group': row[1],
                        'parent_group': row[2],
                        'group_name': row[3],
                        'is_revenue': row[4],
                        'is_deemed_positive': row[5],
                        'affects_gross_profit': row[6],
                        'sort_position': row[7]
                    }
            self.logger.info(f"Loaded {len(self.group_cache)} groups into cache")
        except Exception as e:
            self.logger.error(f"Error loading group cache: {str(e)}", exc_info=True)
            raise

    def get_group_details(self, parent_group_name: str) -> Dict:
        """Get group details from cache."""
        return self.group_cache.get(parent_group_name, {})

    def get_tally_data(self, payload: str) -> str:
        try:
            self.logger.info(f"Sending request to Tally: {self.tally_url}")
            response = requests.post(self.tally_url, data=payload, headers={"Content-Type": "text/xml;charset=utf-8"})
            response.raise_for_status()
            self.logger.info("Received response from Tally")
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Error fetching data from Tally: {str(e)}", exc_info=True)
            raise

    def clean_xml(self, xml_string: str) -> str:
        """Clean the XML string to handle invalid characters and encoding issues."""
        # Remove invalid XML characters
        xml_string = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_string)
       
        # Handle special cases before general replacement
        xml_string = xml_string.replace('&', '&')  # Fix double-encoded ampersands
        xml_string = xml_string.replace('&', '&')  # Properly encode ampersands
       
        if xml_string.startswith('\ufeff'):
            xml_string = xml_string[1:]
        return xml_string

    def decode_html_entities(self, text: str) -> str:
        """Decode HTML entities to their proper characters."""
        # First unescape any HTML entities
        text = html.unescape(text)
        # Fix any remaining & issues
        text = text.replace('&', '&')
        return text

    def parse_tally_response(self, xml_response: str) -> List[Dict]:
        """Parse XML response from Tally and extract ledger information."""
        ledgers = []
        try:
            cleaned_xml = self.clean_xml(xml_response)
           
            # Parse using line-by-line approach
            ledger_pattern = re.compile(r'<LEDGER\s+NAME="([^"]+)"[^>]*>')
            parent_pattern = re.compile(r'<PARENT>([^<]+)</PARENT>')
           
            current_ledger = None
            current_parent = None
           
            for line in cleaned_xml.splitlines():
                ledger_match = ledger_pattern.search(line)
                if ledger_match:
                    current_ledger = self.decode_html_entities(ledger_match.group(1))
               
                parent_match = parent_pattern.search(line)
                if parent_match:
                    current_parent = self.decode_html_entities(parent_match.group(1))
               
                if current_ledger and current_parent:
                    ledger_info = {
                        'name': current_ledger.strip(),
                        'parent_group': current_parent.strip()
                    }
                    ledgers.append(ledger_info)
                    if len(ledgers) <= 3:
                        self.logger.info(f"Sample ledger: {ledger_info}")
                    current_ledger = None
                    current_parent = None
           
            self.logger.info(f"Successfully parsed {len(ledgers)} valid ledgers")
            return ledgers
        except Exception as e:
            self.logger.error(f"Error parsing Tally response: {str(e)}")
            raise

    def ensure_tables_exist(self, conn):
        """Ensure both ledger and group tables exist with correct schema."""
        # Create group_data table if it doesn't exist
        create_groups_table_query = f"""
        CREATE TABLE IF NOT EXISTS {GROUP_TABLE_NAME}(
            name TEXT PRIMARY KEY,
            primary_group TEXT,
            parent_group TEXT,
            group_name TEXT,
            is_revenue BOOLEAN,
            is_deemed_positive BOOLEAN,
            affects_gross_profit BOOLEAN,
            sort_position INTEGER
        );
        """

        # Enhanced ledger table with group-related columns
        create_ledger_table_query = f"""
        CREATE TABLE IF NOT EXISTS {LEDGER_TABLE_NAME} (
            ledger_id SERIAL PRIMARY KEY,
            ledger_name VARCHAR(255) NOT NULL UNIQUE,
            parent_group VARCHAR(500) NOT NULL,
            primary_group VARCHAR(500),
            group_name VARCHAR(500),
            group_parent VARCHAR(500),  -- This is parent_group from group_data
            is_revenue BOOLEAN,
            is_deemed_positive BOOLEAN,
            affects_gross_profit BOOLEAN,
            sort_position INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with conn.cursor() as cursor:
            cursor.execute(create_groups_table_query)
            cursor.execute(create_ledger_table_query)
        conn.commit()
        self.logger.info("Ensured all tables exist with correct schema")

    def save_group_data_to_database(self, group_data: List[Dict]):
        """Insert or update group data into the database."""
        if not group_data:
            self.logger.warning("No group data to save to database")
            return

        try:
            with psycopg2.connect(**self.db_params) as conn:
                self.ensure_tables_exist(conn)
                
                with conn.cursor() as cursor:
                    upsert_query = sql.SQL("""
                        INSERT INTO {table} (name, primary_group, parent_group, group_name, is_revenue, is_deemed_positive, affects_gross_profit, sort_position)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (name)
                        DO UPDATE SET
                            primary_group = EXCLUDED.primary_group,
                            parent_group = EXCLUDED.parent_group,
                            group_name = EXCLUDED.group_name,
                            is_revenue = EXCLUDED.is_revenue,
                            is_deemed_positive = EXCLUDED.is_deemed_positive,
                            affects_gross_profit = EXCLUDED.affects_gross_profit,
                            sort_position = EXCLUDED.sort_position;
                    """).format(table=sql.Identifier(GROUP_TABLE_NAME))
                    
                    values = [(
                        group['name'],
                        group['primary_group'],
                        group['parent_group'],
                        group['group_name'],
                        group['is_revenue'],
                        group['is_deemed_positive'],
                        group['affects_gross_profit'],
                        group['sort_position']
                    ) for group in group_data]
                    
                    extras.execute_batch(cursor, upsert_query.as_string(conn), values)
                    
                conn.commit()
                self.logger.info(f"Inserted/Updated {len(group_data)} group records into {GROUP_TABLE_NAME}")
        except psycopg2.Error as e:
            self.logger.error(f"Database error while saving group data: {str(e)}")
            raise

    def parse_csv_response(self, csv_data: str) -> List[Dict]:
        """
        Parses the CSV data received from Tally and transforms it according to specifications.
        """
        data = []
        sample_logs = 0
        max_sample_logs = 5

        try:
            # Define the expected field names in the order they appear in the CSV
            fieldnames = ["Name", "PrimaryGroup", "ParentGroup", "GroupName", "IsRevenue", "IsDeemedPositive", "AffectsGrossProfit", "SortPosition"]

            # Use StringIO to read the CSV data
            csv_reader = csv.DictReader(StringIO(csv_data), fieldnames=fieldnames)

            # Read the first row to check if it's a header
            first_row = next(csv_reader)
            if first_row["Name"].strip().lower() == "name":
                self.logger.info("Header row detected. Skipping header.")
            else:
                # First row is actual data, process it
                row = first_row
                transformed_row = {
                    "name": row['Name'].strip(),
                    "primary_group": row['PrimaryGroup'].strip(),
                    "parent_group": row['ParentGroup'].strip(),
                    "group_name": row['GroupName'].strip(),
                    "is_revenue": row['IsRevenue'].strip().lower() == "yes",
                    "is_deemed_positive": row['IsDeemedPositive'].strip().lower() == "yes",
                    "affects_gross_profit": row['AffectsGrossProfit'].strip().lower() == "yes",
                    "sort_position": int(row['SortPosition']) if row['SortPosition'].isdigit() else None
                }
                data.append(transformed_row)
                if sample_logs < max_sample_logs:
                    self.logger.info(f"Sample Row: {transformed_row}")
                    sample_logs += 1

            # Iterate over remaining rows
            for row in csv_reader:
                # Log sample rows for debugging
                if sample_logs < max_sample_logs:
                    self.logger.info(f"Sample Row: {row}")
                    sample_logs += 1

                # Transform fields
                transformed_row = {
                    "name": row['Name'].strip(),
                    "primary_group": row['PrimaryGroup'].strip(),
                    "parent_group": row['ParentGroup'].strip(),
                    "group_name": row['GroupName'].strip(),
                    "is_revenue": row['IsRevenue'].strip().lower() == "yes",
                    "is_deemed_positive": row['IsDeemedPositive'].strip().lower() == "yes",
                    "affects_gross_profit": row['AffectsGrossProfit'].strip().lower() == "yes",
                    "sort_position": int(row['SortPosition']) if row['SortPosition'].isdigit() else None
                }
                data.append(transformed_row)

            self.logger.info(f"Parsed {len(data)} group records from CSV.")
            return data

        except Exception as e:
            self.logger.error(f"Error parsing CSV: {e}")
            raise

    def fetch_and_store_group_data(self):
        try:
            self.logger.info("Fetching group data from Tally")
            payload = self.construct_group_payload()
            response = self.get_tally_data(payload)
            group_data = self.parse_csv_response(response)
            self.save_group_data_to_database(group_data)
        except Exception as e:
            self.logger.error(f"Error in fetch_and_store_group_data: {str(e)}", exc_info=True)
            raise

    def construct_group_payload(self) -> str:
        """
        Constructs the XML payload for fetching group data from Tally.
        """
        payload_xml = """<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>GroupMasterReport</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>ASCII (Comma Delimited)</SVEXPORTFORMAT>
            </STATICVARIABLES>
            <TDL>
                <TDLMESSAGE>
                    <REPORT NAME="GroupMasterReport">
                        <FORMS>GroupForm</FORMS>
                    </REPORT>
                    <FORM NAME="GroupForm">
                        <PARTS>GroupPart</PARTS>
                    </FORM>
                    <PART NAME="GroupPart">
                        <LINES>GroupLine</LINES>
                        <REPEAT>GroupLine : GroupCollection</REPEAT>
                        <SCROLLED>Vertical</SCROLLED>
                    </PART>
                    <LINE NAME="GroupLine">
                        <FIELDS>Name,PrimaryGroup,ParentGroup,GroupName,IsRevenue,IsDeemedPositive,AffectsGrossProfit,SortPosition</FIELDS>
                    </LINE>
                    <FIELD NAME="Name">
                        <SET>$Name</SET>
                    </FIELD>
                    <FIELD NAME="PrimaryGroup">
                        <SET>$_PrimaryGroup</SET>
                    </FIELD>
                    <FIELD NAME="ParentGroup">
                        <SET>$Parent</SET>
                    </FIELD>
                    <FIELD NAME="GroupName">
                        <SET>$GroupName</SET>
                    </FIELD>
                    <FIELD NAME="IsRevenue">
                        <SET>$IsRevenue</SET>
                    </FIELD>
                    <FIELD NAME="IsDeemedPositive">
                        <SET>$IsDeemedPositive</SET>
                    </FIELD>
                    <FIELD NAME="AffectsGrossProfit">
                        <SET>$AffectsGrossProfit</SET>
                    </FIELD>
                    <FIELD NAME="SortPosition">
                        <SET>$SortPosition</SET>
                    </FIELD>
                    <COLLECTION NAME="GroupCollection">
                        <TYPE>Group</TYPE>
                        <FETCH>Name,PrimaryGroup,Parent,GroupName,IsRevenue,IsDeemedPositive,AffectsGrossProfit,SortPosition</FETCH>
                    </COLLECTION>
                </TDLMESSAGE>
            </TDL>
        </DESC>
    </BODY>
</ENVELOPE>"""
        return payload_xml

    def parse_tally_ledger_response(self, xml_response: str) -> List[Dict]:
        """Parse XML response from Tally and extract ledger information."""
        ledgers = []
        try:
            cleaned_xml = self.clean_xml(xml_response)
           
            # Parse using line-by-line approach
            ledger_pattern = re.compile(r'<LEDGER\s+NAME="([^"]+)"[^>]*>')
            parent_pattern = re.compile(r'<PARENT>([^<]+)</PARENT>')
           
            current_ledger = None
            current_parent = None
           
            for line in cleaned_xml.splitlines():
                ledger_match = ledger_pattern.search(line)
                if ledger_match:
                    current_ledger = self.decode_html_entities(ledger_match.group(1))
               
                parent_match = parent_pattern.search(line)
                if parent_match:
                    current_parent = self.decode_html_entities(parent_match.group(1))
               
                if current_ledger and current_parent:
                    ledger_info = {
                        'name': current_ledger.strip(),
                        'parent_group': current_parent.strip()
                    }
                    ledgers.append(ledger_info)
                    if len(ledgers) <= 3:
                        self.logger.info(f"Sample ledger: {ledger_info}")
                    current_ledger = None
                    current_parent = None
           
            self.logger.info(f"Successfully parsed {len(ledgers)} valid ledgers")
            return ledgers
        except Exception as e:
            self.logger.error(f"Error parsing Tally ledger response: {str(e)}")
            raise

    def save_to_database(self, ledgers: List[Dict]):
        """Insert or update ledger data into the database."""
        if not ledgers:
            self.logger.warning("No ledgers to save to database")
            return

        try:
            with psycopg2.connect(**self.db_params) as conn:
                self.ensure_tables_exist(conn)
                self.load_group_cache(conn)
                
                inserted_count = 0
                updated_count = 0
                skipped_count = 0
                
                with conn.cursor() as cursor:
                    for ledger in ledgers:
                        if not ledger['name']:
                            self.logger.warning(f"Skipping ledger with empty name: {ledger}")
                            skipped_count += 1
                            continue
                        
                        # Get group details from cache
                        group_details = self.get_group_details(ledger['parent_group'])
                        
                        if not group_details:
                            self.logger.warning(f"No matching group found for ledger {ledger['name']} with parent {ledger['parent_group']}")
                            skipped_count += 1
                            continue

                        upsert_query = sql.SQL("""
                        INSERT INTO {ledger_table} (
                            ledger_name, parent_group, primary_group, group_name, group_parent,
                            is_revenue, is_deemed_positive, affects_gross_profit, sort_position
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ledger_name)
                        DO UPDATE SET
                            parent_group = EXCLUDED.parent_group,
                            primary_group = EXCLUDED.primary_group,
                            group_name = EXCLUDED.group_name,
                            group_parent = EXCLUDED.group_parent,
                            is_revenue = EXCLUDED.is_revenue,
                            is_deemed_positive = EXCLUDED.is_deemed_positive,
                            affects_gross_profit = EXCLUDED.affects_gross_profit,
                            sort_position = EXCLUDED.sort_position,
                            created_at = CURRENT_TIMESTAMP
                        RETURNING (xmax = 0) AS inserted;
                        """).format(ledger_table=sql.Identifier(LEDGER_TABLE_NAME))
                        
                        cursor.execute(upsert_query, (
                            ledger['name'],
                            ledger['parent_group'],
                            group_details.get('primary_group'),
                            group_details.get('group_name'),
                            group_details.get('parent_group'),  # This is the parent_group from group_data
                            group_details.get('is_revenue'),
                            group_details.get('is_deemed_positive'),
                            group_details.get('affects_gross_profit'),
                            group_details.get('sort_position')
                        ))
                        
                        # Check if it was an insert or update
                        result = cursor.fetchone()
                        if result and result[0]:
                            inserted_count += 1
                        else:
                            updated_count += 1
                            
                conn.commit()
                self.logger.info(f"Database sync completed: {inserted_count} inserted, {updated_count} updated, {skipped_count} skipped")
                
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {str(e)}")
            raise

    def sync_ledgers(self):
        try:
            self.logger.info("Syncing ledgers")
            payload = self.construct_ledger_payload()
            response = self.get_tally_data(payload)
            ledgers = self.parse_tally_ledger_response(response)
            self.save_to_database(ledgers)
        except Exception as e:
            self.logger.error(f"Error in sync_ledgers: {str(e)}", exc_info=True)
            raise


    def construct_ledger_payload(self) -> str:
        """Construct the XML payload for fetching ledger data from Tally."""
        payload_xml = """
        <ENVELOPE>
            <HEADER>
                <VERSION>1</VERSION>
                <TALLYREQUEST>Export</TALLYREQUEST>
                <TYPE>Data</TYPE>
                <ID>List of Accounts</ID>
            </HEADER>
            <BODY>
                <DESC>
                    <STATICVARIABLES>
                        <EXPLODEFLAG>Yes</EXPLODEFLAG>
                        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                    </STATICVARIABLES>
                    <TDL>
                        <TDLMESSAGE>
                            <COLLECTION NAME="List of Accounts">
                                <TYPE>Ledger</TYPE>
                                <FETCH>NAME, PARENT, GROUPNAME, PRIMARYGROUP</FETCH>
                            </COLLECTION>
                        </TDLMESSAGE>
                    </TDL>
                </DESC>
            </BODY>
        </ENVELOPE>
        """
        return payload_xml

    def sync_all(self):
        """Perform the complete synchronization process."""
        try:
            self.logger.info("Starting synchronization process")
            self.logger.info(f"Session data: {self.session_data}")
            self.logger.info(f"DB params: {self.db_params}")
            self.logger.info(f"Tally URL: {self.tally_url}")
            
            self.fetch_and_store_group_data()
            self.sync_ledgers()
            
            self.logger.info("Synchronization completed successfully")
        except Exception as e:
            self.logger.error(f"Synchronization process failed: {str(e)}", exc_info=True)
            raise

def main():
    logger.info("Starting ledgergroups.py script")
    
    if len(sys.argv) < 2:
        logger.error("Usage: python ledgergroups.py key1=value1 key2=value2 ...")
        sys.exit(1)

    session_data = {}
    for arg in sys.argv[1:]:
        try:
            key, value = arg.split('=', 1)  # Split on first '=' only
            session_data[key] = value.strip('"')  # Remove surrounding quotes if present
        except ValueError:
            logger.error(f"Invalid argument format: {arg}")
            sys.exit(1)

    logger.info(f"Received session data: {session_data}")

    required_keys = ['userId', 'userCompanyId', 'tallyCompanyId', 'subscribeId', 'start_date', 'end_date']
    missing_keys = [key for key in required_keys if key not in session_data]
    if missing_keys:
        logger.error(f"Error: Missing required session data. Missing keys: {', '.join(missing_keys)}")
        sys.exit(1)

    try:
        tally_sync = TallyLedgerSync(session_data, TALLY_URL)
        tally_sync.sync_all()
    except Exception as e:
        logger.error(f"An error occurred during synchronization: {str(e)}", exc_info=True)
        sys.exit(1)

    logger.info("ledgergroups.py script completed successfully")

if __name__ == "__main__":
    main()




