import sys
import psycopg2
from psycopg2 import Error
from datetime import datetime
import openai
from typing import Dict, List
import json
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define a filter to allow only logs below a certain level
class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level

    def filter(self, record):
        return record.levelno < self.max_level

# Set up logging configuration:
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all logs in handlers

# Console handler: show INFO and below (ERROR messages will not appear on the console)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.addFilter(MaxLevelFilter(logging.ERROR))
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# File handler: log ERROR and above to a file
error_handler = logging.FileHandler('error.log')
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)

logger.addHandler(console_handler)
logger.addHandler(error_handler)


class LedgerAIClassifier:
    """AI-powered classifier for ledger entries using OpenAI's GPT models."""
    def __init__(self, api_key: str, classification_schema: str = None):
        openai.api_key = api_key
        self.classification_cache = {}
        if classification_schema is None:
            self.classification_schema = (
                "You are a financial accounting expert. Given the following account groups and categories with their GL code ranges, "
                "classify each ledger entry into one of the account group codes listed below. Note that while the categories use 4-digit GL codes, "
                "the final posting ledger must use a 6-digit GL code (the 4-digit base expanded with two digits as per the sub-range). "
                "\n\nCategories (fixed):\n"
                " - Owners Equity: 1000 to 1999\n"
                " - Non-current liabilities: 2000 to 2999\n"
                " - Current liabilities: 3000 to 3999\n"
                " - Non-current assets: 4000 to 4999\n"
                " - Current assets: 5000 to 5999\n"
                " - Income: 6000 to 6999\n"
                " - Expenses: 7000 to 8999\n"
                " - Appropriation: 8800 to 8999\n\n"
                "Account Groups:\n"
                "1. Share capital (Code: SHCP) – belongs to Owners Equity, Subrange: 1000 to 1099.\n"
                "2. Reserves and Surplus (Code: RESV) – belongs to Owners Equity, Subrange: 1100 to 1999.\n"
                "3. Long-term borrowings (Code: LTBR) – belongs to Non-current liabilities, Subrange: 2000 to 2249.\n"
                "4. Trade payables (Non-current) (Code: TRPY-N) – belongs to Non-current liabilities, Subrange: 2250 to 2299.\n"
                "5. Other long-term liabilities (Code: OLTL) – belongs to Non-current liabilities, Subrange: 2300 to 2549.\n"
                "6. Long-term provisions (Code: LTPR) – belongs to Non-current liabilities, Subrange: 2550 to 2799.\n"
                "7. Short-term borrowings (Code: STBR) – belongs to Current liabilities, Subrange: 3000 to 3149.\n"
                "8. Trade payables (Current) (Code: TRPY-C) – belongs to Current liabilities, Subrange: 3150 to 3499.\n"
                "9. Other current liabilities (Code: OCLI) – belongs to Current liabilities, Subrange: 3500 to 3799.\n"
                "10. Short-term provisions (Code: STPR) – belongs to Current liabilities, Subrange: 3800 to 3899.\n"
                "11. Property, Plant and Equipment (Code: PPEA) – belongs to Non-current assets, Subrange: 4000 to 4299.\n"
                "12. Intangible assets (Code: INTG) – belongs to Non-current assets, Subrange: 4300 to 4349.\n"
                "13. Capital work-in-progress (Code: CWIP) – belongs to Non-current assets, Subrange: 4350 to 4399.\n"
                "14. Non-current investments (Code: NCIN) – belongs to Non-current assets, Subrange: 4400 to 4499.\n"
                "15. Deferred tax assets (net) (Code: DTAX) – belongs to Non-current assets, Subrange: 4500 to 4529.\n"
                "16. Long-term Loans and advances (Code: LTLA) – belongs to Non-current assets, Subrange: 4530 to 4599.\n"
                "17. Trade receivables (Non-current) (Code: TRDR-N) – belongs to Non-current assets, Subrange: 4600 to 4649.\n"
                "18. Other non-current assets (Code: ONCA) – belongs to Non-current assets, Subrange: 4650 to 4699.\n"
                "19. Current investments (Code: CUIN) – belongs to Current assets, Subrange: 5000 to 5299.\n"
                "20. Inventories (Code: INVT) – belongs to Current assets, Subrange: 5300 to 5549.\n"
                "21. Trade receivables (Current) (Code: TRDR-C) – belongs to Current assets, Subrange: 5550 to 5799.\n"
                "22. Cash and bank balances (Code: CASH) – belongs to Current assets, Subrange: 5800 to 5849.\n"
                "23. Short-term Loans and advances (Code: STLA) – belongs to Current assets, Subrange: 5850 to 5899.\n"
                "24. Other current assets (Code: OCCA) – belongs to Current assets, Subrange: 5900 to 5949.\n"
                "25. Revenue from operations (Code: REVO) – belongs to Income, Subrange: 6000 to 6799.\n"
                "26. Other income (Code: OTHI) – belongs to Income, Subrange: 6800 to 6999.\n"
                "27. Cost of material & components consumed (Code: COGS) – belongs to Expenses, Subrange: 7000 to 7399.\n"
                "28. Purchases (Code: PURC) – belongs to Expenses, Subrange: 7400 to 7799.\n"
                "29. (Increase)/decrease in Inventories (Code: INVT) – belongs to Expenses, Subrange: 7800 to 7999.\n"
                "30. Employee benefits expense (Code: EMPB) – belongs to Expenses, Subrange: 8000 to 8199.\n"
                "31. Depreciation and amortization expense (Code: DEPR) – belongs to Expenses, Subrange: 8200 to 8299.\n"
                "32. Finance costs (Code: FNCE) – belongs to Expenses, Subrange: 8300 to 8390.\n"
                "33. Share of (profit)/ loss from investment in partnership firm (Code: INPF) – belongs to Expenses, Subrange: 8391 to 8399.\n"
                "34. Other expenses (Code: OTHE) – belongs to Expenses, Subrange: 8400 to 8799.\n"
                "35. Exceptional items (Code: EXIT) – belongs to Appropriation, Subrange: 8800 to 8879.\n"
                "36. Tax expenses (Code: TAXE) – belongs to Appropriation, Subrange: 8880 to 8939.\n"
                "37. Current tax (Code: CURT) – belongs to Appropriation, Subrange: 8940 to 8969.\n"
                "38. Deferred tax (Code: DEFT) – belongs to Appropriation, Subrange: 8970 to 8979.\n\n"
                "For each ledger entry provided, analyze the fields 'primary_group', 'parent_group', and 'ledger_name' and assign it the appropriate "
                "account group code from the list above. Return ONLY the account group code (e.g., SHCP, RESV, LTBR, etc.) in a JSON array corresponding "
                "to the order of the ledger entries provided. Do not include any additional text or explanation."
            )
        else:
            self.classification_schema = classification_schema

    def get_cache_key(self, entry_data: Dict) -> str:
        return f"{entry_data['primary_group']}::{entry_data['parent_group']}"

    def batch_classify_entries(self, entries: List[Dict]) -> Dict[str, str]:
        unique_entries = {}
        classifications = {}

        for entry in entries:
            cache_key = self.get_cache_key(entry)
            if cache_key in self.classification_cache:
                classifications[cache_key] = self.classification_cache[cache_key]
            elif cache_key not in unique_entries:
                unique_entries[cache_key] = entry

        if not unique_entries:
            return classifications

        try:
            entries_description = "\n".join(
                f"Entry: Primary Group: {entry['primary_group']}, Parent Group: {entry['parent_group']}, Name: {entry['ledger_name']}"
                for entry in unique_entries.values()
            )
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": self.classification_schema},
                    {"role": "user", "content": f"Classify these entries (return an array of account group codes only):\n{entries_description}"}
                ],
                temperature=0.1
            )
            try:
                categories = json.loads(response.choices[0].message.content)
                for (cache_key, _), category in zip(unique_entries.items(), categories):
                    if category in self._get_valid_categories():
                        self.classification_cache[cache_key] = category
                        classifications[cache_key] = category
                    else:
                        logger.warning(f"Invalid category received: {category}")
                        classifications[cache_key] = self._fallback_classification(unique_entries[cache_key])
            except json.JSONDecodeError:
                logger.error("Failed to parse AI response as JSON")
                for cache_key, entry in unique_entries.items():
                    classifications[cache_key] = self._fallback_classification(entry)
        except Exception as e:
            logger.error(f"Batch AI classification failed: {e}")
            for cache_key, entry in unique_entries.items():
                classifications[cache_key] = self._fallback_classification(entry)

        return classifications

    def _get_valid_categories(self):
        return {
            'SHCP', 'RESV', 'LTBR', 'TRPY-N', 'OLTL', 'LTPR', 'STBR', 'TRPY-C', 'OCLI', 'STPR',
            'PPEA', 'INTG', 'CWIP', 'NCIN', 'DTAX', 'LTLA', 'TRDR-N', 'ONCA', 'CUIN', 'INVT',
            'TRDR-C', 'CASH', 'STLA', 'OCCA', 'REVO', 'OTHI', 'COGS', 'PURC', 'EMPB', 'DEPR',
            'FNCE', 'INPF', 'OTHE', 'EXIT', 'TAXE', 'CURT', 'DEFT'
        }

    def _fallback_classification(self, entry_data: Dict) -> str:
        primary_group = (entry_data['primary_group'] or '').lower()
        parent_group = (entry_data['parent_group'] or '').lower()
        if any(term in primary_group for term in ['sundry debtor', 'cash', 'bank']):
            return 'CASH'
        elif any(term in primary_group for term in ['fixed asset', 'property', 'plant']):
            return 'PPEA'
        elif 'capital' in primary_group:
            return 'SHCP'
        elif any(term in primary_group for term in ['salary', 'wage']):
            return 'EMPB'
        elif 'sales' in primary_group:
            return 'REVO'
        elif 'purchase' in primary_group:
            return 'PURC'
        return 'CASH'


class LedgerGLCodeAssignment:
    def __init__(self, db_params, openai_api_key):
        self.db_params = db_params
        self.conn = None
        self.cur = None
        self.ai_classifier = LedgerAIClassifier(openai_api_key)
        self.batch_size = 50
        self.category_ranges = {}
        self.gl_counters = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor()
            logger.info("Database connection established successfully")
        except Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def load_category_ranges(self):
        """Load category ranges from account_groups and categories tables."""
        try:
            query = """
            SELECT 
                ag.main_groups_code,
                ag.gl_code_start,
                ag.gl_code_end,
                c.category_name,
                ag.main_groups,
                ag.group_id
            FROM account_groups ag
            JOIN categories c ON ag.category_id = c.category_id
            """
            self.cur.execute(query)
            rows = self.cur.fetchall()
            self.category_ranges = {}
            for row in rows:
                group_code, gl_start, gl_end, category_name, group_name, group_id = row
                # Convert the 4-digit GL code range into a 6-digit range.
                start = int(str(gl_start) + '01')
                end = int(str(gl_end) + '99')
                self.category_ranges[group_code] = {
                    'start': start,
                    'end': end,
                    'category': category_name,
                    'group': group_name,
                    'group_id': group_id
                }
            logger.info("Category ranges loaded successfully from database")
        except Error as e:
            logger.error(f"Error loading category ranges: {e}")
            raise

    def create_gl_code_table(self):
        """Create the ledger_table_gl_code if it doesn't exist with the new column sequence."""
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS ledger_table_gl_code (
                gl_code VARCHAR(10),
                ledger_id INTEGER,
                ledger_name VARCHAR(255),
                category_name VARCHAR(255),
                account_group_name VARCHAR(255),
                primary_group VARCHAR(255),
                parent_group VARCHAR(255),
                group_parent VARCHAR(255),
                isupdate BOOLEAN DEFAULT false,
                group_name VARCHAR(255),
                is_revenue BOOLEAN,
                is_deemed_positive BOOLEAN,
                affects_gross_profit BOOLEAN
            )
            """
            self.cur.execute(create_table_query)
            self.conn.commit()
            logger.info("ledger_table_gl_code table checked/created successfully")
        except Error as e:
            logger.error(f"Error with ledger_table_gl_code creation: {e}")
            self.conn.rollback()
            raise

    def initialize_gl_counters(self):
        """Initialize GL counters based on existing entries."""
        try:
            if not self.category_ranges:
                self.load_category_ranges()
            self.gl_counters = {code: info['start'] for code, info in self.category_ranges.items()}
            for category_code in self.category_ranges.keys():
                query = """
                SELECT MAX(CAST(gl_code AS INTEGER))
                FROM ledger_table_gl_code
                WHERE CAST(gl_code AS INTEGER) BETWEEN %s AND %s
                """
                self.cur.execute(query, (
                    self.category_ranges[category_code]['start'],
                    self.category_ranges[category_code]['end']
                ))
                max_gl_code = self.cur.fetchone()[0]
                if max_gl_code:
                    self.gl_counters[category_code] = max_gl_code + 1
            logger.info("GL counters initialized successfully")
        except Error as e:
            logger.error(f"Error initializing GL counters: {e}")
            raise

    def get_unprocessed_entries(self):
        """
        Fetch entries from ledger_table that haven't been processed yet.
        This function assumes that ledger_table is already present in the database.
        """
        try:
            query = """
            SELECT lt.*
            FROM ledger_table lt
            LEFT JOIN ledger_table_gl_code gl ON lt.ledger_id = gl.ledger_id
            WHERE gl.ledger_id IS NULL
            """
            self.cur.execute(query)
            return self.cur.fetchall()
        except Error as e:
            logger.error(f"Error fetching unprocessed entries: {e}")
            raise

    def get_next_gl_code(self, category_code):
        """Get the next available GL code for a category as a 6-digit string."""
        current_value = self.gl_counters[category_code]
        if current_value > self.category_ranges[category_code]['end']:
            raise ValueError(f"GL code range exceeded for category {category_code}")
        gl_code = f"{current_value:06d}"
        self.gl_counters[category_code] += 1
        return gl_code

    def process_ledger_entries(self):
        """Process new ledger entries in batches and insert them into ledger_table_gl_code."""
        try:
            self.initialize_gl_counters()
            unprocessed_entries = self.get_unprocessed_entries()
            total_entries = len(unprocessed_entries)
            if total_entries == 0:
                logger.info("No new entries to process")
                return
            processed_count = 0
            logger.info(f"Found {total_entries} new entries to process")
            for i in range(0, total_entries, self.batch_size):
                batch = unprocessed_entries[i:i + self.batch_size]
                batch_entries = []
                for entry in batch:
                    entry_dict = {
                        'ledger_id': entry[0],
                        'ledger_name': entry[1],
                        'parent_group': entry[2],
                        'primary_group': entry[3],
                        'group_name': entry[4],
                        'group_parent': entry[5]
                    }
                    batch_entries.append((entry, entry_dict))
                classifications = self.ai_classifier.batch_classify_entries(
                    [entry_dict for _, entry_dict in batch_entries]
                )
                for entry, entry_dict in batch_entries:
                    try:
                        cache_key = self.ai_classifier.get_cache_key(entry_dict)
                        category_code = classifications.get(cache_key, 'CASH')
                        gl_code = self.get_next_gl_code(category_code)
                        category_info = self.category_ranges[category_code]
                        params = {
                            'gl_code': gl_code,
                            'ledger_id': entry[0],
                            'ledger_name': entry[1],
                            'category_name': category_info['category'],
                            'account_group_name': category_info['group'],
                            'primary_group': entry[3],
                            'parent_group': entry[2],
                            'group_parent': entry[5],
                            'isupdate': False,
                            'group_name': entry[4],
                            'is_revenue': entry[6],
                            'is_deemed_positive': entry[7],
                            'affects_gross_profit': entry[8]
                        }
                        insert_query = """
                        INSERT INTO ledger_table_gl_code (
                            gl_code, ledger_id, ledger_name, category_name, account_group_name, primary_group, parent_group, group_parent, isupdate, group_name, is_revenue, is_deemed_positive, affects_gross_profit
                        ) VALUES (
                            %(gl_code)s, %(ledger_id)s, %(ledger_name)s, %(category_name)s, %(account_group_name)s, %(primary_group)s, %(parent_group)s, %(group_parent)s, %(isupdate)s, %(group_name)s, %(is_revenue)s, %(is_deemed_positive)s, %(affects_gross_profit)s
                        )
                        """
                        self.cur.execute(insert_query, params)
                        processed_count += 1
                        if processed_count % 10 == 0:
                            logger.info(f"Processed {processed_count}/{total_entries} entries")
                    except Exception as e:
                        logger.error(f"Error processing entry {entry[0]}: {e}")
                        continue
                self.conn.commit()
                logger.info(f"Committed batch of {len(batch)} entries")
            logger.info(f"Successfully processed all {total_entries} new entries")
        except Error as e:
            logger.error(f"Database error: {e}")
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self.conn.rollback()
            raise

    def process_updated_entries(self):
        """
        Process ledger entries from ledger_table where isupdate is TRUE.
        For each such entry:
          - Re-classify the entry using the AI classifier.
          - Compare the following fields from ledger_table and ledger_table_gl_code:
                ledger_name, parent_group, primary_group, group_name, group_parent,
                is_revenue, is_deemed_positive, affects_gross_profit
          - Also compare the classification (account_group_name and category_name).
          - If any of these fields have changed, generate a new GL code and update the record in ledger_table_gl_code.
          - In all cases, update isupdate to false in both tables.
        """
        try:
            query = """
            SELECT *
            FROM ledger_table
            WHERE isupdate = TRUE
            """
            self.cur.execute(query)
            updated_entries = self.cur.fetchall()
            if not updated_entries:
                logger.info("No updated entries to process")
                return
            logger.info(f"Found {len(updated_entries)} updated entries to process")
            batch_entries = []
            for entry in updated_entries:
                entry_dict = {
                    'ledger_id': entry[0],
                    'ledger_name': entry[1],
                    'parent_group': entry[2],
                    'primary_group': entry[3],
                    'group_name': entry[4],
                    'group_parent': entry[5]
                }
                batch_entries.append((entry, entry_dict))
            classifications = self.ai_classifier.batch_classify_entries(
                [entry_dict for _, entry_dict in batch_entries]
            )
            for entry, entry_dict in batch_entries:
                ledger_id = entry[0]
                cache_key = self.ai_classifier.get_cache_key(entry_dict)
                new_category_code = classifications.get(cache_key, 'CASH')
                new_category_info = self.category_ranges[new_category_code]
                # Fetch the existing record from ledger_table_gl_code (selecting needed fields)
                select_query = """
                    SELECT account_group_name, category_name, ledger_name, parent_group, primary_group, group_name, group_parent, is_revenue, is_deemed_positive, affects_gross_profit
                    FROM ledger_table_gl_code
                    WHERE ledger_id = %s
                """
                self.cur.execute(select_query, (ledger_id,))
                result = self.cur.fetchone()
                # Current values from ledger_table
                current_values = {
                    'ledger_name': entry[1],
                    'parent_group': entry[2],
                    'primary_group': entry[3],
                    'group_name': entry[4],
                    'group_parent': entry[5],
                    'is_revenue': entry[6],
                    'is_deemed_positive': entry[7],
                    'affects_gross_profit': entry[8]
                }
                update_required = False
                if result:
                    existing_values = {
                        'account_group_name': result[0],
                        'category_name': result[1],
                        'ledger_name': result[2],
                        'parent_group': result[3],
                        'primary_group': result[4],
                        'group_name': result[5],
                        'group_parent': result[6],
                        'is_revenue': result[7],
                        'is_deemed_positive': result[8],
                        'affects_gross_profit': result[9]
                    }
                    # Check if classification changed
                    if (existing_values['account_group_name'] != new_category_info['group'] or 
                        existing_values['category_name'] != new_category_info['category']):
                        update_required = True
                    # Check if any of the critical fields have changed
                    for field in ['ledger_name', 'parent_group', 'primary_group', 'group_name', 'group_parent', 'is_revenue', 'is_deemed_positive', 'affects_gross_profit']:
                        if str(existing_values.get(field)) != str(current_values.get(field)):
                            update_required = True
                            break
                    if update_required:
                        try:
                            new_gl_code = self.get_next_gl_code(new_category_code)
                        except Exception as e:
                            logger.error(f"Error generating new GL code for ledger_id {ledger_id}: {e}")
                            continue
                        update_query = """
                            UPDATE ledger_table_gl_code
                            SET gl_code = %s, account_group_name = %s, category_name = %s,
                                ledger_name = %s, primary_group = %s, parent_group = %s, group_parent = %s,
                                isupdate = false, group_name = %s, is_revenue = %s, is_deemed_positive = %s, affects_gross_profit = %s
                            WHERE ledger_id = %s
                        """
                        self.cur.execute(update_query, (
                            new_gl_code,
                            new_category_info['group'],
                            new_category_info['category'],
                            current_values['ledger_name'],
                            current_values['primary_group'],
                            current_values['parent_group'],
                            current_values['group_parent'],
                            current_values['group_name'],
                            current_values['is_revenue'],
                            current_values['is_deemed_positive'],
                            current_values['affects_gross_profit'],
                            ledger_id
                        ))
                        logger.info(f"Updated ledger {ledger_id} with new GL code and updated fields.")
                    else:
                        logger.info(f"Ledger {ledger_id} has no changes in critical fields. No update needed.")
                else:
                    # No record exists: insert new record into ledger_table_gl_code
                    logger.warning(f"Updated ledger {ledger_id} not found in ledger_table_gl_code. Inserting new record.")
                    try:
                        new_gl_code = self.get_next_gl_code(new_category_code)
                    except Exception as e:
                        logger.error(f"Error generating new GL code for ledger_id {ledger_id}: {e}")
                        continue
                    params = {
                        'gl_code': new_gl_code,
                        'ledger_id': entry[0],
                        'ledger_name': entry[1],
                        'category_name': new_category_info['category'],
                        'account_group_name': new_category_info['group'],
                        'primary_group': entry[3],
                        'parent_group': entry[2],
                        'group_parent': entry[5],
                        'isupdate': False,
                        'group_name': entry[4],
                        'is_revenue': entry[6],
                        'is_deemed_positive': entry[7],
                        'affects_gross_profit': entry[8]
                    }
                    insert_query = """
                        INSERT INTO ledger_table_gl_code (
                            gl_code, ledger_id, ledger_name, category_name, account_group_name, primary_group, parent_group, group_parent, isupdate, group_name, is_revenue, is_deemed_positive, affects_gross_profit
                        ) VALUES (
                            %(gl_code)s, %(ledger_id)s, %(ledger_name)s, %(category_name)s, %(account_group_name)s, %(primary_group)s, %(parent_group)s, %(group_parent)s, %(isupdate)s, %(group_name)s, %(is_revenue)s, %(is_deemed_positive)s, %(affects_gross_profit)s
                        )
                    """
                    self.cur.execute(insert_query, params)
                # In all cases, update the isupdate flag in ledger_table to false.
                update_ledger_query = """
                    UPDATE ledger_table
                    SET isupdate = false
                    WHERE ledger_id = %s
                """
                self.cur.execute(update_ledger_query, (ledger_id,))
            self.conn.commit()
            logger.info("Processed all updated entries successfully")
        except Error as e:
            logger.error(f"Database error in process_updated_entries: {e}")
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error(f"Unexpected error in process_updated_entries: {e}")
            self.conn.rollback()
            raise

    def close_connection(self):
        """Clean up database connections."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def main(session_data):
    """Main function to execute the GL code assignment process."""
    db_params = {
        'dbname': f"user_{session_data['userId']}_db",
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    openai_api_key = os.getenv('OPENAI_API_KEY')

    processor = LedgerGLCodeAssignment(db_params, openai_api_key)
    try:
        processor.connect()
        # Here we assume that ledger_table is already present in the database.
        # If ledger_table does not exist, the get_unprocessed_entries() call will error out.
        processor.load_category_ranges()
        processor.create_gl_code_table()
        processor.process_ledger_entries()
        # Process entries that have been updated (isupdate = TRUE)
        processor.process_updated_entries()
        logger.info("GL code assignment completed successfully")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        processor.close_connection()


if __name__ == "__main__":
    # Parse command-line arguments in the format key=value
    session_data = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            session_data[key] = value

    if 'userId' not in session_data:
        logger.error("Missing required argument: userId")
        sys.exit(1)

    main(session_data)