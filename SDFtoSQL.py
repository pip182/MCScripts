import os
import re
import subprocess
import logging
import shutil
import glob
import sqlite3

# === CONFIGURATION ===
EXPORT_TOOL_PATH = r"C:\Users\JuicyJerry\Dev\Microvellum\ExportSqlCe40.exe"
SEARCH_DIR = r"M:\Homestead_Library\Work Orders"
OUTPUT_DIR = r"D:\My Documents\TEST\SQLFiles"
TEMP_DIR = r"D:\My Documents\TEST\SQLFiles\_temp_work"

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def sanitize_filename(name):
    """Sanitize filenames and paths for safe usage on Windows."""
    return re.sub(r'[<>:"/\\|?*()#&\'!]', '_', name).strip()


def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    os.makedirs(directory, exist_ok=True)


def clean_temp_dir():
    """Clean up temporary directory"""
    if os.path.exists(TEMP_DIR):
        for file in glob.glob(os.path.join(TEMP_DIR, "*")):
            try:
                if os.path.isfile(file):
                    os.remove(file)
            except Exception as e:
                logging.warning(f"Failed to remove temp file {file}: {e}")


def fix_sql_value(value):
    """Fix SQL Server CE values to be compatible with SQLite"""
    # Handle NULL values
    if value.upper() == 'NULL':
        return 'NULL'

    # Handle numeric values (don't quote them)
    if re.match(r'^-?\d+(\.\d+)?$', value):
        return value

    # Handle bit/boolean values
    if value in ('0', '1'):
        return value

    # Handle datetime values - keep them as strings but ensure proper quoting
    if re.match(r"N?'[\d\-\s:/]+'", value):
        # Remove N prefix for national character string literals
        value = re.sub(r"^N'(.+)'$", r"'\1'", value)
        return value

    # Handle string values - ensure proper quoting
    if value.startswith("'") and value.endswith("'"):
        # Already quoted, but escape any internal single quotes
        inner_value = value[1:-1].replace("'", "''")
        return f"'{inner_value}'"

    # For unquoted values, add quotes and escape
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'"


def process_insert_statement(statement, cursor):
    """Process a single INSERT statement with better handling of values"""
    try:
        # First, let's log a sample of the original statement for debugging
        logging.debug(f"Processing INSERT: {statement[:200]}...")

        # Clean up the statement - remove line breaks and extra spaces
        statement = re.sub(r'\s+', ' ', statement).strip()

        # Extract table name
        table_match = re.search(r'INSERT\s+INTO\s+(?:\[\w+\]\.)?\[([^\]]+)\]', statement, re.IGNORECASE)
        if not table_match:
            return False, f"Could not identify table name: {statement[:100]}..."

        table_name = table_match.group(1).strip()

        # Extract columns part
        columns_match = re.search(r'INSERT\s+INTO\s+(?:\[\w+\]\.)?\[[^\]]+\]\s*\(([^)]+)\)', statement, re.IGNORECASE)
        if not columns_match:
            # Try to handle INSERT without column specifications
            if 'VALUES' in statement.upper():
                columns = '*'  # All columns
            else:
                return False, f"Could not parse columns and no VALUES keyword found: {statement[:100]}..."
        else:
            columns = columns_match.group(1).strip()

        # Extract VALUES part - more flexible approach
        values_match = re.search(r'VALUES\s*\((.*)\)', statement, re.IGNORECASE | re.DOTALL)
        if not values_match:
            return False, f"Could not find VALUES part: {statement[:100]}..."

        values_text = values_match.group(1).strip()

        # For debugging
        logging.debug(f"Extracted - Table: {table_name}, Columns: {columns}, Values: {values_text[:50]}...")

        # Clean up column names - replace [Name] with "Name"
        if columns != '*':
            columns = re.sub(r'\[([^\]]+)\]', r'"\1"', columns)

        # We need to parse the values respecting quoted strings and nested levels
        values = []
        current_value = ""
        in_string = False
        quote_char = None  # Track what kind of quote we're in
        bracket_level = 0

        i = 0
        while i < len(values_text):
            char = values_text[i]

            # Handle quotes
            if char in ("'", '"') and (i == 0 or values_text[i-1] != '\\'):
                if not in_string:
                    # Starting a string
                    in_string = True
                    quote_char = char
                    current_value += char
                elif char == quote_char:
                    # Check for escaped quotes (double quotes)
                    if i + 1 < len(values_text) and values_text[i + 1] == char:
                        # This is an escaped quote, not the end of the string
                        current_value += char * 2
                        i += 1  # Skip the next quote
                    else:
                        # End of string
                        current_value += char
                        in_string = False
                else:
                    # Different quote character inside a string
                    current_value += char

            # Handle N prefix for strings (N'string')
            elif char == 'N' and i + 1 < len(values_text) and values_text[i + 1] in ("'", '"') and not in_string:
                current_value += 'N'  # Include the N prefix

            # Handle brackets and parentheses
            elif char == '(' and not in_string:
                bracket_level += 1
                current_value += char
            elif char == ')' and not in_string:
                bracket_level -= 1
                current_value += char

            # Handle value separators
            elif char == ',' and not in_string and bracket_level == 0:
                values.append(current_value.strip())
                current_value = ""

            # Handle normal characters
            else:
                current_value += char

            i += 1

        # Add the last value if there's anything left
        if current_value:
            values.append(current_value.strip())

        # Fix each value for SQLite compatibility
        fixed_values = [fix_sql_value(val) for val in values]

        # For debugging - show how many values we extracted
        logging.debug(f"Extracted {len(values)} values")

        # Construct the final INSERT statement
        if columns == '*':
            insert_stmt = f'INSERT INTO "{table_name}" VALUES ({",".join(fixed_values)})'
        else:
            insert_stmt = f'INSERT INTO "{table_name}" ({columns}) VALUES ({",".join(fixed_values)})'

        # Execute the statement
        try:
            cursor.execute(insert_stmt)
            return True, None
        except sqlite3.Error as e:
            # Try to diagnose the issue better
            error_msg = str(e)
            logging.debug(f"SQL Error: {error_msg}")
            logging.debug(f"Problem statement: {insert_stmt[:200]}...")

            # If error indicates wrong number of values, log the details
            if "1 values for" in error_msg or "values for" in error_msg:
                logging.debug(f"Column count: {columns.count(',') + 1}, Value count: {len(fixed_values)}")

            return False, f"SQLite error: {e}\nStatement: {insert_stmt[:100]}..."

    except Exception as e:
        import traceback
        logging.debug(f"Error parsing INSERT: {e}\n{traceback.format_exc()}")
        return False, f"Error: {e}\nStatement: {statement[:100]}..."


def sql_to_sqlite(sql_files, sqlite_path):
    """Convert SQL files to a single SQLite database"""
    logging.info(f"Creating SQLite database: {sqlite_path}")

    # Remove existing database if it exists
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    # Connect to SQLite database
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Track overall statistics
    tables_created = 0
    rows_inserted = 0

    # First pass: Create all tables (schema)
    logging.info("First pass: Creating database schema...")
    for sql_file in sorted(sql_files):
        try:
            # Read file content with binary mode first to handle null bytes properly
            with open(sql_file, 'rb') as f:
                binary_content = f.read()

            # Remove null bytes and decode
            sql_content = binary_content.replace(b'\x00', b'').decode('utf-8', errors='ignore')

            # Extract and execute CREATE TABLE statements
            create_statements = re.findall(r'CREATE TABLE\s+[^;]+;', sql_content, re.IGNORECASE | re.DOTALL)

            for statement in create_statements:
                try:
                    # Fix SQL Server CE specific syntax
                    # Convert [dbo].[TableName] to "TableName"
                    statement = re.sub(r'\[\w+\]\.\[(\w+)\]', r'"\1"', statement)

                    # Replace other bracketed identifiers
                    statement = re.sub(r'\[([^\]]+)\]', r'"\1"', statement)

                    # Remove IDENTITY specifications
                    statement = re.sub(r'IDENTITY\(\d+,\s*\d+\)', '', statement)

                    # Change data types
                    statement = re.sub(r'NVARCHAR\(\d+\)', 'TEXT', statement)
                    statement = re.sub(r'VARCHAR\(\d+\)', 'TEXT', statement)
                    statement = re.sub(r'DATETIME', 'TEXT', statement)
                    statement = re.sub(r'DECIMAL\(\d+,\s*\d+\)', 'REAL', statement)
                    statement = re.sub(r'MONEY', 'REAL', statement)
                    statement = re.sub(r'BIT', 'INTEGER', statement)
                    statement = re.sub(r'IMAGE', 'BLOB', statement)

                    cursor.execute(statement)
                    tables_created += 1

                except sqlite3.Error as e:
                    logging.warning(f"Error creating table: {e}\nStatement: {statement[:150]}...")

        except Exception as e:
            logging.error(f"Error processing schema in {os.path.basename(sql_file)}: {e}")

    # Diagnostic: Analyze the SQL file structure directly
    logging.info(f"Schema created successfully. Created {tables_created} tables.")
    logging.info("Analyzing INSERT statements format in SQL files...")

    # Before second pass, let's analyze some representative statements
    for sql_file in sorted(sql_files):
        with open(sql_file, 'rb') as f:
            binary_content = f.read()
            sql_content = binary_content.replace(b'\x00', b'').decode('utf-8', errors='ignore')

        # Find a few sample INSERT statements for debugging
        inserts = []
        for match in re.finditer(r'INSERT\s+INTO.*?;', sql_content, re.IGNORECASE | re.DOTALL):
            inserts.append(match.group(0))
            if len(inserts) >= 3:  # Just get a few examples
                break

        # Log samples of the first few INSERT statements
        logging.debug(f"File: {os.path.basename(sql_file)}")
        for i, insert in enumerate(inserts):
            # Clean up for logging
            cleaned = re.sub(r'\s+', ' ', insert)
            logging.debug(f"Sample INSERT #{i+1}: {cleaned[:200]}...")

            # Try to determine structure
            parts = re.search(r'INSERT\s+INTO\s+(?:\[\w+\]\.)?\[([^\]]+)\](?:\s*\(([^)]+)\))?\s*VALUES\s*\((.*?)(?:\)(?:\s*GO)?;|$)',
                             cleaned, re.IGNORECASE)

            if parts:
                table = parts.group(1)
                columns = parts.group(2) if parts.group(2) else "Default columns"
                values = parts.group(3)
                logging.debug(f"  Table: {table}")
                logging.debug(f"  Columns: {columns}")
                logging.debug(f"  Values (start): {values[:50]}...")
                logging.debug(f"  Values count: {values.count(',') + 1}")

                # Check for unbalanced quotes or parentheses
                quotes = values.count("'")
                parens = values.count('(') - values.count(')')
                if quotes % 2 != 0 or parens != 0:
                    logging.debug(f"  Warning: Unbalanced quotes ({quotes}) or parentheses ({parens})")

            else:
                logging.debug("  Could not parse statement structure")

        # Break after checking the first file with INSERT statements
        if inserts:
            break

    # Second pass: Using a more direct approach for INSERT statements
    logging.info("Second pass: Inserting data using direct SQL approach...")

    # First, let's prepare tables by finding all column names
    table_columns = {}
    for table_info in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        table_name = table_info[0]
        columns = []
        for column_info in cursor.execute(f'PRAGMA table_info("{table_name}")').fetchall():
            columns.append(column_info[1])  # Column name
        table_columns[table_name] = columns
        logging.debug(f"Table '{table_name}' has {len(columns)} columns: {', '.join(columns)}")

    # Process INSERT statements - simplified direct approach
    for sql_file in sorted(sql_files):
        try:
            with open(sql_file, 'rb') as f:
                binary_content = f.read()
                sql_content = binary_content.replace(b'\x00', b'').decode('utf-8', errors='ignore')

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            # Split into statements
            statements = re.split(r';[\s\n]*', sql_content)

            inserts_count = 0
            for statement in statements:
                statement = statement.strip()

                # Only process INSERT statements
                if not statement.upper().startswith('INSERT INTO'):
                    continue

                # Try a simplified approach: extract key parts first
                try:
                    # Convert brackets to quotes for SQLite compatibility
                    statement = re.sub(r'\[\w+\]\.\[(\w+)\]', r'"\1"', statement)
                    statement = re.sub(r'\[([^\]]+)\]', r'"\1"', statement)

                    # Find table name
                    table_match = re.search(r'INSERT\s+INTO\s+"([^"]+)"', statement, re.IGNORECASE)
                    if not table_match:
                        continue

                    table_name = table_match.group(1)

                    # Check if this table exists in our schema
                    if table_name not in table_columns:
                        logging.debug(f"Table '{table_name}' not found in schema, skipping")
                        continue

                    # Find VALUES clause
                    values_match = re.search(r'VALUES\s*\((.*?)\)(?:\s*GO)?$', statement, re.IGNORECASE | re.DOTALL)
                    if not values_match:
                        logging.debug(f"Couldn't extract values from: {statement[:100]}...")
                        continue

                    # Execute the cleaned up INSERT statement
                    cleaned_stmt = statement.replace('\n', ' ').replace('\r', '')

                    # Add semicolon at the end if needed
                    if not cleaned_stmt.rstrip().endswith(';'):
                        cleaned_stmt += ';'

                    cursor.execute(cleaned_stmt)
                    inserts_count += 1
                    rows_inserted += 1

                except sqlite3.Error as e:
                    logging.debug(f"SQLite error inserting: {e}\nStatement: {statement[:100]}...")
                except Exception as e:
                    logging.debug(f"General error inserting: {e}\nStatement: {statement[:100]}...")

            # Commit transaction
            conn.commit()
            logging.info(f"Inserted {inserts_count} rows from {os.path.basename(sql_file)}")

        except Exception as e:
            # Rollback on error
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

            logging.error(f"Error processing file {os.path.basename(sql_file)}: {e}")
            import traceback
            logging.error(traceback.format_exc())

    # Check database stats
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    for table in tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{table[0]}"')
            count = cursor.fetchone()[0]
            logging.info(f"Table '{table[0]}': {count} rows")
        except sqlite3.Error as e:
            logging.warning(f"Error counting rows in table '{table[0]}': {e}")

    # Commit any remaining changes and close connection
    conn.commit()
    conn.close()

    logging.info(f"SQLite conversion complete: {tables_created} tables, {rows_inserted} rows inserted")
    return True


def convert_sdf_to_sql(sdf_path, output_dir):
    """Convert SDF file to SQL using ExportSQLCE40.exe and then to SQLite"""
    file_name = os.path.basename(sdf_path)
    parent_dir = os.path.basename(os.path.dirname(sdf_path))

    # Create output directory using parent folder name of the SDF file
    target_dir = os.path.join(output_dir, parent_dir)
    ensure_dir(target_dir)

    # Clean and prepare temp directory
    ensure_dir(TEMP_DIR)
    clean_temp_dir()

    # Prepare temp files
    temp_sdf = os.path.join(TEMP_DIR, "input.sdf")

    try:
        # Copy SDF to temp directory
        logging.info(f"Processing: {sdf_path}")
        shutil.copyfile(sdf_path, temp_sdf)

        # Run conversion tool
        logging.info("Running SQL export tool...")
        result = subprocess.run([
            EXPORT_TOOL_PATH,
            'Data Source=input.sdf',
            'output.sql'
        ], cwd=TEMP_DIR, capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"Export failed: {result.stderr}")
            return False

        # Find output files
        sql_files = glob.glob(os.path.join(TEMP_DIR, "output*.sql"))
        if not sql_files:
            logging.error("No SQL files were generated")
            return False

        # Save SQL files to target directory for reference
        for sql_file in sql_files:
            sql_name = os.path.basename(sql_file)
            target_path = os.path.join(target_dir, sql_name)
            shutil.copyfile(sql_file, target_path)

        # Create SQLite database
        db_name = f"{parent_dir}.db"
        sqlite_path = os.path.join(target_dir, db_name)

        # Convert SQL files to SQLite
        if sql_to_sqlite(sql_files, sqlite_path):
            logging.info(f"✓ Created SQLite database: {sqlite_path}")

        logging.info(f"✓ Converted {file_name} -> {target_dir}")
        return True

    except Exception as e:
        logging.error(f"Error processing {sdf_path}: {e}")
        return False
    finally:
        # Clean up temp files
        clean_temp_dir()


def main():
    logging.info("Starting SDF to SQL/SQLite conversion")
    logging.info(f"Searching for SDF files in: {SEARCH_DIR}")

    # Check if search directory exists
    if not os.path.exists(SEARCH_DIR):
        logging.error(f"Search directory not found: {SEARCH_DIR}")
        return

    # Ensure output directory exists
    ensure_dir(OUTPUT_DIR)

    count = 0
    successful = 0

    # Find and process all SDF files
    for root, _, files in os.walk(SEARCH_DIR):
        for file in files:
            if file.lower().endswith('.sdf'):
                count += 1
                sdf_path = os.path.join(root, file)

                if convert_sdf_to_sql(sdf_path, OUTPUT_DIR):
                    successful += 1

    logging.info(f"Conversion complete: {successful} of {count} files processed successfully")


if __name__ == "__main__":
    main()
