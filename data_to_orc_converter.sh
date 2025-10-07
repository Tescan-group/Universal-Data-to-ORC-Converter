#!/bin/bash

# Universal Data to ORC Converter
# Supports: MySQL, SQL Dumps, CSV Files
# Cross-platform: macOS & Linux

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/data_to_orc_config.conf"
LOG_FILE="${SCRIPT_DIR}/data_to_orc.log"

# Logging functions
log() { echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"; }
warn() { echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"; }
error() { echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"; exit 1; }

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Darwin*) OS="macOS";;
        Linux*) OS="Linux";;
        *) OS="UNKNOWN";;
    esac
    log "Detected OS: $OS"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        warn "Python3 not found. Installing..."
        install_python
    fi
    
    # Check Java (for Spark)
    if ! command -v java &> /dev/null; then
        warn "Java not found. Installing..."
        install_java
    fi
    
    install_python_deps
}

# Installation functions
install_python() {
    if [ "$OS" = "macOS" ]; then
        if ! command -v brew &> /dev/null; then
            error "Homebrew not installed. Please install from https://brew.sh"
        fi
        brew install python3
    else
        sudo apt update && sudo apt install -y python3 python3-pip
    fi
}

install_java() {
    if [ "$OS" = "macOS" ]; then
        brew install openjdk
    else
        sudo apt update && sudo apt install -y openjdk-11-jdk
    fi
}

install_python_deps() {
    log "Installing Python dependencies..."
    pip3 install pandas pyarrow mysql-connector-python pyorc || {
        warn "Some dependencies failed. Installing basic set..."
        pip3 install pandas pyarrow
    }
}

# Get conversion type
get_conversion_type() {
    echo
    log "Select input data type:"
    echo "1) MySQL Database"
    echo "2) SQL Dump File"
    echo "3) CSV Files"
    read -p "Enter choice [1]: " DATA_TYPE
    DATA_TYPE=${DATA_TYPE:-1}
}

# MySQL configuration
get_mysql_config() {
    echo
    log "MySQL Database Configuration"
    read -p "MySQL Host [localhost]: " MYSQL_HOST
    MYSQL_HOST=${MYSQL_HOST:-localhost}
    
    read -p "MySQL Port [3306]: " MYSQL_PORT
    MYSQL_PORT=${MYSQL_PORT:-3306}
    
    read -p "MySQL Username: " MYSQL_USER
    read -s -p "MySQL Password: " MYSQL_PASS
    echo
    
    read -p "Database Name: " MYSQL_DB
    
    echo
    read -p "Export all tables? [y/N]: " EXPORT_ALL
    EXPORT_ALL=${EXPORT_ALL:-n}
    
    if [ "$EXPORT_ALL" = "y" ] || [ "$EXPORT_ALL" = "Y" ]; then
        TABLE_LIST="all"
    else
        read -p "Table names (comma-separated): " TABLE_LIST
    fi
}

# SQL Dump configuration
get_sql_dump_config() {
    echo
    log "SQL Dump Configuration"
    read -p "Path to SQL dump file: " SQL_DUMP_FILE
    
    if [ ! -f "$SQL_DUMP_FILE" ]; then
        error "SQL dump file not found: $SQL_DUMP_FILE"
    fi
    
    read -p "Extract specific tables? [y/N]: " EXTRACT_SPECIFIC
    EXTRACT_SPECIFIC=${EXTRACT_SPECIFIC:-n}
    
    if [ "$EXTRACT_SPECIFIC" = "y" ] || [ "$EXTRACT_SPECIFIC" = "Y" ]; then
        read -p "Table names (comma-separated): " SQL_TABLES
    else
        SQL_TABLES="all"
    fi
}

# CSV configuration
get_csv_config() {
    echo
    log "CSV Files Configuration"
    read -p "CSV file or directory path: " CSV_PATH
    
    if [ ! -e "$CSV_PATH" ]; then
        error "CSV path not found: $CSV_PATH"
    fi
    
    read -p "CSV delimiter [,]: " CSV_DELIMITER
    CSV_DELIMITER=${CSV_DELIMITER:-,}
    
    read -p "Has header row? [Y/n]: " CSV_HEADER
    CSV_HEADER=${CSV_HEADER:-y}
    
    read -p "Infer schema from data? [Y/n]: " INFER_SCHEMA
    INFER_SCHEMA=${INFER_SCHEMA:-y}
}

# Common output configuration
get_output_config() {
    echo
    log "Output Configuration"
    read -p "Output directory [./orc_output]: " OUTPUT_DIR
    OUTPUT_DIR=${OUTPUT_DIR:-./orc_output}
    
    read -p "Compression [snappy]: " COMPRESSION
    COMPRESSION=${COMPRESSION:-snappy}
    
    read -p "Parallel workers [4]: " PARALLEL_WORKERS
    PARALLEL_WORKERS=${PARALLEL_WORKERS:-4}
}

# Create MySQL to ORC converter
create_mysql_converter() {
    cat > "${SCRIPT_DIR}/mysql_to_orc_converter.py" << 'EOF'
#!/usr/bin/env python3
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLToORCConverter:
    def __init__(self, host, port, user, password, database, output_dir, compression='snappy', chunk_size=50000):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.output_dir = output_dir
        self.compression = compression
        self.chunk_size = chunk_size
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host, port=self.port, user=self.user,
                password=self.password, database=self.database, charset='utf8mb4'
            )
            logger.info(f"Connected to MySQL database: {self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def get_table_row_count(self, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def export_table_to_orc(self, table_name):
        try:
            logger.info(f"Starting export of table: {table_name}")
            
            # Create table directory
            table_dir = os.path.join(self.output_dir, table_name)
            os.makedirs(table_dir, exist_ok=True)
            
            total_rows = self.get_table_row_count(table_name)
            logger.info(f"Table {table_name} has {total_rows:,} rows")
            
            offset = 0
            part_num = 0
            
            while offset < total_rows:
                query = f"SELECT * FROM `{table_name}` LIMIT {self.chunk_size} OFFSET {offset}"
                chunk = pd.read_sql(query, self.connection)
                
                if chunk.empty:
                    break
                
                # Convert to PyArrow Table and write as ORC
                arrow_table = pa.Table.from_pandas(chunk)
                output_file = os.path.join(table_dir, f"part-{part_num:05d}.orc")
                orc.write_table(arrow_table, output_file, compression=self.compression)
                
                offset += len(chunk)
                part_num += 1
                logger.info(f"Progress {table_name}: {min(offset, total_rows):,}/{total_rows:,} rows")
            
            logger.info(f"Successfully exported {table_name} to {table_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export table {table_name}: {e}")
            return False
    
    def export_tables_parallel(self, tables, max_workers=4):
        self.connect()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self.export_table_to_orc, table): table 
                for table in tables
            }
            
            success_count = 0
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    logger.error(f"Table {table} generated exception: {e}")
            
            logger.info(f"Export completed: {success_count}/{len(tables)} tables successful")
        
        if self.connection:
            self.connection.close()

def main():
    if len(sys.argv) != 9:
        print("Usage: python3 mysql_to_orc_converter.py <host> <port> <user> <password> <database> <output_dir> <compression> <tables>")
        sys.exit(1)
    
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    database, output_dir, compression, tables = sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8].split(',')
    
    converter = MySQLToORCConverter(host, port, user, password, database, output_dir, compression)
    converter.export_tables_parallel(tables, max_workers=4)

if __name__ == "__main__":
    main()
EOF
    chmod +x "${SCRIPT_DIR}/mysql_to_orc_converter.py"
}

# Create SQL dump to ORC converter
create_sql_dump_converter() {
    cat > "${SCRIPT_DIR}/sql_dump_to_orc_converter.py" << 'EOF'
#!/usr/bin/env python3
import re
import os
import sys
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLDumpToORCConverter:
    def __init__(self, sql_file, output_dir, compression='snappy', target_tables=None):
        self.sql_file = sql_file
        self.output_dir = output_dir
        self.compression = compression
        self.target_tables = target_tables or []
        os.makedirs(output_dir, exist_ok=True)
    
    def parse_sql_dump(self):
        """Parse SQL dump and extract table data"""
        logger.info(f"Reading SQL dump: {self.sql_file}")
        
        with open(self.sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Find CREATE TABLE statements to get table names
        table_pattern = r'CREATE TABLE `?(\w+)`?'
        tables = re.findall(table_pattern, content, re.IGNORECASE)
        
        if self.target_tables and 'all' not in self.target_tables:
            tables = [t for t in tables if t in self.target_tables]
        
        logger.info(f"Found tables: {tables}")
        return tables
    
    def extract_table_data(self, table_name, content):
        """Extract INSERT statements for a specific table"""
        logger.info(f"Extracting data for table: {table_name}")
        
        # Pattern to match INSERT statements for this table
        insert_pattern = rf'INSERT INTO `?{table_name}`?[^;]+;'
        insert_matches = re.findall(insert_pattern, content, re.IGNORECASE | re.MULTILINE)
        
        if not insert_matches:
            logger.warning(f"No INSERT statements found for table: {table_name}")
            return None
        
        all_data = []
        for insert_stmt in insert_matches:
            # Extract values part
            values_match = re.search(r'VALUES\s*(\([^;]+\))', insert_stmt, re.IGNORECASE)
            if values_match:
                values_str = values_match.group(1)
                # Simple parsing - this is a basic implementation
                rows = re.findall(r'\([^)]+\)', values_str)
                for row in rows:
                    # Remove parentheses and split by commas
                    row_data = row[1:-1].split(',')
                    # Clean up data
                    row_data = [cell.strip().strip("'") for cell in row_data]
                    all_data.append(row_data)
        
        return all_data
    
    def convert_to_orc(self):
        """Main conversion method"""
        tables = self.parse_sql_dump()
        
        with open(self.sql_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        success_count = 0
        for table in tables:
            try:
                logger.info(f"Processing table: {table}")
                data = self.extract_table_data(table, content)
                
                if data and len(data) > 0:
                    # Create DataFrame (you might need to adjust column names)
                    df = pd.DataFrame(data)
                    
                    # Create table directory
                    table_dir = os.path.join(self.output_dir, table)
                    os.makedirs(table_dir, exist_ok=True)
                    
                    # Convert to ORC
                    arrow_table = pa.Table.from_pandas(df)
                    output_file = os.path.join(table_dir, "data.orc")
                    orc.write_table(arrow_table, output_file, compression=self.compression)
                    
                    logger.info(f"Successfully converted {table} with {len(data)} rows")
                    success_count += 1
                else:
                    logger.warning(f"No data found for table: {table}")
                    
            except Exception as e:
                logger.error(f"Failed to process table {table}: {e}")
        
        logger.info(f"Conversion completed: {success_count}/{len(tables)} tables successful")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 sql_dump_to_orc_converter.py <sql_file> <output_dir> [compression] [tables]")
        print("Example: python3 sql_dump_to_orc_converter.py dump.sql ./output snappy users,products")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    output_dir = sys.argv[2]
    compression = sys.argv[3] if len(sys.argv) > 3 else 'snappy'
    tables = sys.argv[4].split(',') if len(sys.argv) > 4 else ['all']
    
    converter = SQLDumpToORCConverter(sql_file, output_dir, compression, tables)
    converter.convert_to_orc()

if __name__ == "__main__":
    main()
EOF
    chmod +x "${SCRIPT_DIR}/sql_dump_to_orc_converter.py"
}

# Create CSV to ORC converter
create_csv_converter() {
    cat > "${SCRIPT_DIR}/csv_to_orc_converter.py" << 'EOF'
#!/usr/bin/env python3
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import os
import sys
import glob
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVToORCConverter:
    def __init__(self, csv_path, output_dir, compression='snappy', delimiter=',', has_header=True, infer_schema=True):
        self.csv_path = csv_path
        self.output_dir = output_dir
        self.compression = compression
        self.delimiter = delimiter
        self.has_header = has_header
        self.infer_schema = infer_schema
        os.makedirs(output_dir, exist_ok=True)
    
    def get_csv_files(self):
        """Get list of CSV files to process"""
        if os.path.isfile(self.csv_path):
            return [self.csv_path]
        elif os.path.isdir(self.csv_path):
            return glob.glob(os.path.join(self.csv_path, "*.csv"))
        else:
            raise ValueError(f"CSV path not found: {self.csv_path}")
    
    def convert_csv_file(self, csv_file):
        """Convert a single CSV file to ORC"""
        try:
            logger.info(f"Converting CSV file: {csv_file}")
            
            # Get base filename for output
            base_name = os.path.splitext(os.path.basename(csv_file))[0]
            output_file = os.path.join(self.output_dir, f"{base_name}.orc")
            
            # Read CSV with appropriate parameters
            if self.has_header:
                df = pd.read_csv(csv_file, delimiter=self.delimiter)
            else:
                df = pd.read_csv(csv_file, delimiter=self.delimiter, header=None)
            
            logger.info(f"Read {len(df):,} rows from {csv_file}")
            
            # Convert to ORC
            arrow_table = pa.Table.from_pandas(df)
            orc.write_table(arrow_table, output_file, compression=self.compression)
            
            logger.info(f"Successfully converted to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to convert {csv_file}: {e}")
            return False
    
    def convert_all(self, max_workers=4):
        """Convert all CSV files"""
        csv_files = self.get_csv_files()
        logger.info(f"Found {len(csv_files)} CSV files to convert")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self.convert_csv_file, csv_file): csv_file 
                for csv_file in csv_files
            }
            
            success_count = 0
            for future in as_completed(future_to_file):
                csv_file = future_to_file[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    logger.error(f"File {csv_file} generated exception: {e}")
            
            logger.info(f"Conversion completed: {success_count}/{len(csv_files)} files successful")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 csv_to_orc_converter.py <csv_path> <output_dir> [delimiter] [has_header] [compression]")
        print("Example: python3 csv_to_orc_converter.py ./data ./output , true snappy")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    output_dir = sys.argv[2]
    delimiter = sys.argv[3] if len(sys.argv) > 3 else ','
    has_header = sys.argv[4].lower() == 'true' if len(sys.argv) > 4 else True
    compression = sys.argv[5] if len(sys.argv) > 5 else 'snappy'
    
    converter = CSVToORCConverter(csv_path, output_dir, compression, delimiter, has_header)
    converter.convert_all(max_workers=4)

if __name__ == "__main__":
    main()
EOF
    chmod +x "${SCRIPT_DIR}/csv_to_orc_converter.py"
}

# Perform conversion based on type
perform_conversion() {
    log "Starting conversion..."
    
    case $DATA_TYPE in
        1) # MySQL
            TABLE_STRING=$(echo $TABLES | tr ' ' ',')
            python3 "${SCRIPT_DIR}/mysql_to_orc_converter.py" \
                "$MYSQL_HOST" "$MYSQL_PORT" "$MYSQL_USER" "$MYSQL_PASS" \
                "$MYSQL_DB" "$OUTPUT_DIR" "$COMPRESSION" "$TABLE_STRING"
            ;;
        2) # SQL Dump
            TABLE_STRING=${SQL_TABLES:-"all"}
            python3 "${SCRIPT_DIR}/sql_dump_to_orc_converter.py" \
                "$SQL_DUMP_FILE" "$OUTPUT_DIR" "$COMPRESSION" "$TABLE_STRING"
            ;;
        3) # CSV
            HEADER_OPT="true"
            if [ "$CSV_HEADER" = "n" ] || [ "$CSV_HEADER" = "N" ]; then
                HEADER_OPT="false"
            fi
            python3 "${SCRIPT_DIR}/csv_to_orc_converter.py" \
                "$CSV_PATH" "$OUTPUT_DIR" "$CSV_DELIMITER" "$HEADER_OPT" "$COMPRESSION"
            ;;
    esac
    
    # Verify output
    if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A "$OUTPUT_DIR" 2>/dev/null)" ]; then
        log "Conversion completed successfully!"
        log "ORC files saved to: $OUTPUT_DIR"
        log "File structure:"
        find "$OUTPUT_DIR" -name "*.orc" | head -20
        if [ $(find "$OUTPUT_DIR" -name "*.orc" | wc -l) -gt 20 ]; then
            log "... and more files"
        fi
    else
        error "No ORC files were created. Check the log: $LOG_FILE"
    fi
}

# Get table list for MySQL
get_mysql_table_list() {
    if [ "$TABLE_LIST" = "all" ]; then
        log "Getting list of all tables..."
        TABLES=$(mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" -N -e "SHOW TABLES IN $MYSQL_DB" 2>/dev/null || {
            error "Cannot connect to MySQL or access database. Check credentials."
        })
    else
        TABLES=$(echo "$TABLE_LIST" | tr ',' ' ')
    fi
    log "Tables to export: $TABLES"
}

# Test MySQL connection
test_mysql_connection() {
    log "Testing MySQL connection..."
    if ! mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" -e "USE $MYSQL_DB" 2>/dev/null; then
        error "Cannot connect to MySQL database. Please check credentials."
    fi
    log "MySQL connection successful"
}

# Main execution flow
main() {
    log "Starting Universal Data to ORC Converter"
    
    detect_os
    check_prerequisites
    get_conversion_type
    get_output_config
    
    case $DATA_TYPE in
        1) # MySQL
            get_mysql_config
            test_mysql_connection
            get_mysql_table_list
            create_mysql_converter
            ;;
        2) # SQL Dump
            get_sql_dump_config
            create_sql_dump_converter
            ;;
        3) # CSV
            get_csv_config
            create_csv_converter
            ;;
        *)
            error "Invalid data type selected"
            ;;
    esac
    
    perform_conversion
    
    log "Process completed! Check $LOG_FILE for details."
    log "Remember: All .orc files in each table directory should be read together as one table."
}

# Run main function
main "$@"
