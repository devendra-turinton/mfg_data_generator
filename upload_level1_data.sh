#!/bin/bash

# Database configuration
DB_NAME="isa95_manufacturing_level1"
DB_USER="turintonadmin"
DB_HOST="insights-db.postgres.database.azure.com"
DB_PORT="5432"
CSV_FOLDER="/home/devendra_yadav/mfg_data_generator/data_scripts/data"

# Tables ordered by dependency (no foreign keys first, then dependent tables)
# This order ensures foreign key constraints are satisfied
TABLES=(
  # Independent tables (no foreign keys to other Level 1 tables)
  "sensors"
  "actuators"
  
  # Tables with foreign keys to above tables
  "control_loops"              # depends on sensors, actuators
  
  # Tables with foreign keys (time-series data)
  "sensor_readings"            # depends on sensors
  "actuator_commands"          # depends on actuators
  "device_diagnostics"         # depends on both sensors and actuators (device_id)
)

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}ISA-95 Level 1 Data Upload Script${NC}"
echo "================================================"
echo "Database: $DB_NAME"
echo "Host: $DB_HOST"
echo "User: $DB_USER"
echo "CSV Folder: $CSV_FOLDER"
echo "================================================"

# Check if CSV folder exists
if [ ! -d "$CSV_FOLDER" ]; then
    echo -e "${RED}Error: CSV folder does not exist: $CSV_FOLDER${NC}"
    exit 1
fi

# Prompt for password
echo -n "Enter PostgreSQL password for user ${DB_USER}: "
read -s PGPASSWORD
echo
export PGPASSWORD

# Test database connection
echo -e "\n${YELLOW}Testing database connection...${NC}"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Cannot connect to database. Please check your credentials.${NC}"
    unset PGPASSWORD
    exit 1
fi
echo -e "${GREEN}Connection successful!${NC}"

# Function to format large numbers with commas
format_number() {
    printf "%'d" $1
}

# Counter for tracking progress
TOTAL_TABLES=${#TABLES[@]}
CURRENT=0
SUCCESS=0
FAILED=0

echo -e "\n${YELLOW}Starting data upload for $TOTAL_TABLES tables...${NC}\n"

# Upload data for each table
for table in "${TABLES[@]}"
do
    CURRENT=$((CURRENT + 1))
    CSV_FILE="${CSV_FOLDER}/${table}.csv"
    
    echo -e "${YELLOW}[$CURRENT/$TOTAL_TABLES] Processing table: ${table}${NC}"
    
    # Check if CSV file exists
    if [ ! -f "$CSV_FILE" ]; then
        echo -e "${RED}  ✗ CSV file not found: ${CSV_FILE}${NC}"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Get row count from CSV (minus header)
    ROW_COUNT=$(( $(wc -l < "$CSV_FILE") - 1 ))
    echo "  → CSV file found with $(format_number $ROW_COUNT) rows"
    
    # Special handling for large tables
    if [[ "$table" == "sensor_readings" || "$table" == "actuator_commands" || "$table" == "device_diagnostics" ]]; then
        echo "  → Large table detected. This may take several minutes..."
    fi
    
    # Upload data
    echo "  → Uploading data..."
    
    # For very large tables, show progress
    if [[ "$table" == "sensor_readings" || "$table" == "actuator_commands" ]] && [ $ROW_COUNT -gt 100000 ]; then
        # Use pv (pipe viewer) if available for progress bar
        if command -v pv &> /dev/null; then
            pv "$CSV_FILE" | psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM STDIN WITH (FORMAT csv, HEADER true)" 2>&1
            UPLOAD_RESULT=$?
        else
            ERROR_MSG=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM '${CSV_FILE}' WITH (FORMAT csv, HEADER true)" 2>&1)
            UPLOAD_RESULT=$?
        fi
    else
        ERROR_MSG=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM '${CSV_FILE}' WITH (FORMAT csv, HEADER true)" 2>&1)
        UPLOAD_RESULT=$?
    fi
    
    if [ $UPLOAD_RESULT -eq 0 ]; then
        # Verify upload by counting rows in database
        DB_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM ${table}" 2>/dev/null | tr -d ' ')
        echo -e "${GREEN}  ✓ Successfully uploaded $(format_number $DB_COUNT) rows to ${table}${NC}"
        SUCCESS=$((SUCCESS + 1))
    else
        echo -e "${RED}  ✗ Failed to upload ${table}${NC}"
        echo -e "${RED}  Error: ${ERROR_MSG}${NC}"
        FAILED=$((FAILED + 1))
    fi
    echo
done

# Clean up
unset PGPASSWORD

# Summary
echo "================================================"
echo -e "${YELLOW}Upload Summary:${NC}"
echo -e "${GREEN}  Successful: $SUCCESS tables${NC}"
echo -e "${RED}  Failed: $FAILED tables${NC}"
echo "================================================"

# Optional: Show table row counts and statistics
echo -e "\n${YELLOW}Do you want to see detailed statistics for all tables? (y/n)${NC}"
read -n 1 SHOW_STATS
echo

if [ "$SHOW_STATS" = "y" ] || [ "$SHOW_STATS" = "Y" ]; then
    echo -e "\n${YELLOW}Table Statistics:${NC}"
    echo "Enter PostgreSQL password again:"
    read -s PGPASSWORD
    export PGPASSWORD
    
    echo -e "\n${YELLOW}Row Counts:${NC}"
    for table in "${TABLES[@]}"
    do
        COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM ${table}" 2>/dev/null | tr -d ' ')
        if [ ! -z "$COUNT" ]; then
            printf "  %-25s %15s rows\n" "$table:" "$(format_number $COUNT)"
        fi
    done
    
    # Show additional statistics for time-series tables
    echo -e "\n${YELLOW}Time Range Statistics:${NC}"
    
    # Sensor readings time range
    SENSOR_TIME_RANGE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT 
            'Sensor Readings: ' || 
            TO_CHAR(MIN(timestamp), 'YYYY-MM-DD HH24:MI:SS') || ' to ' || 
            TO_CHAR(MAX(timestamp), 'YYYY-MM-DD HH24:MI:SS')
        FROM sensor_readings" 2>/dev/null | xargs)
    if [ ! -z "$SENSOR_TIME_RANGE" ]; then
        echo "  $SENSOR_TIME_RANGE"
    fi
    
    # Actuator commands time range
    ACTUATOR_TIME_RANGE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT 
            'Actuator Commands: ' || 
            TO_CHAR(MIN(timestamp), 'YYYY-MM-DD HH24:MI:SS') || ' to ' || 
            TO_CHAR(MAX(timestamp), 'YYYY-MM-DD HH24:MI:SS')
        FROM actuator_commands" 2>/dev/null | xargs)
    if [ ! -z "$ACTUATOR_TIME_RANGE" ]; then
        echo "  $ACTUATOR_TIME_RANGE"
    fi
    
    # Device diagnostics time range
    DIAG_TIME_RANGE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT 
            'Device Diagnostics: ' || 
            TO_CHAR(MIN(timestamp), 'YYYY-MM-DD HH24:MI:SS') || ' to ' || 
            TO_CHAR(MAX(timestamp), 'YYYY-MM-DD HH24:MI:SS')
        FROM device_diagnostics" 2>/dev/null | xargs)
    if [ ! -z "$DIAG_TIME_RANGE" ]; then
        echo "  $DIAG_TIME_RANGE"
    fi
    
    # Show device counts
    echo -e "\n${YELLOW}Device Counts:${NC}"
    
    # Active sensors by type
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT sensor_type, COUNT(*) as count
        FROM sensors
        WHERE status = 'Active'
        GROUP BY sensor_type
        ORDER BY count DESC
        LIMIT 5" 2>/dev/null
    
    # Active actuators by type
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT actuator_type, COUNT(*) as count
        FROM actuators
        WHERE status = 'Active'
        GROUP BY actuator_type
        ORDER BY count DESC
        LIMIT 5" 2>/dev/null
    
    unset PGPASSWORD
fi

# Optional: Verify data integrity
echo -e "\n${YELLOW}Do you want to verify data integrity (foreign key relationships)? (y/n)${NC}"
read -n 1 VERIFY_INTEGRITY
echo

if [ "$VERIFY_INTEGRITY" = "y" ] || [ "$VERIFY_INTEGRITY" = "Y" ]; then
    echo -e "\n${YELLOW}Verifying Data Integrity:${NC}"
    echo "Enter PostgreSQL password again:"
    read -s PGPASSWORD
    export PGPASSWORD
    
    # Check for orphaned sensor readings
    ORPHANED_READINGS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM sensor_readings sr
        LEFT JOIN sensors s ON sr.sensor_id = s.sensor_id
        WHERE s.sensor_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_READINGS" = "0" ]; then
        echo -e "${GREEN}  ✓ All sensor readings have valid sensor references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_READINGS orphaned sensor readings${NC}"
    fi
    
    # Check for orphaned actuator commands
    ORPHANED_COMMANDS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM actuator_commands ac
        LEFT JOIN actuators a ON ac.actuator_id = a.actuator_id
        WHERE a.actuator_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_COMMANDS" = "0" ]; then
        echo -e "${GREEN}  ✓ All actuator commands have valid actuator references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_COMMANDS orphaned actuator commands${NC}"
    fi
    
    # Check control loops integrity
    INVALID_LOOPS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM control_loops cl
        LEFT JOIN sensors s ON cl.process_variable_sensor_id = s.sensor_id
        LEFT JOIN actuators a ON cl.control_output_actuator_id = a.actuator_id
        WHERE s.sensor_id IS NULL OR a.actuator_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$INVALID_LOOPS" = "0" ]; then
        echo -e "${GREEN}  ✓ All control loops have valid sensor and actuator references${NC}"
    else
        echo -e "${RED}  ✗ Found $INVALID_LOOPS control loops with invalid references${NC}"
    fi
    
    unset PGPASSWORD
fi

echo -e "\n${GREEN}Script completed!${NC}"
echo -e "${YELLOW}Note: For optimal performance, consider running ANALYZE on the database after large data uploads.${NC}"