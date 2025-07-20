#!/bin/bash

# Database configuration
DB_NAME="isa95_manufacturing_level2"
DB_USER="turintonadmin"
DB_HOST="insights-db.postgres.database.azure.com"
DB_PORT="5432"
CSV_FOLDER="/home/devendra_yadav/mfg_data_generator/data_scripts/data"

# Tables ordered by dependency (no foreign keys first, then dependent tables)
# This order ensures foreign key constraints are satisfied
TABLES=(
  # Independent tables (no foreign keys to other Level 2 tables)
  "facilities"
  
  # Tables with foreign keys to facilities
  "process_areas"              # depends on facilities
  
  # Tables with foreign keys to process_areas
  "equipment"                  # depends on process_areas
  
  # Tables with foreign keys to equipment
  "equipment_states"           # depends on equipment
  "alarms"                     # depends on equipment
  "process_parameters"         # depends on equipment
  
  # Recipe-related tables
  "recipes"                    # independent (references products from Level 4)
  "batch_steps"               # depends on recipes
  
  # Batch tables
  "batches"                   # depends on recipes, equipment
  "batch_execution"           # depends on batches, batch_steps, equipment
)

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}ISA-95 Level 2 Data Upload Script${NC}"
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

# Function to format large numbers with commas
format_number() {
    printf "%'d" $1
}

# Function to check if file is large (over 100MB)
is_large_file() {
    local file=$1
    local size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null)
    [ $size -gt 104857600 ]  # 100MB in bytes
}

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

# Counter for tracking progress
TOTAL_TABLES=${#TABLES[@]}
CURRENT=0
SUCCESS=0
FAILED=0
LARGE_TABLE_COUNT=0

# Check for large files first
echo -e "\n${YELLOW}Checking for large data files...${NC}"
for table in "${TABLES[@]}"
do
    CSV_FILE="${CSV_FOLDER}/${table}.csv"
    if [ -f "$CSV_FILE" ] && is_large_file "$CSV_FILE"; then
        FILE_SIZE=$(du -h "$CSV_FILE" | cut -f1)
        echo -e "  ${BLUE}Large file detected: ${table}.csv (${FILE_SIZE})${NC}"
        LARGE_TABLE_COUNT=$((LARGE_TABLE_COUNT + 1))
    fi
done

if [ $LARGE_TABLE_COUNT -gt 0 ]; then
    echo -e "${YELLOW}Note: $LARGE_TABLE_COUNT large files detected. Upload may take longer.${NC}"
fi

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
    if [[ "$table" == "equipment_states" || "$table" == "alarms" || "$table" == "process_parameters" || "$table" == "batch_execution" ]]; then
        if [ $ROW_COUNT -gt 100000 ]; then
            echo "  → Large time-series table detected. This may take several minutes..."
        fi
    fi
    
    # Check if it's a large file
    if is_large_file "$CSV_FILE"; then
        FILE_SIZE=$(du -h "$CSV_FILE" | cut -f1)
        echo "  → Large file size: $FILE_SIZE"
        
        # Use pv (pipe viewer) if available for progress bar
        if command -v pv &> /dev/null; then
            echo "  → Uploading with progress indicator..."
            pv "$CSV_FILE" | psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM STDIN WITH (FORMAT csv, HEADER true)" 2>&1
            UPLOAD_RESULT=$?
        else
            echo "  → Uploading data (this may take a while)..."
            ERROR_MSG=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM '${CSV_FILE}' WITH (FORMAT csv, HEADER true)" 2>&1)
            UPLOAD_RESULT=$?
        fi
    else
        # Regular upload for smaller files
        echo "  → Uploading data..."
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
    echo "Table Name                     Row Count"
    echo "----------------------------------------"
    for table in "${TABLES[@]}"
    do
        COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM ${table}" 2>/dev/null | tr -d ' ')
        if [ ! -z "$COUNT" ]; then
            printf "%-30s %15s\n" "$table" "$(format_number $COUNT)"
        fi
    done
    
    # Show facility and area distribution
    echo -e "\n${YELLOW}Facility and Area Distribution:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            f.facility_name,
            f.facility_type,
            COUNT(DISTINCT pa.area_id) as area_count,
            COUNT(DISTINCT e.equipment_id) as equipment_count
        FROM facilities f
        LEFT JOIN process_areas pa ON f.facility_id = pa.facility_id
        LEFT JOIN equipment e ON pa.area_id = e.area_id
        GROUP BY f.facility_id, f.facility_name, f.facility_type
        ORDER BY f.facility_name;" 2>/dev/null
    
    # Show equipment distribution by type
    echo -e "\n${YELLOW}Equipment Distribution by Type:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            equipment_type,
            COUNT(*) as count,
            COUNT(CASE WHEN equipment_status = 'Running' THEN 1 END) as running,
            COUNT(CASE WHEN equipment_status = 'Maintenance' THEN 1 END) as maintenance
        FROM equipment
        GROUP BY equipment_type
        ORDER BY count DESC
        LIMIT 10;" 2>/dev/null
    
    # Show batch statistics
    echo -e "\n${YELLOW}Batch Statistics:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            batch_status,
            COUNT(*) as count,
            ROUND(AVG(batch_size)::numeric, 2) as avg_batch_size
        FROM batches
        GROUP BY batch_status
        ORDER BY count DESC;" 2>/dev/null
    
    # Show time-series data ranges
    echo -e "\n${YELLOW}Time-Series Data Ranges:${NC}"
    
    # Equipment states time range
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            'Equipment States' as table_name,
            MIN(start_timestamp) as earliest,
            MAX(end_timestamp) as latest,
            COUNT(DISTINCT equipment_id) as unique_equipment
        FROM equipment_states;" 2>/dev/null
    
    # Alarms time range
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            'Alarms' as table_name,
            MIN(activation_timestamp) as earliest,
            MAX(resolution_timestamp) as latest,
            COUNT(DISTINCT equipment_id) as unique_equipment
        FROM alarms;" 2>/dev/null
    
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
    
    # Check process areas → facilities
    ORPHANED_AREAS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM process_areas pa
        LEFT JOIN facilities f ON pa.facility_id = f.facility_id
        WHERE f.facility_id IS NULL AND pa.facility_id IS NOT NULL AND pa.facility_id != ''" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_AREAS" = "0" ]; then
        echo -e "${GREEN}  ✓ All process areas have valid facility references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_AREAS orphaned process areas${NC}"
    fi
    
    # Check equipment → process areas
    ORPHANED_EQUIPMENT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM equipment e
        LEFT JOIN process_areas pa ON e.area_id = pa.area_id
        WHERE pa.area_id IS NULL AND e.area_id IS NOT NULL AND e.area_id != ''" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_EQUIPMENT" = "0" ]; then
        echo -e "${GREEN}  ✓ All equipment have valid area references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_EQUIPMENT orphaned equipment${NC}"
    fi
    
    # Check equipment states → equipment
    ORPHANED_STATES=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM equipment_states es
        LEFT JOIN equipment e ON es.equipment_id = e.equipment_id
        WHERE e.equipment_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_STATES" = "0" ]; then
        echo -e "${GREEN}  ✓ All equipment states have valid equipment references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_STATES orphaned equipment states${NC}"
    fi
    
    # Check batches → recipes
    ORPHANED_BATCHES=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM batches b
        LEFT JOIN recipes r ON b.recipe_id = r.recipe_id
        WHERE r.recipe_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_BATCHES" = "0" ]; then
        echo -e "${GREEN}  ✓ All batches have valid recipe references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_BATCHES orphaned batches${NC}"
    fi
    
    # Check batch execution → batches and batch steps
    ORPHANED_EXECUTION=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM batch_execution be
        LEFT JOIN batches b ON be.batch_id = b.batch_id
        LEFT JOIN batch_steps bs ON be.step_id = bs.step_id
        WHERE b.batch_id IS NULL OR bs.step_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_EXECUTION" = "0" ]; then
        echo -e "${GREEN}  ✓ All batch execution records have valid references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_EXECUTION orphaned batch execution records${NC}"
    fi
    
    unset PGPASSWORD
fi

# Performance recommendations
echo -e "\n${YELLOW}Performance Recommendations:${NC}"
echo "1. Run ANALYZE on the database to update statistics"
echo "2. Consider creating indexes on foreign key columns"
echo "3. For time-series tables, consider partitioning by date"
echo "4. Monitor table bloat for frequently updated tables"

echo -e "\n${GREEN}Script completed!${NC}"