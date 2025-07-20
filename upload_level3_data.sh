#!/bin/bash

# Database configuration
DB_NAME="isa95_manufacturing_level3"
DB_USER="turintonadmin"
DB_HOST="insights-db.postgres.database.azure.com"
DB_PORT="5432"
CSV_FOLDER="/home/devendra_yadav/mfg_data_generator/data_scripts/data"

# Tables ordered by dependency (no foreign keys first, then dependent tables)
# This order ensures foreign key constraints are satisfied
TABLES=(
  # Independent tables (no foreign keys to other Level 3 tables)
  "work_orders"                # May reference Level 2/4 data but independent within Level 3
  
  # Material management tables
  "material_lots"              # Independent, references materials from Level 4
  "material_transactions"      # depends on material_lots, work_orders
  "material_consumption"       # depends on material_lots, work_orders, equipment (from Level 2)
  
  # Quality management tables
  "quality_tests"              # depends on material_lots, work_orders
  "quality_events"             # depends on quality_tests, equipment, material_lots
  
  # Operations management tables
  "maintenance_activities"     # depends on equipment (from Level 2), work_orders
  "resource_utilization"       # depends on equipment, material_lots, work_orders
  "production_performance"     # depends on equipment, work_orders
)

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${YELLOW}ISA-95 Level 3 Data Upload Script${NC}"
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

# Check for required Level 2 data
echo -e "\n${YELLOW}Checking for required Level 2 dependencies...${NC}"
EQUIPMENT_EXISTS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='equipment')" 2>/dev/null | tr -d ' ')

if [ "$EQUIPMENT_EXISTS" = "f" ]; then
    echo -e "${RED}Warning: Equipment table not found. Some Level 3 tables require Level 2 equipment data.${NC}"
    echo -e "${YELLOW}Consider running Level 2 data generation first.${NC}"
fi

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
    if [[ "$table" == "material_transactions" || "$table" == "quality_tests" || "$table" == "resource_utilization" || "$table" == "production_performance" ]]; then
        if [ $ROW_COUNT -gt 100000 ]; then
            echo "  → Large operational data table detected. This may take several minutes..."
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
    
    # Show work order statistics
    echo -e "\n${YELLOW}Work Order Statistics:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            work_order_type,
            status,
            COUNT(*) as count,
            ROUND(AVG(planned_quantity)::numeric, 2) as avg_quantity
        FROM work_orders
        GROUP BY work_order_type, status
        ORDER BY work_order_type, count DESC
        LIMIT 15;" 2>/dev/null
    
    # Show material lot statistics
    echo -e "\n${YELLOW}Material Lot Statistics:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            status,
            quality_status,
            COUNT(*) as count,
            COUNT(DISTINCT material_id) as unique_materials
        FROM material_lots
        GROUP BY status, quality_status
        ORDER BY count DESC;" 2>/dev/null
    
    # Show quality test results
    echo -e "\n${YELLOW}Quality Test Results Summary:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            test_type,
            test_result,
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY test_type), 2) as percentage
        FROM quality_tests
        GROUP BY test_type, test_result
        ORDER BY test_type, test_result;" 2>/dev/null
    
    # Show production performance summary
    echo -e "\n${YELLOW}Production Performance Summary:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            DATE_TRUNC('day', timestamp::timestamp) as date,
            ROUND(AVG(oee_percent)::numeric, 2) as avg_oee,
            ROUND(AVG(availability_percent)::numeric, 2) as avg_availability,
            ROUND(AVG(performance_percent)::numeric, 2) as avg_performance,
            ROUND(AVG(quality_percent)::numeric, 2) as avg_quality,
            SUM(production_count) as total_production,
            SUM(reject_count) as total_rejects
        FROM production_performance
        WHERE timestamp::date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY DATE_TRUNC('day', timestamp::timestamp)
        ORDER BY date DESC
        LIMIT 7;" 2>/dev/null
    
    # Show maintenance activity status
    echo -e "\n${YELLOW}Maintenance Activity Status:${NC}"
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        SELECT 
            activity_type,
            status,
            COUNT(*) as count,
            ROUND(AVG(planned_duration_hours)::numeric, 2) as avg_duration_hours
        FROM maintenance_activities
        GROUP BY activity_type, status
        ORDER BY activity_type, count DESC;" 2>/dev/null
    
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
    
    # Check material transactions → material lots
    ORPHANED_TRANSACTIONS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM material_transactions mt
        LEFT JOIN material_lots ml ON mt.lot_id = ml.lot_id
        WHERE ml.lot_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_TRANSACTIONS" = "0" ]; then
        echo -e "${GREEN}  ✓ All material transactions have valid lot references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_TRANSACTIONS orphaned material transactions${NC}"
    fi
    
    # Check material consumption → material lots
    ORPHANED_CONSUMPTION=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM material_consumption mc
        LEFT JOIN material_lots ml ON mc.lot_id = ml.lot_id
        WHERE ml.lot_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_CONSUMPTION" = "0" ]; then
        echo -e "${GREEN}  ✓ All material consumptions have valid lot references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_CONSUMPTION orphaned material consumptions${NC}"
    fi
    
    # Check quality tests → material lots (where lot_id is not empty)
    ORPHANED_TESTS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
        SELECT COUNT(*) 
        FROM quality_tests qt
        LEFT JOIN material_lots ml ON qt.lot_id = ml.lot_id
        WHERE qt.lot_id IS NOT NULL AND qt.lot_id != '' AND ml.lot_id IS NULL" 2>/dev/null | tr -d ' ')
    
    if [ "$ORPHANED_TESTS" = "0" ]; then
        echo -e "${GREEN}  ✓ All quality tests have valid lot references${NC}"
    else
        echo -e "${RED}  ✗ Found $ORPHANED_TESTS orphaned quality tests${NC}"
    fi
    
    # Check if equipment references exist (cross-level check)
    if [ "$EQUIPMENT_EXISTS" = "t" ]; then
        # Check maintenance activities → equipment
        ORPHANED_MAINTENANCE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
            SELECT COUNT(*) 
            FROM maintenance_activities ma
            LEFT JOIN equipment e ON ma.equipment_id = e.equipment_id
            WHERE e.equipment_id IS NULL" 2>/dev/null | tr -d ' ')
        
        if [ "$ORPHANED_MAINTENANCE" = "0" ]; then
            echo -e "${GREEN}  ✓ All maintenance activities have valid equipment references${NC}"
        else
            echo -e "${RED}  ✗ Found $ORPHANED_MAINTENANCE orphaned maintenance activities${NC}"
        fi
        
        # Check production performance → equipment
        ORPHANED_PERFORMANCE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "
            SELECT COUNT(*) 
            FROM production_performance pp
            LEFT JOIN equipment e ON pp.equipment_id = e.equipment_id
            WHERE e.equipment_id IS NULL" 2>/dev/null | tr -d ' ')
        
        if [ "$ORPHANED_PERFORMANCE" = "0" ]; then
            echo -e "${GREEN}  ✓ All production performance records have valid equipment references${NC}"
        else
            echo -e "${RED}  ✗ Found $ORPHANED_PERFORMANCE orphaned production performance records${NC}"
        fi
    else
        echo -e "${YELLOW}  ⚠ Skipping equipment reference checks (equipment table not found)${NC}"
    fi
    
    unset PGPASSWORD
fi

# Performance recommendations
echo -e "\n${YELLOW}Performance Recommendations:${NC}"
echo "1. Run ANALYZE on the database to update statistics"
echo "2. Create indexes on foreign key columns for better join performance"
echo "3. Consider partitioning large tables by date (material_transactions, quality_tests)"
echo "4. For time-series data, consider using TimescaleDB extension"
echo "5. Monitor query performance and create additional indexes as needed"

# Data insights
echo -e "\n${MAGENTA}Data Insights:${NC}"
echo "- Work Orders: Track production planning and scheduling"
echo "- Material Management: Monitor inventory movements and consumption"
echo "- Quality Management: Track test results and quality events"
echo "- Maintenance: Monitor equipment maintenance activities"
echo "- Performance: Analyze OEE and production metrics"

echo -e "\n${GREEN}Script completed!${NC}"