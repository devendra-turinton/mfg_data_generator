#!/bin/bash

# Database configuration
DB_NAME="isa95_manufacturing_level4"
DB_USER="turintonadmin"
DB_HOST="insights-db.postgres.database.azure.com"
DB_PORT="5432"
CSV_FOLDER="/home/devendra_yadav/mfg_data_generator/data_scripts/data"

# Tables ordered by dependency (no foreign keys first, then dependent tables)
# This order ensures foreign key constraints are satisfied
TABLES=(
  # Independent tables (no foreign keys to other Level 4 tables)
  "products"
  "materials"
  "customers"
  "suppliers"
  "personnel"
  
  # Tables with foreign keys to above tables
  "facilities"                    # depends on personnel
  "bill_of_materials"            # depends on products, materials
  "customer_orders"              # depends on customers, personnel
  "purchase_orders"              # depends on suppliers, personnel
  
  # Tables with foreign keys to above tables
  "storage_locations"            # depends on facilities
  "shifts"                       # depends on facilities, personnel
  "production_schedules"         # depends on facilities, personnel
  "order_lines"                  # depends on customer_orders, products
  "material_lots"                # depends on materials, suppliers, storage_locations
  
  # Tables with complex dependencies
  "purchase_order_lines"         # depends on purchase_orders, materials, material_lots
  "scheduled_production"         # depends on production_schedules, products, customer_orders
  "inventory_transactions"       # depends on materials, material_lots, storage_locations, personnel
  "material_consumption"         # depends on material_lots, personnel
  
  # Financial tables (depend on many other tables)
  "costs"                        # depends on products, personnel
  "cogs"                         # depends on products
)

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}ISA-95 Level 4 Data Upload Script${NC}"
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
    echo "  → CSV file found with $ROW_COUNT rows"
    
    # Upload data
    echo "  → Uploading data..."
    ERROR_MSG=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\copy ${table} FROM '${CSV_FILE}' WITH (FORMAT csv, HEADER true)" 2>&1)
    
    if [ $? -eq 0 ]; then
        # Verify upload by counting rows in database
        DB_COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM ${table}" 2>/dev/null | tr -d ' ')
        echo -e "${GREEN}  ✓ Successfully uploaded $DB_COUNT rows to ${table}${NC}"
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

# Optional: Show table row counts
echo -e "\n${YELLOW}Do you want to see row counts for all tables? (y/n)${NC}"
read -n 1 SHOW_COUNTS
echo

if [ "$SHOW_COUNTS" = "y" ] || [ "$SHOW_COUNTS" = "Y" ]; then
    echo -e "\n${YELLOW}Table Row Counts:${NC}"
    echo "Enter PostgreSQL password again:"
    read -s PGPASSWORD
    export PGPASSWORD
    
    for table in "${TABLES[@]}"
    do
        COUNT=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM ${table}" 2>/dev/null | tr -d ' ')
        if [ ! -z "$COUNT" ]; then
            printf "  %-30s %s rows\n" "$table:" "$COUNT"
        fi
    done
    
    unset PGPASSWORD
fi

echo -e "\n${GREEN}Script completed!${NC}"