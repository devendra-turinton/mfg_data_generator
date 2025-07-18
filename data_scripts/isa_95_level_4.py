import pandas as pd
import numpy as np
import uuid
import json
import os
import csv
from datetime import datetime, timedelta
import random
import time
import argparse

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

class ISA95Level4DataGenerator:
    """
    Generator for ISA-95 Level 4 (Business Planning & Logistics) data.
    
    This class generates synthetic data for all tables in Level 4:
    - Products
    - Materials
    - Bill of Materials
    - Customers
    - Customer Orders
    - Order Lines
    - Suppliers
    - Purchase Orders
    - Purchase Order Lines
    - Production Schedules
    - Scheduled Production
    - Facilities
    - Storage Locations
    - Shifts
    - Inventory Transactions
    - Material Lots
    - Material Consumption
    - Costs
    - Cogs
    """
    
    def __init__(self, output_dir="data", level3_data_available=False):
        """
        Initialize the data generator.
        
        Parameters:
        - output_dir: Directory where generated data will be saved
        - level3_data_available: Whether Level 3 data is available to reference
        """
        self.output_dir = output_dir
        self.level3_data_available = level3_data_available
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Track generated data for relationships
        self.products_df = None
        self.materials_df = None
        self.bill_of_materials_df = None
        self.customers_df = None
        self.customer_orders_df = None
        self.order_lines_df = None
        self.suppliers_df = None
        self.purchase_orders_df = None
        self.purchase_order_lines_df = None
        self.production_schedules_df = None
        self.scheduled_production_df = None
        self.facilities_df = None
        self.personnel_df = None
        self.storage_locations_df = None
        self.shifts_df = None
        self.inventory_transactions_df = None
        self.material_lots_df = None
        self.material_consumption_df = None
        self.costs_df = None
    
        
        # Define common reference data
        self.product_ids = []
        self.material_ids = []
        self.customer_ids = []
        self.supplier_ids = []
        self.order_ids = []
        self.work_order_ids = []
        self.batch_ids = []
        self.lot_ids = []
        self.facility_ids = []
        self.location_ids = []
        self.personnel_ids = []
        self.equipment_ids = []
        
        # Initialize reference data
        self._init_reference_data()

    def validate_personnel_availability(self, required_count=1, role_description=""):
        """
        Validate that sufficient personnel records are available
        
        Parameters:
        - required_count: Minimum number of personnel needed
        - role_description: Description of the role (for error messages)
        """
        if not self.personnel_ids or len(self.personnel_ids) < required_count:
            raise ValueError(
                f"Insufficient personnel data. Need at least {required_count} personnel for {role_description}. "
                f"Currently have {len(self.personnel_ids) if self.personnel_ids else 0}. "
                "Generate personnel data first."
            )

    def validate_data_consistency(self):
        """Validate that all stored IDs match their respective DataFrames"""
        validations = []
        
        if hasattr(self, 'products_df') and self.products_df is not None:
            df_ids = set(self.products_df['product_id'].tolist())
            stored_ids = set(self.product_ids)
            if df_ids != stored_ids:
                validations.append(f"Product IDs mismatch: {len(df_ids)} in df vs {len(stored_ids)} stored")
        
        if hasattr(self, 'materials_df') and self.materials_df is not None:
            df_ids = set(self.materials_df['material_id'].tolist())
            stored_ids = set(self.material_ids)
            if df_ids != stored_ids:
                validations.append(f"Material IDs mismatch: {len(df_ids)} in df vs {len(stored_ids)} stored")
        
        if validations:
            print("Data consistency issues found:")
            for issue in validations:
                print(f"  - {issue}")
            return False
        return True

    def _init_reference_data(self):
        """Initialize reference data used across tables"""
        # Try to load Level 3 data for references if available
        self._load_level3_data()
        
        # Create product IDs if empty
        if not self.product_ids:
            self.product_ids = [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create material IDs if empty
        if not self.material_ids:
            self.material_ids = [f"MAT-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create customer IDs if empty
        if not self.customer_ids:
            self.customer_ids = [f"CUST-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create supplier IDs if empty
        if not self.supplier_ids:
            self.supplier_ids = [f"SUP-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create work order IDs if empty
        if not self.work_order_ids:
            self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create batch IDs if empty
        if not self.batch_ids:
            self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create personnel IDs if empty
        if not self.personnel_ids:
            self.personnel_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
    
    def _load_level3_data(self):
        """Load existing Level 3 data if available for reference"""
        if not self.level3_data_available:
            return
        
        try:
            # Try to load work orders data
            work_orders_file = os.path.join(self.output_dir, "work_orders.csv")
            if os.path.exists(work_orders_file):
                work_orders_df = pd.read_csv(work_orders_file)
                if 'work_order_id' in work_orders_df.columns:
                    self.work_order_ids = work_orders_df['work_order_id'].unique().tolist()
            
            # Try to load equipment data
            equipment_file = os.path.join(self.output_dir, "equipment.csv")
            if os.path.exists(equipment_file):
                equipment_df = pd.read_csv(equipment_file)
                if 'equipment_id' in equipment_df.columns:
                    self.equipment_ids = equipment_df['equipment_id'].unique().tolist()
            
            # Try to load batches data
            batches_file = os.path.join(self.output_dir, "batches.csv")
            if os.path.exists(batches_file):
                batches_df = pd.read_csv(batches_file)
                if 'batch_id' in batches_df.columns:
                    self.batch_ids = batches_df['batch_id'].unique().tolist()
                
                if 'product_id' in batches_df.columns:
                    product_ids = batches_df['product_id'].unique().tolist()
                    product_ids = [p for p in product_ids if p and pd.notna(p)]
                    if product_ids:
                        self.product_ids = product_ids
            
            # Try to load material lots data
            material_lots_file = os.path.join(self.output_dir, "material_lots.csv")
            if os.path.exists(material_lots_file):
                material_lots_df = pd.read_csv(material_lots_file)
                if 'lot_id' in material_lots_df.columns:
                    self.lot_ids = material_lots_df['lot_id'].unique().tolist()
                
                if 'material_id' in material_lots_df.columns:
                    material_ids = material_lots_df['material_id'].unique().tolist()
                    material_ids = [m for m in material_ids if m and pd.notna(m)]
                    if material_ids:
                        self.material_ids = material_ids
            
            print(f"Loaded Level 3 data references")
            
        except Exception as e:
            print(f"Warning: Could not load Level 3 data: {e}")

    def generate_all_data(self, num_products=100, num_materials=150, num_bill_of_materials=None,
                        num_customers=100, num_customer_orders=300, num_order_lines=None,
                        num_suppliers=50, num_purchase_orders=200, num_purchase_order_lines=None,
                        num_production_schedules=20, num_scheduled_production=None,
                        num_facilities=15, num_storage_locations=None, num_shifts=None,
                        num_inventory_transactions=1000, num_material_lots=200, 
                        num_material_consumption=300, num_costs=500, num_cogs=200):
        """
        Generate data for all Level 4 tables.
        
        Parameters:
        - num_products: Number of product records to generate
        - num_materials: Number of material records to generate
        - num_bill_of_materials: Number of BOM records to generate (auto-calculated if None)
        - num_customers: Number of customer records to generate
        - num_customer_orders: Number of customer order records to generate
        - num_order_lines: Number of order line records to generate (auto-calculated if None)
        - num_suppliers: Number of supplier records to generate
        - num_purchase_orders: Number of purchase order records to generate
        - num_purchase_order_lines: Number of PO line records to generate (auto-calculated if None)
        - num_production_schedules: Number of production schedule records to generate
        - num_scheduled_production: Number of scheduled production records to generate (auto-calculated if None)
        - num_facilities: Number of facility records to generate
        - num_storage_locations: Number of storage location records to generate (auto-calculated if None)
        - num_shifts: Number of shift records to generate (auto-calculated if None)
        - num_inventory_transactions: Number of inventory transaction records to generate
        - num_material_lots: Number of material lot records to generate
        - num_material_consumption: Number of material consumption records to generate
        - num_costs: Number of cost records to generate
        """
        print("=== ISA-95 Level 4 Data Generation ===")
        
        # Define date ranges
        start_time = datetime.now() - timedelta(days=365)
        end_time = datetime.now() + timedelta(days=30)
        
        # Generate data for each table in a logical order to maintain relationships
        print(f"\n1. Generating {num_products} Products...")
        self.generate_products(num_products)
        
        print(f"\n2. Generating {num_materials} Materials...")
        self.generate_materials(num_materials)
        
        print(f"\n3. Generating Bill of Materials...")
        # Validate data consistency before generating BOM
        if not self.validate_data_consistency():
            print("Warning: Data consistency issues detected. Attempting to fix...")
            # Refresh IDs from DataFrames
            if self.products_df is not None:
                self.product_ids = self.products_df['product_id'].tolist()
            if self.materials_df is not None:
                self.material_ids = self.materials_df['material_id'].tolist()

        if num_bill_of_materials is None:
            self.generate_bill_of_materials()
        else:
            self.generate_bill_of_materials(num_bill_of_materials)
        
        print(f"\n4. Generating {num_customers} Customers...")
        self.generate_customers(num_customers)

        print(f"\n5. Generating {num_suppliers} Suppliers...")
        self.generate_suppliers(num_suppliers)

        # Generate Personnel BEFORE anything that needs personnel IDs
        print(f"\n6. Generating Personnel Records...")
        self.generate_personnel(50)  # Generate 50 personnel records

        # Now generate customer orders (needs personnel for sales reps)
        print(f"\n7. Generating {num_customer_orders} Customer Orders...")
        try:
            self.validate_personnel_availability(10, "sales representatives")
            self.generate_customer_orders(num_customer_orders, start_time, end_time)
        except ValueError as e:
            print(f"Error: {e}")
            print("Skipping customer orders generation")
            return

        print(f"\n8. Generating Order Lines...")
        if num_order_lines is None:
            self.generate_order_lines()
        else:
            self.generate_order_lines(num_order_lines)

        # Generate purchase orders (needs personnel for buyers)
        print(f"\n9. Generating {num_purchase_orders} Purchase Orders...")
        try:
            self.validate_personnel_availability(5, "buyers")
            self.generate_purchase_orders(num_purchase_orders, start_time, end_time)
        except ValueError as e:
            print(f"Error: {e}")
            print("Skipping purchase orders generation")
            return
            
        print(f"\n10. Generating Purchase Order Lines...")
        if num_purchase_order_lines is None:     
            self.generate_purchase_order_lines()
        else:
            self.generate_purchase_order_lines(num_purchase_order_lines)

        print(f"\n11. Generating {num_facilities} Facilities...")
        self.generate_facilities(num_facilities)

        print(f"\n12. Generating Storage Locations...")
        if num_storage_locations is None:
            self.generate_storage_locations()
        else:
            self.generate_storage_locations(num_storage_locations)
        
        print(f"\n13. Generating Shifts...")
        if num_shifts is None:
            self.generate_shifts()
        else:
            self.generate_shifts(num_shifts)
        
        print(f"\n14. Generating {num_production_schedules} Production Schedules...")
        self.generate_production_schedules(num_production_schedules, start_time, end_time)
        
        print(f"\n15. Generating Scheduled Production...")
        if num_scheduled_production is None:
            self.generate_scheduled_production()
        else:
            self.generate_scheduled_production(num_scheduled_production)
        
        print(f"\n16. Generating {num_material_lots} Material Lots...")
        self.generate_material_lots(num_material_lots)
        
        print(f"\n17. Generating {num_inventory_transactions} Inventory Transactions...")
        self.generate_inventory_transactions(num_inventory_transactions, start_time, end_time)
        
        print(f"\n18. Generating {num_material_consumption} Material Consumption Records...")
        self.generate_material_consumption(num_material_consumption)
        
        print(f"\n19. Generating {num_costs} Cost Records...")
        self.generate_costs(num_costs, start_time, end_time)

        print(f"\n20. Generating {num_cogs} COGS Records...")
        self.generate_cogs(num_cogs, start_time, end_time)
        
        print("\nData generation complete!")

    def generate_products(self, num_products=100):
        """
        Generate synthetic data for the Products table.
        
        Parameters:
        - num_products: Number of product records to generate
        
        Returns:
        - DataFrame containing the generated products data
        """
        # Define product categories and types
        product_categories = {
            "Pharmaceutical": [
                "Tablet", "Capsule", "Liquid", "Injection", "Cream", "Ointment", 
                "Inhaler", "Patch", "Suppository", "Suspension"
            ],
            "Food & Beverage": [
                "Dairy", "Bakery", "Beverage", "Snack", "Frozen", "Canned", 
                "Condiment", "Ready Meal", "Confectionery", "Dried"
            ],
            "Chemical": [
                "Industrial", "Agricultural", "Cleaning", "Coating", "Adhesive", 
                "Solvent", "Catalyst", "Resin", "Pigment", "Additive"
            ],
            "Electronics": [
                "Consumer", "Industrial", "Component", "Circuit Board", "Sensor", 
                "Power Supply", "Display", "Memory", "Processor", "Controller"
            ],
            "Automotive": [
                "Engine", "Transmission", "Chassis", "Interior", "Exterior", 
                "Electrical", "Cooling", "Fuel", "Suspension", "Braking"
            ],
            "Consumer Goods": [
                "Personal Care", "Household", "Paper", "Textile", "Appliance", 
                "Furniture", "Toy", "Sporting", "Tool", "Office"
            ]
        }
        
        # Define units of measurement by category
        units_of_measure = {
            "Pharmaceutical": ["mg", "g", "ml", "tablet", "capsule", "vial", "patch", "unit"],
            "Food & Beverage": ["g", "kg", "ml", "L", "oz", "lb", "piece", "pack"],
            "Chemical": ["g", "kg", "L", "gal", "ton", "drum", "pallet", "IBC"],
            "Electronics": ["unit", "piece", "kit", "set", "assembly", "module"],
            "Automotive": ["unit", "piece", "kit", "set", "assembly"],
            "Consumer Goods": ["unit", "piece", "pack", "box", "case", "pallet"]
        }
        
        # Define status options
        product_statuses = ["Active", "In Development", "Obsolete", "On Hold", "Discontinued"]
        status_weights = [0.7, 0.1, 0.1, 0.05, 0.05]  # Mostly active products
        
        # Generate data structure
        data = {
            "product_id": [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_products)],
            "product_name": [],
            "product_code": [],
            "product_family": [],
            "description": [],
            "unit_of_measure": [],
            "status": [],
            "introduction_date": [],
            "discontinuation_date": [],
            "revision": [],
            "base_cost": [],
            "list_price": [],
            "shelf_life_days": [],
            "storage_requirements": [],
            "parent_product_id": []
        }
        
        # Define storage requirements by category
        storage_requirements = {
            "Pharmaceutical": [
                "Room Temperature (15-25°C)", "Refrigerated (2-8°C)", "Frozen (-20°C)", 
                "Cold Chain", "Controlled Room Temperature (20-25°C)", "Protected from Light",
                "Protected from Moisture", "Controlled Humidity (<60% RH)"
            ],
            "Food & Beverage": [
                "Room Temperature", "Refrigerated", "Frozen", "Cool and Dry", 
                "Protected from Light", "Protected from Moisture", "Ambient"
            ],
            "Chemical": [
                "Room Temperature", "Controlled Temperature", "Flammable Storage", 
                "Acid Cabinet", "Base Cabinet", "Ventilated Area", "Away from Incompatibles",
                "Protected from Moisture", "Protected from Light"
            ],
            "Electronics": [
                "ESD Protected", "Temperature Controlled", "Humidity Controlled", 
                "Dust-Free Environment", "Standard Warehouse", "Clean Room"
            ],
            "Automotive": [
                "Standard Warehouse", "Climate Controlled", "Covered Storage", 
                "Protected from Moisture", "Protected from Corrosives"
            ],
            "Consumer Goods": [
                "Standard Warehouse", "Temperature Controlled", "Humidity Controlled", 
                "Stack Limit", "Heavy Item Storage", "Fragile Item Handling"
            ]
        }
        
        # Track products for potential parent-child relationships
        all_products = data["product_id"].copy()
        potential_parents = random.sample(all_products, int(len(all_products) * 0.2))  # 20% can be parents
        
        # Generate data for each product
        for i in range(num_products):
            # Select product category
            category = random.choice(list(product_categories.keys()))
            
            # Select product type within category
            product_type = random.choice(product_categories[category])
            
            # Set product family (category)
            data["product_family"].append(category)
            
            # Generate product name and code
            product_series = random.choice(["Pro", "Elite", "Standard", "Premium", "Ultra", "Max", "Advanced", "Basic"])
            product_number = random.randint(100, 999)
            
            product_name = f"{product_type} {product_series} {product_number}"
            data["product_name"].append(product_name)
            
            # Generate product code (category abbreviation + type abbreviation + number)
            category_abbr = category[0].upper()
            type_abbr = ''.join([word[0].upper() for word in product_type.split()])
            product_code = f"{category_abbr}{type_abbr}{product_number}"
            data["product_code"].append(product_code)
            
            # Generate description
            descriptions = [
                f"Standard {product_type.lower()} for general use",
                f"Premium quality {product_type.lower()} with enhanced features",
                f"Industrial grade {product_type.lower()} for professional applications",
                f"Cost-effective {product_type.lower()} solution",
                f"High-performance {product_type.lower()} designed for demanding environments"
            ]
            data["description"].append(random.choice(descriptions))
            
            # Select unit of measure
            if category in units_of_measure:
                unit = random.choice(units_of_measure[category])
            else:
                unit = "unit"
            data["unit_of_measure"].append(unit)
            
            # Set status (weighted random)
            status = random.choices(product_statuses, weights=status_weights)[0]
            data["status"].append(status)
            
            # Generate introduction date (between 10 years ago and now)
            intro_days_ago = random.randint(0, 3650)
            intro_date = datetime.now() - timedelta(days=intro_days_ago)
            data["introduction_date"].append(intro_date.strftime("%Y-%m-%d"))
            
            # Generate discontinuation date (only for obsolete or discontinued products)
            if status in ["Obsolete", "Discontinued"]:
                # Discontinuation date is after introduction but before now
                min_disc_days_ago = min(intro_days_ago - 1, 1)  # At least 1 day after intro
                disc_days_ago = random.randint(min_disc_days_ago, intro_days_ago - 1)
                disc_date = datetime.now() - timedelta(days=disc_days_ago)
                data["discontinuation_date"].append(disc_date.strftime("%Y-%m-%d"))
            else:
                data["discontinuation_date"].append("")
            
            # Generate revision (format: 1.0, 1.1, 2.0, etc.)
            major_revision = random.randint(1, 3)
            minor_revision = random.randint(0, 9)
            data["revision"].append(f"{major_revision}.{minor_revision}")
            
            # Generate cost and price
            # Different price ranges for different categories
            if category == "Pharmaceutical":
                base_cost = random.uniform(5, 500)
            elif category == "Food & Beverage":
                base_cost = random.uniform(1, 50)
            elif category == "Chemical":
                base_cost = random.uniform(10, 200)
            elif category == "Electronics":
                base_cost = random.uniform(20, 1000)
            elif category == "Automotive":
                base_cost = random.uniform(15, 800)
            else:  # Consumer Goods
                base_cost = random.uniform(2, 100)
            
            data["base_cost"].append(round(base_cost, 2))
            
            # List price is typically cost + markup
            markup = random.uniform(1.2, 2.5)  # 20% to 150% markup
            list_price = base_cost * markup
            data["list_price"].append(round(list_price, 2))
            
            # Generate shelf life
            # Different shelf life ranges for different categories
            if category == "Pharmaceutical":
                shelf_life = random.randint(365, 1825)  # 1-5 years
            elif category == "Food & Beverage":
                shelf_life = random.randint(30, 730)  # 1 month to 2 years
            elif category == "Chemical":
                shelf_life = random.randint(365, 3650)  # 1-10 years
            elif category in ["Electronics", "Automotive", "Consumer Goods"]:
                shelf_life = random.randint(730, 3650)  # 2-10 years
            else:
                shelf_life = random.randint(365, 1825)  # 1-5 years
            
            data["shelf_life_days"].append(shelf_life)
            
            # Set storage requirements
            if category in storage_requirements:
                data["storage_requirements"].append(random.choice(storage_requirements[category]))
            else:
                data["storage_requirements"].append("Standard Storage")
            
            # Determine parent product (if any)
            # About 15% of products will have a parent
            if i > num_products // 2 and random.random() < 0.15:
                # Select parent from products created earlier
                available_parents = data["product_id"][:i//2]  # Only from first half
                if available_parents:
                    parent_id = random.choice(available_parents)
                    data["parent_product_id"].append(parent_id)
                else:
                    data["parent_product_id"].append("")
            else:
                data["parent_product_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Sort by product_id to ensure consistent ordering
        df = df.sort_values('product_id').reset_index(drop=True)

        # Save to CSV
        output_file = os.path.join(self.output_dir, "products.csv")
        df.to_csv(output_file, index=False)

        # Store for later use - ensure synchronization
        self.products_df = df.copy()  # Use copy to avoid reference issues
        self.product_ids = df["product_id"].tolist()

        # Validate storage
        if len(self.product_ids) != len(df):
            raise ValueError(f"Product ID storage mismatch: {len(self.product_ids)} vs {len(df)}")
        
        print(f"Saved {num_products} product records to {output_file}")
        
        return df
    
    def generate_materials(self, num_materials=150):
        """
        Generate synthetic data for the Materials table.
        
        Parameters:
        - num_materials: Number of material records to generate
        
        Returns:
        - DataFrame containing the generated materials data
        """
        # Define material types and their probabilities
        material_types = {
            "Raw Material": 0.4,
            "Packaging": 0.25,
            "WIP": 0.15,
            "Intermediate": 0.1,
            "Consumable": 0.1
        }
        
        # Define material categories
        material_categories = {
            "Chemical": [
                "Solvent", "Reagent", "Catalyst", "Acid", "Base", "Salt", 
                "Polymer", "Monomer", "Pigment", "Additive"
            ],
            "Pharmaceutical": [
                "API", "Excipient", "Binder", "Filler", "Coating", "Disintegrant", 
                "Preservative", "Stabilizer", "Colorant", "Flavoring"
            ],
            "Food": [
                "Ingredient", "Additive", "Flavoring", "Coloring", "Preservative", 
                "Thickener", "Sweetener", "Emulsifier", "Stabilizer", "Nutrient"
            ],
            "Packaging": [
                "Container", "Closure", "Label", "Carton", "Insert", "Film", 
                "Foil", "Bottle", "Cap", "Box"
            ],
            "Industrial": [
                "Metal", "Plastic", "Rubber", "Glass", "Ceramic", "Composite", 
                "Textile", "Paper", "Wood", "Lubricant"
            ]
        }
        
        # Define units of measurement by material type
        units_of_measure = {
            "Raw Material": ["kg", "L", "g", "ton", "m³"],
            "Packaging": ["piece", "unit", "roll", "sheet", "box"],
            "WIP": ["kg", "L", "batch", "unit"],
            "Intermediate": ["kg", "L", "batch", "unit"],
            "Consumable": ["piece", "unit", "kg", "L"]
        }
        
        # Define storage requirements by material category
        storage_requirements = {
            "Chemical": [
                "Flammable Storage", "Acid Cabinet", "Base Cabinet", "Ventilated Area", 
                "Temperature Controlled (15-25°C)", "Protected from Light", "Protected from Moisture",
                "Refrigerated (2-8°C)", "Freezer (-20°C)"
            ],
            "Pharmaceutical": [
                "Room Temperature (15-25°C)", "Refrigerated (2-8°C)", "Frozen (-20°C)", 
                "Controlled Room Temperature (20-25°C)", "Protected from Light",
                "Protected from Moisture", "Controlled Humidity (<60% RH)"
            ],
            "Food": [
                "Room Temperature", "Refrigerated", "Frozen", "Cool and Dry", 
                "Protected from Light", "Protected from Moisture", "Ambient"
            ],
            "Packaging": [
                "Standard Warehouse", "Climate Controlled", "Protected from Moisture", 
                "Protected from Dust", "Away from Chemicals"
            ],
            "Industrial": [
                "Standard Warehouse", "Climate Controlled", "Protected from Moisture", 
                "Protected from Corrosives", "Hazardous Materials Storage"
            ]
        }
        
        # Define hazard classifications
        hazard_classifications = [
            "Non-Hazardous", "Flammable", "Corrosive", "Toxic", "Oxidizer", 
            "Explosive", "Environmentally Hazardous", "Irritant", "Pressurized Gas",
            "Carcinogen", "Radioactive", "Biohazard", "None"
        ]
        
        # Generate data structure
        data = {
            "material_id": [f"MAT-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_materials)],
            "material_name": [],
            "material_type": [],
            "description": [],
            "unit_of_measure": [],
            "standard_cost": [],
            "lead_time_days": [],
            "minimum_order_quantity": [],
            "approved_suppliers": [],
            "safety_stock": [],
            "reorder_point": [],
            "status": [],
            "storage_requirements": [],
            "hazard_classification": []
        }
        
        # Generate data for each material
        for i in range(num_materials):
            # Select material type (weighted random)
            material_type = random.choices(
                list(material_types.keys()), 
                weights=list(material_types.values())
            )[0]
            data["material_type"].append(material_type)
            
            # Select material category
            category = random.choice(list(material_categories.keys()))
            
            # Select material subtype within category
            material_subtype = random.choice(material_categories[category])
            
            # Generate material name
            material_grade = random.choice(["Standard", "Premium", "Technical", "USP", "NF", "EP", "BP", "CP", "ACS", "Ultra"])
            material_number = random.randint(100, 999)
            
            material_name = f"{material_subtype} {material_grade} {material_number}"
            data["material_name"].append(material_name)
            
            # Generate description
            descriptions = [
                f"Standard {material_subtype.lower()} for general use",
                f"{material_grade} grade {material_subtype.lower()} for {material_type.lower()} applications",
                f"High-quality {material_subtype.lower()} meeting {material_grade} specifications",
                f"Industrial {material_subtype.lower()} for manufacturing processes",
                f"{material_grade} certified {material_subtype.lower()}"
            ]
            data["description"].append(random.choice(descriptions))
            
            # Select unit of measure
            if material_type in units_of_measure:
                unit = random.choice(units_of_measure[material_type])
            else:
                unit = "kg"
            data["unit_of_measure"].append(unit)
            
            # Generate cost
            # Different cost ranges for different material types
            if material_type == "Raw Material":
                if material_subtype in ["API", "Catalyst"]:
                    # High-value materials
                    standard_cost = random.uniform(100, 5000)
                else:
                    standard_cost = random.uniform(10, 500)
            elif material_type == "Packaging":
                standard_cost = random.uniform(0.5, 50)
            elif material_type in ["WIP", "Intermediate"]:
                standard_cost = random.uniform(20, 1000)
            else:  # Consumable
                standard_cost = random.uniform(5, 200)
            
            data["standard_cost"].append(round(standard_cost, 2))
            
            # Generate lead time
            if material_type == "Raw Material":
                lead_time = random.randint(30, 120)  # 1-4 months
            elif material_type == "Packaging":
                lead_time = random.randint(14, 60)  # 2-8 weeks
            else:
                lead_time = random.randint(7, 30)  # 1-4 weeks
            
            data["lead_time_days"].append(lead_time)
            
            # Generate minimum order quantity
            if unit in ["kg", "L"]:
                if standard_cost > 1000:
                    # Expensive materials have lower MOQs
                    min_order = random.choice([0.1, 0.25, 0.5, 1, 5])
                else:
                    min_order = random.choice([1, 5, 10, 25, 50, 100])
            elif unit in ["g", "ml"]:
                min_order = random.choice([100, 250, 500, 1000, 5000])
            else:
                min_order = random.choice([10, 25, 50, 100, 500, 1000])
            
            data["minimum_order_quantity"].append(min_order)
            
            # Generate approved suppliers
            if self.supplier_ids:
                num_suppliers = random.randint(1, 3)
                approved_suppliers = random.sample(self.supplier_ids, min(num_suppliers, len(self.supplier_ids)))
            else:
                # Create temporary supplier IDs if none exist yet
                temp_supplier_ids = [f"SUP-{uuid.uuid4().hex[:8].upper()}" for _ in range(10)]
                num_suppliers = random.randint(1, 3)
                approved_suppliers = random.sample(temp_supplier_ids, num_suppliers)
                
            data["approved_suppliers"].append(str(approved_suppliers))
            
            # Generate safety stock and reorder point
            # Higher for critical materials, lower for consumables
            if material_type in ["Raw Material", "Intermediate"]:
                safety_factor = random.uniform(0.5, 2.0)
                consumption_rate = random.uniform(1, 10) * min_order
                
                # Safety stock based on lead time and consumption rate
                safety_stock = consumption_rate * lead_time * safety_factor / 30  # Normalized to monthly consumption
                reorder_point = safety_stock + (consumption_rate * lead_time / 30)
            else:
                safety_stock = min_order * random.uniform(1, 3)
                reorder_point = min_order * random.uniform(2, 5)
            
            data["safety_stock"].append(round(safety_stock, 2))
            data["reorder_point"].append(round(reorder_point, 2))
            
            # Set status (mostly active)
            statuses = ["Active", "Pending Approval", "Obsolete", "On Hold", "Discontinued"]
            status_weights = [0.8, 0.05, 0.05, 0.05, 0.05]
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Set storage requirements
            if category in storage_requirements:
                data["storage_requirements"].append(random.choice(storage_requirements[category]))
            else:
                data["storage_requirements"].append("Standard Storage")
            
            # Set hazard classification
            if category == "Chemical":
                # Chemicals are more likely to be hazardous
                hazard_weights = [0.1, 0.15, 0.15, 0.15, 0.1, 0.05, 0.1, 0.1, 0.05, 0.05, 0.0, 0.0, 0.0]
            elif category == "Pharmaceutical":
                # Pharmaceuticals can be hazardous but less so
                hazard_weights = [0.3, 0.1, 0.05, 0.1, 0.05, 0.0, 0.05, 0.1, 0.0, 0.05, 0.0, 0.05, 0.15]
            else:
                # Other categories are less likely to be hazardous
                hazard_weights = [0.6, 0.05, 0.05, 0.05, 0.0, 0.0, 0.05, 0.05, 0.0, 0.0, 0.0, 0.0, 0.15]
            
            data["hazard_classification"].append(random.choices(hazard_classifications, weights=hazard_weights)[0])
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "materials.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.materials_df = df
        self.material_ids = df["material_id"].tolist()
        
        print(f"Saved {num_materials} material records to {output_file}")
        
        return df

    def generate_bill_of_materials(self, num_records=None):
        """
        Generate synthetic data for the BillOfMaterials table.
        
        Parameters:
        - num_records: Number of BOM records to generate (auto-calculated if None)
        
        Returns:
        - DataFrame containing the generated bill of materials data
        """
        if self.products_df is None or len(self.products_df) == 0:
            print("Error: No products data available. Generate products first.")
            return None
            
        if self.materials_df is None or len(self.materials_df) == 0:
            print("Error: No materials data available. Generate materials first.")
            return None
        
        # Validate that product IDs in dataframe match stored product IDs
        df_product_ids = set(self.products_df['product_id'].tolist())
        stored_product_ids = set(self.product_ids)
        
        if df_product_ids != stored_product_ids:
            print(f"Warning: Product ID mismatch. DataFrame has {len(df_product_ids)} products, stored list has {len(stored_product_ids)}")
            # Refresh the stored list from dataframe
            self.product_ids = self.products_df['product_id'].tolist()

        # Define data structure
        data = {
            "bom_id": [],
            "product_id": [],
            "material_id": [],
            "quantity": [],
            "unit": [],
            "reference_designator": [],
            "bom_level": [],
            "effective_date": [],
            "obsolete_date": [],
            "alternative_material_ids": [],
            "scrap_factor": []
        }
        
        # Track the number of BOM records we'll create
        total_bom_records = 0
        
        # Process each product to create its bill of materials
        for _, product in self.products_df.iterrows():
            product_id = product['product_id']
            product_family = product['product_family']
            product_status = product['status']
            
            # Skip creating BOMs for obsolete or discontinued products
            if product_status in ["Obsolete", "Discontinued"]:
                continue
            
            # Determine the number of materials to include in this product's BOM
            # More complex products have more components
            if product_family in ["Pharmaceutical", "Electronics", "Automotive"]:
                num_materials = random.randint(5, 15)
            elif product_family in ["Food & Beverage", "Chemical"]:
                num_materials = random.randint(3, 10)
            else:
                num_materials = random.randint(2, 8)
            
            # Ensure we don't try to select more materials than available
            num_materials = min(num_materials, len(self.materials_df))
            
            # Select materials for this product's BOM (without replacement for this product)
            # Filter materials by type first to ensure logical combinations
            raw_materials = self.materials_df[self.materials_df['material_type'] == 'Raw Material']
            packaging_materials = self.materials_df[self.materials_df['material_type'] == 'Packaging']
            other_materials = self.materials_df[~self.materials_df['material_type'].isin(['Raw Material', 'Packaging'])]
            
            # Determine mix of material types (usually more raw materials than packaging)
            num_raw = int(num_materials * 0.6)
            num_packaging = int(num_materials * 0.3)
            num_other = num_materials - num_raw - num_packaging
            
            # Adjust if not enough materials of a certain type
            if len(raw_materials) < num_raw:
                num_raw = len(raw_materials)
                num_other += (num_materials - num_raw - num_packaging)
            
            if len(packaging_materials) < num_packaging:
                num_packaging = len(packaging_materials)
                num_other += (num_materials - num_raw - num_packaging)
            
            if len(other_materials) < num_other:
                num_other = len(other_materials)
            
            # Select materials of each type
            selected_raw = raw_materials.sample(num_raw) if num_raw > 0 else pd.DataFrame()
            selected_packaging = packaging_materials.sample(num_packaging) if num_packaging > 0 else pd.DataFrame()
            selected_other = other_materials.sample(num_other) if num_other > 0 else pd.DataFrame()
            
            # Combine selected materials
            selected_materials = pd.concat([selected_raw, selected_packaging, selected_other])
            
            # Create BOM records for this product
            for level, (_, material) in enumerate(selected_materials.iterrows(), 1):
                material_id = material['material_id']
                material_type = material['material_type']
                
                # Generate a unique BOM ID
                bom_id = f"BOM-{uuid.uuid4().hex[:8].upper()}"
                data["bom_id"].append(bom_id)
                data["product_id"].append(product_id)
                data["material_id"].append(material_id)
                
                # Set BOM level
                if material_type == 'Raw Material':
                    bom_level = 1  # Raw materials typically at level 1
                elif material_type == 'Packaging':
                    bom_level = 2  # Packaging typically at level 2
                else:
                    bom_level = random.randint(1, 3)  # Other materials at various levels
                    
                data["bom_level"].append(bom_level)
                
                # Set material quantity based on material type and unit
                material_unit = material['unit_of_measure']
                data["unit"].append(material_unit)
                
                if material_type == 'Raw Material':
                    if material_unit in ['kg', 'L']:
                        quantity = random.uniform(0.1, 100)
                    elif material_unit in ['g', 'ml']:
                        quantity = random.uniform(1, 5000)
                    else:
                        quantity = random.uniform(1, 100)
                elif material_type == 'Packaging':
                    if material_unit in ['piece', 'unit']:
                        quantity = random.randint(1, 10)
                    else:
                        quantity = random.uniform(0.1, 10)
                else:
                    quantity = random.uniform(0.1, 50)
                    
                # Round to appropriate precision
                if material_unit in ['g', 'ml']:
                    quantity = round(quantity, 0)
                else:
                    quantity = round(quantity, 2)
                    
                data["quantity"].append(quantity)
                
                # Set reference designator (mainly for assembled products)
                if product_family in ["Electronics", "Automotive"] and material_type != 'Packaging':
                    reference_designators = [
                        f"POS-{random.randint(1, 100)}",
                        f"COMP-{random.randint(1, 100)}",
                        f"ASY-{random.randint(1, 100)}",
                        f"PCB-{random.randint(1, 100)}",
                        f"MOD-{random.randint(1, 100)}"
                    ]
                    data["reference_designator"].append(random.choice(reference_designators))
                else:
                    data["reference_designator"].append("")
                
                # Set effective and obsolete dates
                # Effective date is typically before product introduction
                if pd.notna(product['introduction_date']):
                    intro_date = pd.to_datetime(product['introduction_date'])
                    effective_date = intro_date - timedelta(days=random.randint(30, 180))
                    data["effective_date"].append(effective_date.strftime("%Y-%m-%d"))
                else:
                    effective_date = datetime.now() - timedelta(days=random.randint(30, 365))
                    data["effective_date"].append(effective_date.strftime("%Y-%m-%d"))
                
                # Most BOM items don't have obsolete dates
                if random.random() < 0.1:  # 10% chance of having an obsolete date
                    obsolete_date = datetime.now() + timedelta(days=random.randint(180, 730))
                    data["obsolete_date"].append(obsolete_date.strftime("%Y-%m-%d"))
                else:
                    data["obsolete_date"].append("")
                
                # Set alternative materials
                # About 20% of materials have alternatives
                if random.random() < 0.2:
                    # Find materials of the same type to use as alternatives
                    same_type_materials = self.materials_df[self.materials_df['material_type'] == material_type]
                    same_type_materials = same_type_materials[same_type_materials['material_id'] != material_id]
                    
                    if len(same_type_materials) > 0:
                        num_alternatives = random.randint(1, min(3, len(same_type_materials)))
                        alternatives = same_type_materials.sample(num_alternatives)['material_id'].tolist()
                        data["alternative_material_ids"].append(str(alternatives))
                    else:
                        data["alternative_material_ids"].append("[]")
                else:
                    data["alternative_material_ids"].append("[]")
                
                # Set scrap factor (higher for more complex materials)
                if material_type == 'Raw Material':
                    scrap_factor = random.uniform(0.02, 0.1)  # 2-10% scrap
                elif material_type == 'Packaging':
                    scrap_factor = random.uniform(0.01, 0.05)  # 1-5% scrap
                else:
                    scrap_factor = random.uniform(0.03, 0.15)  # 3-15% scrap
                    
                data["scrap_factor"].append(round(scrap_factor, 3))
                
                total_bom_records += 1
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "bill_of_materials.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.bill_of_materials_df = df
        
        print(f"Saved {total_bom_records} bill of materials records for {df['product_id'].nunique()} products to {output_file}")
        
        return df
    
    def generate_customers(self, num_customers=100):
        """
        Generate synthetic data for the Customers table.
        
        Parameters:
        - num_customers: Number of customer records to generate
        
        Returns:
        - DataFrame containing the generated customers data
        """
        # Define customer types and their probabilities
        customer_types = {
            "Distributor": 0.3,
            "Retailer": 0.25,
            "Wholesaler": 0.2,
            "Direct Customer": 0.15,
            "Contract Manufacturer": 0.1
        }
        
        # Define industries by customer type
        industries = {
            "Distributor": [
                "Pharmaceutical Distribution", "Food & Beverage Distribution", 
                "Chemical Distribution", "Industrial Supply", "Electronics Distribution",
                "Automotive Parts Distribution", "Consumer Goods Distribution"
            ],
            "Retailer": [
                "Pharmacy", "Grocery", "Department Store", "Specialty Store", 
                "Online Retail", "Home Improvement", "Electronics Retail"
            ],
            "Wholesaler": [
                "Pharmaceutical Wholesale", "Food Service", "Industrial Equipment", 
                "Building Materials", "Electronic Components", "Automotive Parts"
            ],
            "Direct Customer": [
                "Hospital", "Restaurant Chain", "Manufacturing", "Government", 
                "Educational Institution", "Healthcare Provider"
            ],
            "Contract Manufacturer": [
                "Pharmaceutical Manufacturing", "Food Processing", "Electronics Assembly", 
                "Automotive Manufacturing", "Medical Device Manufacturing"
            ]
        }
        
        # Define credit terms
        credit_terms = ["Net 30", "Net 45", "Net 60", "2/10 Net 30", "COD", "Prepaid"]
        credit_terms_weights = [0.4, 0.2, 0.15, 0.1, 0.1, 0.05]  # Probabilities
        
        # Define regions for address generation
        regions = {
            "North America": ["USA", "Canada"],
            "Europe": ["UK", "Germany", "France", "Italy", "Spain", "Netherlands"],
            "Asia": ["Japan", "China", "South Korea", "India", "Singapore", "Taiwan"],
            "Latin America": ["Mexico", "Brazil", "Colombia", "Argentina", "Chile"],
            "Oceania": ["Australia", "New Zealand"]
        }
        
        # Region probabilities (adjust as needed for your business model)
        region_weights = {
            "North America": 0.4,
            "Europe": 0.3,
            "Asia": 0.15,
            "Latin America": 0.1,
            "Oceania": 0.05
        }
        
        # Generate account manager IDs
        if not self.personnel_ids:
            account_manager_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(10)]
        else:
            # Use existing personnel IDs
            account_manager_ids = random.sample(self.personnel_ids, min(10, len(self.personnel_ids)))
        
        # Generate data structure
        data = {
            "customer_id": [f"CUST-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_customers)],
            "customer_name": [],
            "customer_type": [],
            "industry": [],
            "contact_person": [],
            "email": [],
            "phone": [],
            "address": [],
            "credit_terms": [],
            "credit_limit": [],
            "status": [],
            "account_manager_id": []
        }
        
        # First names for contact generation
        first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", 
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa"
        ]
        
        # Last names for contact generation
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", 
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", 
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", 
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
        ]
        
        # Company name components for generating realistic company names
        company_prefixes = [
            "Advanced", "Allied", "American", "Asian", "Atlantic", "Best", "Better", "Blue", 
            "Bright", "Central", "Century", "Consolidated", "Continental", "Digital", "Dynamic", 
            "East", "Eastern", "Euro", "European", "Express", "First", "Global", "Golden"
        ]
        
        company_types = {
            "Distributor": ["Distribution", "Distributors", "Supply", "Supplies", "Logistics"],
            "Retailer": ["Retail", "Stores", "Mart", "Market", "Shop", "Outlet"],
            "Wholesaler": ["Wholesale", "Trading", "Merchandise", "Commerce"],
            "Direct Customer": ["Industries", "Group", "Corporation", "Co.", "Inc.", "Enterprises"],
            "Contract Manufacturer": ["Manufacturing", "Production", "Fabrication", "Industries", "Processors"]
        }
        
        # Generate data for each customer
        for i in range(num_customers):
            # Select customer type (weighted random)
            customer_type = random.choices(
                list(customer_types.keys()), 
                weights=list(customer_types.values())
            )[0]
            data["customer_type"].append(customer_type)
            
            # Generate a realistic company name
            if random.random() < 0.7:  # 70% chance of using prefix
                prefix = random.choice(company_prefixes)
                if customer_type in company_types:
                    suffix = random.choice(company_types[customer_type])
                else:
                    suffix = random.choice(list(company_types.values())[0])
                    
                company_name = f"{prefix} {suffix}"
            else:
                # Use a last name
                last_name = random.choice(last_names)
                if customer_type in company_types:
                    suffix = random.choice(company_types[customer_type])
                else:
                    suffix = random.choice(list(company_types.values())[0])
                    
                company_name = f"{last_name} {suffix}"
            
            data["customer_name"].append(company_name)
            
            # Select industry based on customer type
            if customer_type in industries:
                industry = random.choice(industries[customer_type])
            else:
                # Default to general industry
                industry = "General Manufacturing"
            
            data["industry"].append(industry)
            
            # Generate contact person (random first and last name)
            contact_first = random.choice(first_names)
            contact_last = random.choice(last_names)
            data["contact_person"].append(f"{contact_first} {contact_last}")
            
            # Generate email (company domain based on name)
            company_domain = company_name.lower().replace(" ", "").replace(".", "")
            email_domains = [".com", ".net", ".org", ".co", ".biz"]
            email_domain = random.choice(email_domains)
            data["email"].append(f"{contact_first.lower()}.{contact_last.lower()}@{company_domain}{email_domain}")
            
            # Generate phone
            data["phone"].append(f"+{random.randint(1, 9)}{random.randint(10, 99)} {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(1000, 9999)}")
            
            # Generate address based on region probability
            region = random.choices(
                list(region_weights.keys()), 
                weights=list(region_weights.values())
            )[0]
            
            country = random.choice(regions[region])
            
            # Generate a city name (simplified)
            city_prefixes = ["New", "Old", "East", "West", "North", "South", "Central", "Upper", "Lower", "Port", "Lake", "Mount", "Fort"]
            city_suffixes = ["town", "ville", "burg", "berg", "field", "ford", "port", "mouth", "stad", "furt", "chester", "cester", "bridge", "haven", "minster"]
            
            if random.random() < 0.3:  # 30% chance of using prefix
                city = f"{random.choice(city_prefixes)} {random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            else:
                city = f"{random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            
            # Generate street address
            street_number = random.randint(1, 9999)
            street_types = ["Street", "Avenue", "Boulevard", "Road", "Lane", "Drive", "Way", "Place", "Court", "Terrace"]
            street_name = f"{random.choice(last_names)} {random.choice(street_types)}"
            
            address = f"{street_number} {street_name}, {city}, {country}"
            data["address"].append(address)
            
            # Set credit terms (weighted random)
            credit_term = random.choices(credit_terms, weights=credit_terms_weights)[0]
            data["credit_terms"].append(credit_term)
            
            # Set credit limit based on customer type
            if customer_type in ["Distributor", "Wholesaler"]:
                # Larger customers typically have higher credit limits
                credit_limit = random.randint(50000, 500000)
            elif customer_type == "Retailer":
                credit_limit = random.randint(10000, 100000)
            elif customer_type == "Contract Manufacturer":
                credit_limit = random.randint(100000, 1000000)
            else:
                credit_limit = random.randint(5000, 50000)
                
            data["credit_limit"].append(credit_limit)
            
            # Set status (mostly active)
            statuses = ["Active", "Inactive", "On Hold", "New", "Archived"]
            status_weights = [0.8, 0.05, 0.05, 0.07, 0.03]  # Probabilities
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Assign account manager
            data["account_manager_id"].append(random.choice(account_manager_ids))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "customers.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.customers_df = df
        self.customer_ids = df["customer_id"].tolist()
        
        print(f"Saved {num_customers} customer records to {output_file}")
        
        return df

    def generate_customer_orders(self, num_orders=300, start_time=None, end_time=None):
        """
        Generate synthetic data for the CustomerOrders table.
        
        Parameters:
        - num_orders: Number of customer order records to generate
        - start_time: Start time for order dates
        - end_time: End time for order dates
        
        Returns:
        - DataFrame containing the generated customer orders data
        """
        if self.customers_df is None or len(self.customers_df) == 0:
            print("Error: No customers data available. Generate customers first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now() + timedelta(days=30)
        
        # Define order types and their probabilities
        order_types = {
            "Standard": 0.7,
            "Rush": 0.1,
            "Scheduled": 0.1,
            "Blanket": 0.05,
            "Sample": 0.05
        }
        
        # Define priority levels
        priority_levels = [1, 2, 3, 4, 5]  # 1 = highest, 5 = lowest
        priority_weights = [0.1, 0.2, 0.4, 0.2, 0.1]  # Most orders are medium priority
        
        # Define payment terms
        payment_terms = ["Net 30", "Net 45", "Net 60", "2/10 Net 30", "COD", "Prepaid"]
        
        # Define shipping methods
        shipping_methods = ["Truck", "Air", "Sea", "Rail", "Express", "Courier", "Customer Pickup"]
        
        # Generate sales rep IDs
        if not self.personnel_ids or len(self.personnel_ids) == 0:
            raise ValueError("Personnel data must be generated before customer orders. No personnel IDs available for sales reps.")
            
        # Use existing personnel IDs - select those likely to be sales reps
        sales_rep_ids = []
        if hasattr(self, 'personnel_df') and self.personnel_df is not None:
            # Try to find personnel in sales-related departments
            sales_personnel = self.personnel_df[
                self.personnel_df['department'].isin(['Supply Chain', 'Administration', 'Finance'])
            ]
            if len(sales_personnel) >= 10:
                sales_rep_ids = sales_personnel['personnel_id'].tolist()
            else:
                # If not enough sales personnel, use all available
                sales_rep_ids = self.personnel_df['personnel_id'].tolist()
        else:
            sales_rep_ids = self.personnel_ids

        # Ensure we have at least some sales reps
        if len(sales_rep_ids) == 0:
            raise ValueError("No personnel available to assign as sales representatives")

        # Generate data structure
        data = {
            "order_id": [f"CO-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_orders)],
            "customer_id": [],
            "order_date": [],
            "requested_delivery_date": [],
            "promised_delivery_date": [],
            "status": [],
            "order_type": [],
            "priority": [],
            "order_value": [],
            "payment_terms": [],
            "shipping_method": [],
            "sales_rep_id": [],
            "notes": []
        }
        
        # Generate data for each order
        for i in range(num_orders):
            # Select customer (more active customers place more orders)
            active_customers = self.customers_df[self.customers_df['status'] == 'Active']
            
            if len(active_customers) > 0:
                # Prefer active customers (80% chance)
                if random.random() < 0.8:
                    customer = active_customers.sample(1).iloc[0]
                else:
                    customer = self.customers_df.sample(1).iloc[0]
            else:
                customer = self.customers_df.sample(1).iloc[0]
                
            data["customer_id"].append(customer['customer_id'])
            
            # Generate order date
            time_range_days = (end_time - start_time).days
            days_from_start = random.randint(0, time_range_days)
            order_date = start_time + timedelta(days=days_from_start)
            data["order_date"].append(order_date.strftime("%Y-%m-%d"))
            
            # Select order type (weighted random)
            order_type = random.choices(
                list(order_types.keys()), 
                weights=list(order_types.values())
            )[0]
            data["order_type"].append(order_type)
            
            # Generate requested delivery date based on order type
            if order_type == "Rush":
                # Rush orders have shorter delivery windows
                delivery_window = random.randint(1, 14)  # 1-14 days
            elif order_type == "Standard":
                delivery_window = random.randint(14, 45)  # 2-6 weeks
            elif order_type == "Scheduled":
                delivery_window = random.randint(30, 90)  # 1-3 months
            elif order_type == "Blanket":
                delivery_window = random.randint(60, 180)  # 2-6 months
            else:  # Sample
                delivery_window = random.randint(7, 30)  # 1-4 weeks
                
            requested_delivery_date = order_date + timedelta(days=delivery_window)
            data["requested_delivery_date"].append(requested_delivery_date.strftime("%Y-%m-%d"))
            
            # Generate promised delivery date (usually close to requested, but can vary)
            promise_variation = random.randint(-5, 10)  # -5 to +10 days from requested
            promised_delivery_date = requested_delivery_date + timedelta(days=promise_variation)
            
            # Ensure promised date is not before order date
            if promised_delivery_date <= order_date:
                promised_delivery_date = order_date + timedelta(days=1)
                
            data["promised_delivery_date"].append(promised_delivery_date.strftime("%Y-%m-%d"))
            
            # Determine order status based on dates
            current_date = datetime.now()
            
            if order_date > current_date:
                # Future orders are typically in Draft or Pending status
                status = random.choice(["Draft", "Pending"])
            elif requested_delivery_date > current_date:
                # Current orders are In Process or Confirmed
                status = random.choice(["In Process", "Confirmed", "Partially Shipped"])
            else:
                # Past orders are Completed, Cancelled, or On Hold
                status_options = ["Completed", "Completed", "Completed", "Cancelled", "On Hold"]  # Weighted for more completed
                status = random.choice(status_options)
                
            data["status"].append(status)
            
            # Set priority (weighted random)
            priority = random.choices(priority_levels, weights=priority_weights)[0]
            
            # Rush orders typically have higher priority
            if order_type == "Rush" and priority > 2:
                priority = random.randint(1, 2)
                
            data["priority"].append(priority)
            
            # Generate order value (based on customer credit limit as a rough guide)
            if 'credit_limit' in customer:
                max_order = customer['credit_limit'] * 0.5  # Typically orders are less than 50% of credit limit
            else:
                max_order = 50000
                
            order_value = random.uniform(1000, max_order)
            data["order_value"].append(round(order_value, 2))
            
            # Set payment terms (use customer terms if available)
            if 'credit_terms' in customer and pd.notna(customer['credit_terms']):
                data["payment_terms"].append(customer['credit_terms'])
            else:
                data["payment_terms"].append(random.choice(payment_terms))
            
            # Set shipping method
            data["shipping_method"].append(random.choice(shipping_methods))
            
            # Assign sales rep
            data["sales_rep_id"].append(random.choice(sales_rep_ids))
            
            # Generate notes (mostly empty)
            if random.random() < 0.2:  # 20% chance of having notes
                notes_options = [
                    "Customer requested special packaging",
                    "Delivery must be on exact date",
                    "Call customer before shipping",
                    "Include certificates of analysis",
                    "Partial shipments acceptable",
                    "Do not substitute products",
                    f"Reference PO #{random.randint(10000, 99999)}",
                    "Preferred carrier requested",
                    "Weekend delivery authorized",
                    "Contact warehouse manager upon arrival"
                ]
                data["notes"].append(random.choice(notes_options))
            else:
                data["notes"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "customer_orders.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.customer_orders_df = df
        self.order_ids = df["order_id"].tolist()
        
        print(f"Saved {num_orders} customer order records to {output_file}")
        
        return df

    def generate_order_lines(self, num_lines=None):
        """
        Generate synthetic data for the OrderLines table.
        
        Parameters:
        - num_lines: Number of order line records to generate (auto-calculated if None)
        
        Returns:
        - DataFrame containing the generated order lines data
        """
        if self.customer_orders_df is None or len(self.customer_orders_df) == 0:
            print("Error: No customer orders data available. Generate customer orders first.")
            return None
        
        # Generate data structure
        data = {
            "line_id": [],
            "order_id": [],
            "line_number": [],
            "product_id": [],
            "quantity": [],
            "unit_price": [],
            "line_value": [],
            "requested_delivery_date": [],
            "promised_delivery_date": [],
            "status": [],
            "work_order_id": [],
            "shipped_quantity": [],
            "shipping_date": []
        }
        
        # Process each customer order
        for _, order in self.customer_orders_df.iterrows():
            order_id = order['order_id']
            order_status = order['status']
            
            # Determine number of line items for this order
            num_lines_per_order = random.randint(1, 10)  # 1-10 line items per order
            
            # Keep track of selected products for this order to avoid duplicates
            selected_products = []
            
            # Get order dates
            order_date = pd.to_datetime(order['order_date'])
            requested_delivery_date = pd.to_datetime(order['requested_delivery_date'])
            promised_delivery_date = pd.to_datetime(order['promised_delivery_date'])
            
            # Generate line items
            for line_num in range(1, num_lines_per_order + 1):
                # Create unique line ID
                line_id = f"LINE-{uuid.uuid4().hex[:8].upper()}"
                data["line_id"].append(line_id)
                data["order_id"].append(order_id)
                data["line_number"].append(line_num)
                
                # Select product (avoid duplicates within same order)
                if self.products_df is not None and len(self.products_df) > 0:
                    available_products = [p for p in self.product_ids if p not in selected_products]
                    
                    if not available_products:
                        # If we've used all products, just pick a random one
                        product_id = random.choice(self.product_ids)
                    else:
                        product_id = random.choice(available_products)
                        selected_products.append(product_id)
                else:
                    # Create synthetic product IDs if no products data available
                    product_id = f"PROD-{uuid.uuid4().hex[:8].upper()}"
                    
                data["product_id"].append(product_id)
                
                # Generate quantity
                quantity = random.randint(1, 1000)
                data["quantity"].append(quantity)
                
                # Get unit price
                if self.products_df is not None and len(self.products_df) > 0:
                    # Look up the price from products data
                    product_data = self.products_df[self.products_df['product_id'] == product_id]
                    if len(product_data) > 0 and 'list_price' in product_data.columns:
                        unit_price = product_data.iloc[0]['list_price']
                    else:
                        unit_price = random.uniform(10, 1000)
                else:
                    unit_price = random.uniform(10, 1000)
                    
                # Apply random discount/markup
                price_adjustment = random.uniform(0.9, 1.1)  # -10% to +10%
                unit_price = unit_price * price_adjustment
                
                data["unit_price"].append(round(unit_price, 2))
                
                # Calculate line value
                line_value = quantity * unit_price
                data["line_value"].append(round(line_value, 2))
                
                # Set delivery dates (can vary slightly from order dates for individual lines)
                line_req_variation = random.randint(-3, 3)  # +/- 3 days
                line_requested_date = requested_delivery_date + timedelta(days=line_req_variation)
                
                line_prom_variation = random.randint(-2, 2)  # +/- 2 days
                line_promised_date = promised_delivery_date + timedelta(days=line_prom_variation)
                
                # Ensure dates make sense
                if line_requested_date < order_date:
                    line_requested_date = order_date + timedelta(days=1)
                    
                if line_promised_date < order_date:
                    line_promised_date = order_date + timedelta(days=1)
                    
                data["requested_delivery_date"].append(line_requested_date.strftime("%Y-%m-%d"))
                data["promised_delivery_date"].append(line_promised_date.strftime("%Y-%m-%d"))
                
                # Set line status based on order status
                if order_status == "Draft" or order_status == "Pending":
                    line_status = order_status
                    data["work_order_id"].append("")
                    data["shipped_quantity"].append(0)
                    data["shipping_date"].append("")
                    
                elif order_status == "Confirmed":
                    line_status = "Confirmed"
                    
                    # Some confirmed orders have work orders
                    if random.random() < 0.7:  # 70% chance
                        if self.work_order_ids and len(self.work_order_ids) > 0:
                            data["work_order_id"].append(random.choice(self.work_order_ids))
                        else:
                            data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
                    else:
                        data["work_order_id"].append("")
                        
                    data["shipped_quantity"].append(0)
                    data["shipping_date"].append("")
                    
                elif order_status == "In Process":
                    line_statuses = ["Confirmed", "In Production", "Ready to Ship", "Partially Shipped"]
                    line_status = random.choice(line_statuses)
                    
                    # Most in-process lines have work orders
                    if random.random() < 0.9:  # 90% chance
                        if self.work_order_ids and len(self.work_order_ids) > 0:
                            data["work_order_id"].append(random.choice(self.work_order_ids))
                        else:
                            data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
                    else:
                        data["work_order_id"].append("")
                    
                    # Some lines may be partially shipped
                    if line_status == "Partially Shipped":
                        shipped_qty = random.randint(1, quantity - 1)
                        data["shipped_quantity"].append(shipped_qty)
                        
                        # Shipping date is between order date and current date
                        days_difference = (datetime.now() - order_date).days
                        if days_difference >= 1:
                            ship_days = random.randint(1, days_difference)
                            shipping_date = order_date + timedelta(days=ship_days)
                            data["shipping_date"].append(shipping_date.strftime("%Y-%m-%d"))
                        else:
                            # Handle the case when order_date is today or in the future
                            data["shipping_date"].append("")
                    else:
                        data["shipped_quantity"].append(0)
                        data["shipping_date"].append("")
                    
                elif order_status == "Partially Shipped":
                    # Mix of shipped and unshipped lines
                    if random.random() < 0.6:  # 60% chance this line is shipped
                        line_status = "Shipped"
                        data["shipped_quantity"].append(quantity)
                        
                        # Shipping date is between order date and current date
                        days_difference = (datetime.now() - order_date).days
                        if days_difference >= 1:
                            ship_days = random.randint(1, days_difference)
                            shipping_date = order_date + timedelta(days=ship_days)
                            data["shipping_date"].append(shipping_date.strftime("%Y-%m-%d"))
                        else:
                            # Handle the case when order_date is today or in the future
                            data["shipping_date"].append("")
                    else:
                        line_status = random.choice(["Confirmed", "In Production", "Ready to Ship"])
                        data["shipped_quantity"].append(0)
                        data["shipping_date"].append("")
                    
                    # Most lines have work orders
                    if random.random() < 0.9:  # 90% chance
                        if self.work_order_ids and len(self.work_order_ids) > 0:
                            data["work_order_id"].append(random.choice(self.work_order_ids))
                        else:
                            data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
                    else:
                        data["work_order_id"].append("")
                    
                elif order_status == "Completed":
                    line_status = "Shipped"
                    data["shipped_quantity"].append(quantity)
                    
                    # Shipping date is between order date and promised date
                    ship_days = random.randint(1, (promised_delivery_date - order_date).days)
                    shipping_date = order_date + timedelta(days=ship_days)
                    data["shipping_date"].append(shipping_date.strftime("%Y-%m-%d"))
                    
                    # Most completed lines have work orders
                    if random.random() < 0.95:  # 95% chance
                        if self.work_order_ids and len(self.work_order_ids) > 0:
                            data["work_order_id"].append(random.choice(self.work_order_ids))
                        else:
                            data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
                    else:
                        data["work_order_id"].append("")
                    
                elif order_status == "Cancelled":
                    line_status = "Cancelled"
                    data["work_order_id"].append("")
                    data["shipped_quantity"].append(0)
                    data["shipping_date"].append("")
                    
                else:  # On Hold
                    line_status = "On Hold"
                    
                    # Some on-hold orders have work orders
                    if random.random() < 0.4:  # 40% chance
                        if self.work_order_ids and len(self.work_order_ids) > 0:
                            data["work_order_id"].append(random.choice(self.work_order_ids))
                        else:
                            data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
                    else:
                        data["work_order_id"].append("")
                        
                    data["shipped_quantity"].append(0)
                    data["shipping_date"].append("")
                
                data["status"].append(line_status)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "order_lines.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.order_lines_df = df
        
        print(f"Saved {len(df)} order line records for {df['order_id'].nunique()} orders to {output_file}")
        
        return df
        
    def generate_suppliers(self, num_suppliers=50, output_file="data/suppliers.csv"):
        """
        Generate synthetic data for the Suppliers table from ISA-95 Level 4.
        
        Parameters:
        - num_suppliers: Number of supplier records to generate
        - output_file: CSV file to save the data
        
        Returns:
        - DataFrame containing the generated suppliers data
        """

        # Set output file path based on output directory
        output_file = os.path.join(self.output_dir, "suppliers.csv")

        # Define supplier types and their probabilities
        supplier_types = {
            "Manufacturer": 0.4,
            "Distributor": 0.25,
            "Wholesaler": 0.15,
            "Service Provider": 0.1,
            "Contractor": 0.1
        }
        
        # Define supplier categories by material type
        supplier_categories = {
            "Raw Material": ["Chemical Supplier", "Industrial Raw Material", "Commodity Supplier", 
                        "Mining Company", "Agricultural Supplier", "Petroleum Supplier"],
            "Packaging": ["Packaging Manufacturer", "Container Supplier", "Label Supplier", 
                    "Film Supplier", "Box Manufacturer", "Bottle Supplier"],
            "Equipment": ["Equipment Manufacturer", "Machinery Supplier", "Tool Vendor", 
                    "Instrumentation Supplier", "Automation Provider", "Parts Supplier"],
            "Service": ["Maintenance Service", "Calibration Service", "Cleaning Service", 
                    "Engineering Consultant", "Testing Laboratory", "Transportation Provider"],
            "Consumable": ["Laboratory Supplies", "MRO Supplier", "Office Supplies", 
                        "Safety Equipment", "Utility Provider", "IT Service Provider"]
        }
        
        # Define payment terms
        payment_terms = ["Net 30", "Net 45", "Net 60", "2/10 Net 30", "COD", "Net 15"]
        payment_terms_weights = [0.4, 0.2, 0.15, 0.1, 0.1, 0.05]  # Probabilities
        
        # Define regions for address generation
        regions = {
            "North America": ["USA", "Canada", "Mexico"],
            "Europe": ["UK", "Germany", "France", "Italy", "Spain", "Netherlands"],
            "Asia": ["Japan", "China", "South Korea", "India", "Singapore", "Taiwan"],
            "Latin America": ["Brazil", "Colombia", "Argentina", "Chile", "Peru"],
            "Oceania": ["Australia", "New Zealand"]
        }
        
        # Region probabilities (adjust as needed for your supply chain model)
        region_weights = {
            "North America": 0.35,
            "Europe": 0.25,
            "Asia": 0.25,
            "Latin America": 0.1,
            "Oceania": 0.05
        }
        
        # Generate data structure
        data = {
            "supplier_id": [f"SUP-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_suppliers)],
            "supplier_name": [],
            "supplier_type": [],
            "contact_person": [],
            "email": [],
            "phone": [],
            "address": [],
            "payment_terms": [],
            "lead_time_days": [],
            "quality_rating": [],
            "status": [],
            "primary_materials": [],
            "notes": []
        }
        
        # First names for contact generation
        first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", 
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa", 
            "Matthew", "Margaret", "Anthony", "Betty", "Mark", "Sandra", "Donald", "Ashley", 
            "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle"
        ]
        
        # Last names for contact generation
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", 
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", 
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", 
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker"
        ]
        
        # Company name components for generating realistic company names
        company_prefixes = [
            "Advanced", "Allied", "American", "Asian", "Atlantic", "Best", "Better", "Blue", 
            "Bright", "Central", "Century", "Consolidated", "Continental", "Digital", "Dynamic", 
            "East", "Eastern", "Euro", "European", "Express", "First", "Global", "Golden", 
            "Great", "Green", "International", "Metro", "Modern", "National", "New", "North"
        ]
        
        company_types = {
            "Manufacturer": ["Manufacturing", "Industries", "Fabrication", "Production", "Processors"],
            "Distributor": ["Distribution", "Distributors", "Supply", "Supplies", "Logistics"],
            "Wholesaler": ["Wholesale", "Trading", "Merchandise", "Commerce", "Exchange"],
            "Service Provider": ["Services", "Solutions", "Consulting", "Associates", "Group"],
            "Contractor": ["Contracting", "Construction", "Engineering", "Installations", "Systems"]
        }
        
        # Generate data for each supplier
        for i in range(num_suppliers):
            # Select supplier type (weighted random)
            supplier_type = random.choices(
                list(supplier_types.keys()), 
                weights=list(supplier_types.values())
            )[0]
            data["supplier_type"].append(supplier_type)
            
            # Generate a realistic company name
            if random.random() < 0.7:  # 70% chance of using prefix
                prefix = random.choice(company_prefixes)
                if supplier_type in company_types:
                    suffix = random.choice(company_types[supplier_type])
                else:
                    suffix = random.choice(list(company_types.values())[0])
                    
                company_name = f"{prefix} {suffix}"
            else:
                # Use a last name
                last_name = random.choice(last_names)
                if supplier_type in company_types:
                    suffix = random.choice(company_types[supplier_type])
                else:
                    suffix = random.choice(list(company_types.values())[0])
                    
                company_name = f"{last_name} {suffix}"
            
            data["supplier_name"].append(company_name)
            
            # Generate contact person (random first and last name)
            contact_first = random.choice(first_names)
            contact_last = random.choice(last_names)
            data["contact_person"].append(f"{contact_first} {contact_last}")
            
            # Generate email (company domain based on name)
            company_domain = company_name.lower().replace(" ", "").replace(".", "")
            email_domains = [".com", ".net", ".org", ".co", ".biz"]
            email_domain = random.choice(email_domains)
            data["email"].append(f"{contact_first.lower()}.{contact_last.lower()}@{company_domain}{email_domain}")
            
            # Generate phone
            data["phone"].append(f"+{random.randint(1, 9)}{random.randint(10, 99)} {random.randint(100, 999)} {random.randint(100, 999)} {random.randint(1000, 9999)}")
            
            # Generate address based on region probability
            region = random.choices(
                list(region_weights.keys()), 
                weights=list(region_weights.values())
            )[0]
            
            country = random.choice(regions[region])
            
            # Generate a city name (simplified)
            city_prefixes = ["New", "Old", "East", "West", "North", "South", "Central", "Upper", "Lower", "Port", "Lake", "Mount", "Fort"]
            city_suffixes = ["town", "ville", "burg", "berg", "field", "ford", "port", "mouth", "stad", "furt", "chester", "cester", "bridge", "haven", "minster"]
            
            if random.random() < 0.3:  # 30% chance of using prefix
                city = f"{random.choice(city_prefixes)} {random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            else:
                city = f"{random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            
            # Generate street address
            street_number = random.randint(1, 9999)
            street_types = ["Street", "Avenue", "Boulevard", "Road", "Lane", "Drive", "Way", "Place", "Court", "Terrace"]
            street_name = f"{random.choice(last_names)} {random.choice(street_types)}"
            
            address = f"{street_number} {street_name}, {city}, {country}"
            data["address"].append(address)
            
            # Set payment terms (weighted random)
            payment_term = random.choices(payment_terms, weights=payment_terms_weights)[0]
            data["payment_terms"].append(payment_term)
            
            # Set lead time based on supplier type and region
            if supplier_type in ["Manufacturer", "Contractor"]:
                base_lead_time = random.randint(30, 90)  # Longer lead times for manufacturers
            else:
                base_lead_time = random.randint(7, 45)   # Shorter for distributors
                
            # Adjust for region (international suppliers have longer lead times)
            if region == "North America":
                lead_time_multiplier = 1.0
            elif region == "Europe" or region == "Oceania":
                lead_time_multiplier = 1.5
            elif region == "Asia":
                lead_time_multiplier = 1.8
            else:
                lead_time_multiplier = 1.3
                
            lead_time = int(base_lead_time * lead_time_multiplier)
            data["lead_time_days"].append(lead_time)
            
            # Set quality rating (1-5 scale, 5 being best)
            # Most suppliers should be good (3-5) with fewer poor suppliers
            quality_weights = [0.05, 0.15, 0.30, 0.35, 0.15]  # Weights for ratings 1-5
            quality_rating = round(random.choices([1, 2, 3, 4, 5], weights=quality_weights)[0], 1)
            
            # Add some random decimal to make it more realistic
            if quality_rating < 5:
                quality_rating += round(random.uniform(0, 0.9), 1)
                
            data["quality_rating"].append(quality_rating)
            
            # Set status (mostly active)
            statuses = ["Active", "Inactive", "On Hold", "New", "Disqualified"]
            status_weights = [0.8, 0.05, 0.05, 0.07, 0.03]  # Probabilities
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Set primary materials/categories
            num_categories = random.randint(1, 3)  # 1-3 primary categories per supplier
            categories = []
            
            # Select based on supplier type
            if supplier_type == "Manufacturer":
                category_pool = list(supplier_categories["Raw Material"]) + list(supplier_categories["Equipment"])
            elif supplier_type == "Distributor":
                category_pool = list(supplier_categories["Raw Material"]) + list(supplier_categories["Packaging"]) + list(supplier_categories["Consumable"])
            elif supplier_type == "Service Provider":
                category_pool = list(supplier_categories["Service"])
            else:
                # Mix of all categories
                category_pool = []
                for cat_list in supplier_categories.values():
                    category_pool.extend(cat_list)
                    
            categories = random.sample(category_pool, min(num_categories, len(category_pool)))
            data["primary_materials"].append(str(categories))
            
            # Generate notes (mostly empty)
            if random.random() < 0.3:  # 30% chance of having notes
                notes_options = [
                    "Preferred supplier for critical materials",
                    "Requires minimum order quantities",
                    "ISO 9001 certified",
                    "Long-term contract in place",
                    f"Annual review scheduled for Q{random.randint(1, 4)}",
                    "Sustainability certified",
                    "Offers volume discounts",
                    "Approved for regulated materials",
                    "Subject to import restrictions",
                    "Can provide rush delivery"
                ]
                data["notes"].append(random.choice(notes_options))
            else:
                data["notes"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.suppliers_df = df
        self.supplier_ids = df["supplier_id"].tolist()

        print(f"Successfully generated {len(df)} supplier records.")
        print(f"Data saved to {output_file}")
        
        return df
    
    def generate_purchase_orders(self,num_orders=200, start_time=None, end_time=None):
        """
        Generate synthetic data for the PurchaseOrders table from ISA-95 Level 4.
        
        Parameters:
        - suppliers_df: DataFrame containing suppliers data
        - materials_df: DataFrame containing materials data (optional)
        - personnel_df: DataFrame containing personnel data (optional)
        - num_orders: Number of purchase order records to generate
        - start_time: Start time for order dates (defaults to 365 days ago)
        - end_time: End time for order dates (defaults to 30 days in the future)
        - output_file: CSV file to save the data
        
        Returns:
        - DataFrame containing the generated purchase orders data
        """
        # Set output file path
        output_file = os.path.join(self.output_dir, "purchase_orders.csv")
        
        # Use self.suppliers_df instead of suppliers_df parameter
        suppliers_df = self.suppliers_df
        
        # Use self.materials_df if available
        materials_df = self.materials_df if hasattr(self, 'materials_df') else None
        
        # Use self.personnel_ids if available
        personnel_df = None  # Assuming personnel_df is not stored in the class

        if suppliers_df is None or len(suppliers_df) == 0:
            print("Error: No suppliers data available.")
            return None

        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now() + timedelta(days=30)
        
        # Generate buyer IDs from existing personnel
        if not self.personnel_ids or len(self.personnel_ids) == 0:
            raise ValueError("Personnel data must be generated before purchase orders. No personnel IDs available for buyers.")
            
        # Use existing personnel IDs - select those likely to be buyers
        buyer_ids = []
        if hasattr(self, 'personnel_df') and self.personnel_df is not None:
            # Try to find personnel in purchasing-related departments
            purchasing_personnel = self.personnel_df[
                self.personnel_df['department'].isin(['Supply Chain', 'Finance', 'Administration'])
            ]
            if len(purchasing_personnel) >= 5:
                buyer_ids = purchasing_personnel['personnel_id'].tolist()
            else:
                # If not enough purchasing personnel, use any available
                buyer_ids = self.personnel_df['personnel_id'].tolist()
        else:
            # Fallback to stored personnel IDs
            buyer_ids = self.personnel_ids

        # Ensure we have at least some buyers
        if len(buyer_ids) == 0:
            raise ValueError("No personnel available to assign as buyers")
        
        # Define payment terms (use supplier terms or defaults)
        payment_terms = ["Net 30", "Net 45", "Net 60", "2/10 Net 30", "COD", "Prepaid"]
        
        # Define shipping methods
        shipping_methods = ["Truck", "Air", "Sea", "Rail", "Express", "Courier", "Supplier Delivery"]
        
        # Define approval statuses
        approval_statuses = ["Draft", "Pending Approval", "Approved", "Rejected", "On Hold"]
        
        # Generate data structure
        data = {
            "po_id": [f"PO-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_orders)],
            "supplier_id": [],
            "order_date": [],
            "expected_delivery_date": [],
            "status": [],
            "total_value": [],
            "payment_terms": [],
            "shipping_method": [],
            "buyer_id": [],
            "approval_status": [],
            "notes": []
        }
        
        # Generate data for each purchase order
        for i in range(num_orders):
            # Select supplier (more active suppliers get more orders)
            active_suppliers = suppliers_df[suppliers_df['status'] == 'Active']
            
            if len(active_suppliers) > 0:
                # Prefer active suppliers (80% chance)
                if random.random() < 0.8:
                    supplier = active_suppliers.sample(1).iloc[0]
                else:
                    supplier = suppliers_df.sample(1).iloc[0]
            else:
                supplier = suppliers_df.sample(1).iloc[0]
                
            supplier_id = supplier['supplier_id']
            data["supplier_id"].append(supplier_id)
            
            # Generate order date
            time_range_days = (end_time - start_time).days
            days_from_start = random.randint(0, time_range_days)
            order_date = start_time + timedelta(days=days_from_start)
            data["order_date"].append(order_date.strftime("%Y-%m-%d"))
            
            # Generate expected delivery date based on supplier lead time
            if 'lead_time_days' in supplier and pd.notna(supplier['lead_time_days']):
                lead_time = supplier['lead_time_days']
            else:
                # Default lead time if not available
                lead_time = random.randint(14, 60)
                
            # Add some variation to the lead time
            lead_time_variation = random.uniform(0.8, 1.2)  # +/- 20%
            adjusted_lead_time = int(lead_time * lead_time_variation)
            
            expected_delivery_date = order_date + timedelta(days=adjusted_lead_time)
            data["expected_delivery_date"].append(expected_delivery_date.strftime("%Y-%m-%d"))
            
            # Determine PO status based on dates
            current_date = datetime.now()
            
            if order_date > current_date:
                # Future POs are typically in Draft or Pending status
                status = random.choice(["Draft", "Pending Approval"])
            elif expected_delivery_date > current_date:
                # Current POs are Approved or In Process
                status = random.choice(["Approved", "In Process", "Partially Received"])
            else:
                # Past POs are Completed, Cancelled, or Closed
                status_options = ["Completed", "Completed", "Completed", "Cancelled", "Closed"]  # Weighted for more completed
                status = random.choice(status_options)
                
            data["status"].append(status)
            
            # Generate order value (based on a reasonable range for purchase orders)
            order_value = random.uniform(1000, 50000)
            data["total_value"].append(round(order_value, 2))
            
            # Set payment terms (use supplier terms if available)
            if 'payment_terms' in supplier and pd.notna(supplier['payment_terms']):
                data["payment_terms"].append(supplier['payment_terms'])
            else:
                data["payment_terms"].append(random.choice(payment_terms))
            
            # Set shipping method
            data["shipping_method"].append(random.choice(shipping_methods))
            
            # Assign buyer
            data["buyer_id"].append(random.choice(buyer_ids))
            
            # Set approval status based on PO status
            if status == "Draft":
                approval_status = "Draft"
            elif status == "Pending Approval":
                approval_status = "Pending Approval"
            elif status in ["Cancelled", "Rejected"]:
                approval_status = "Rejected"
            elif status == "On Hold":
                approval_status = "On Hold"
            else:
                approval_status = "Approved"
                
            data["approval_status"].append(approval_status)
            
            # Generate notes (mostly empty)
            if random.random() < 0.2:  # 20% chance of having notes
                notes_options = [
                    "Rush order, critical materials",
                    "Partial shipments acceptable",
                    "Quality certificates required",
                    "Special packaging instructions included",
                    "Price negotiated below standard",
                    "Consolidated order for multiple projects",
                    f"Reference requisition #{random.randint(10000, 99999)}",
                    "Schedule delivery with warehouse manager",
                    "New supplier, additional quality checks",
                    "Replacement for PO cancelled last month"
                ]
                data["notes"].append(random.choice(notes_options))
            else:
                data["notes"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.purchase_orders_df = df
        self.purchase_order_ids = df["po_id"].tolist()

        print(f"Successfully generated {len(df)} purchase order records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_purchase_order_lines(self, num_lines=None):
        """
        Generate synthetic data for the PurchaseOrderLines table from ISA-95 Level 4.
        
        Parameters:
        - num_lines: Number of purchase order line records to generate (auto-calculated if None)
        
        Returns:
        - DataFrame containing the generated purchase order lines data
        """
        if self.purchase_orders_df is None or len(self.purchase_orders_df) == 0:
            print("Error: No purchase orders data available.")
            return None
        
        # Generate material IDs if materials_df is not provided
        if self.materials_df is None or len(self.materials_df) == 0:
            print("Generating synthetic material IDs...")
            material_ids = [f"MAT-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
            
            # Create synthetic material prices
            material_prices = {}
            for mat_id in material_ids:
                material_prices[mat_id] = random.uniform(10, 1000)
        else:
            # Use actual material IDs from materials_df
            material_ids = self.materials_df['material_id'].tolist()
            
            # Create price mapping from standard cost
            material_prices = {}
            for _, material in self.materials_df.iterrows():
                if 'standard_cost' in material and pd.notna(material['standard_cost']):
                    material_prices[material['material_id']] = material['standard_cost']
                else:
                    material_prices[material['material_id']] = random.uniform(10, 1000)
        
        # Generate data structure
        data = {
            "line_id": [],
            "po_id": [],
            "line_number": [],
            "material_id": [],
            "quantity": [],
            "unit_price": [],
            "line_value": [],
            "expected_delivery_date": [],
            "received_quantity": [],
            "receipt_date": [],
            "status": [],
            "lot_id": []
        }
        
        # Process each purchase order
        for _, po in self.purchase_orders_df.iterrows():
            po_id = po['po_id']
            po_status = po['status']
            
            # Determine number of line items for this PO
            num_lines_per_po = random.randint(1, 10)  # 1-10 line items per PO
            
            # Keep track of selected materials for this PO to avoid duplicates
            selected_materials = []
            
            # Get PO dates
            order_date = pd.to_datetime(po['order_date'])
            expected_delivery_date = pd.to_datetime(po['expected_delivery_date'])
            
            # Generate line items
            for line_num in range(1, num_lines_per_po + 1):
                # Create unique line ID
                line_id = f"POLINE-{uuid.uuid4().hex[:8].upper()}"
                data["line_id"].append(line_id)
                data["po_id"].append(po_id)
                data["line_number"].append(line_num)
                
                # Select material (avoid duplicates within same PO)
                available_materials = [m for m in material_ids if m not in selected_materials]
                
                if not available_materials:
                    # If we've used all materials, just pick a random one
                    material_id = random.choice(material_ids)
                else:
                    material_id = random.choice(available_materials)
                    selected_materials.append(material_id)
                    
                data["material_id"].append(material_id)
                
                # Generate quantity based on material (would depend on unit of measure)
                # For simplicity we'll use generic quantities
                quantity = random.randint(1, 1000)
                data["quantity"].append(quantity)
                
                # Get unit price
                if material_id in material_prices:
                    unit_price = material_prices[material_id]
                else:
                    unit_price = random.uniform(10, 1000)
                    
                # Apply random variation (supplier-specific pricing)
                price_variation = random.uniform(0.9, 1.1)  # +/- 10%
                unit_price = unit_price * price_variation
                
                data["unit_price"].append(round(unit_price, 2))
                
                # Calculate line value
                line_value = quantity * unit_price
                data["line_value"].append(round(line_value, 2))
                
                # Set expected delivery date (can vary slightly from PO date)
                line_delivery_variation = random.randint(-5, 5)  # +/- 5 days
                line_delivery_date = expected_delivery_date + timedelta(days=line_delivery_variation)
                
                # Ensure date makes sense
                if line_delivery_date < order_date:
                    line_delivery_date = order_date + timedelta(days=1)
                    
                data["expected_delivery_date"].append(line_delivery_date.strftime("%Y-%m-%d"))
                
                # Set line status and receipt info based on PO status
                if po_status in ["Draft", "Pending Approval"]:
                    line_status = po_status
                    data["received_quantity"].append(0)
                    data["receipt_date"].append("")
                    data["lot_id"].append("")
                    
                elif po_status == "Approved":
                    line_status = "Approved"
                    data["received_quantity"].append(0)
                    data["receipt_date"].append("")
                    data["lot_id"].append("")
                    
                elif po_status == "In Process":
                    line_status = "In Process"
                    data["received_quantity"].append(0)
                    data["receipt_date"].append("")
                    data["lot_id"].append("")
                    
                elif po_status == "Partially Received":
                    # Mix of received and unreceived lines
                    if random.random() < 0.6:  # 60% chance this line is received
                        line_status = "Received"
                        received_qty = quantity
                        
                        # Receipt date is between order date and current date
                        days_difference = (datetime.now() - order_date).days
                        if days_difference >= 1:
                            receipt_days = random.randint(1, days_difference)
                            receipt_date = order_date + timedelta(days=receipt_days)
                            data["receipt_date"].append(receipt_date.strftime("%Y-%m-%d"))
                        else:
                            data["receipt_date"].append(datetime.now().strftime("%Y-%m-%d"))
                        
                        # Generate a lot ID for the received material
                        data["lot_id"].append("")

                    else:
                        line_status = "In Process"
                        received_qty = 0
                        data["receipt_date"].append("")
                        data["lot_id"].append("")
                        
                    data["received_quantity"].append(received_qty)
                    
                elif po_status == "Completed":
                    line_status = "Received"
                    data["received_quantity"].append(quantity)
                    
                    # Receipt date is between order date and expected delivery date
                    receipt_days = random.randint(1, max(1, (expected_delivery_date - order_date).days))
                    receipt_date = order_date + timedelta(days=receipt_days)
                    data["receipt_date"].append(receipt_date.strftime("%Y-%m-%d"))
                    
                    # Generate a lot ID for the received material
                    data["lot_id"].append("")

                    
                elif po_status == "Cancelled":
                    line_status = "Cancelled"
                    data["received_quantity"].append(0)
                    data["receipt_date"].append("")
                    data["lot_id"].append("")
                    
                else:  # Closed or other status
                    line_status = po_status
                    data["received_quantity"].append(quantity)
                    
                    # Receipt date is between order date and expected delivery date
                    receipt_days = random.randint(1, max(1, (expected_delivery_date - order_date).days))
                    receipt_date = order_date + timedelta(days=receipt_days)
                    data["receipt_date"].append(receipt_date.strftime("%Y-%m-%d"))
                    
                    # Generate a lot ID for the received material
                    data["lot_id"].append("")

                
                data["status"].append(line_status)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        output_file = os.path.join(self.output_dir, "purchase_order_lines.csv")
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.purchase_order_lines_df = df
        
        print(f"Successfully generated {len(df)} purchase order line records for {df['po_id'].nunique()} purchase orders.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_production_schedules(self, num_schedules=20, start_time=None, end_time=None):
        """
        Generate synthetic data for the ProductionSchedules table from ISA-95 Level 4.
        
        Parameters:
        - num_schedules: Number of production schedule records to generate
        - start_time: Start time for schedule dates (defaults to 180 days ago)
        - end_time: End time for schedule dates (defaults to 180 days in the future)
        
        Returns:
        - DataFrame containing the generated production schedules data
        """
        if self.products_df is None or len(self.products_df) == 0:
            print("Error: No products data available.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now() + timedelta(days=180)
        
        # Generate facility IDs if facilities_df is not provided
        if not self.facility_ids:
            print("Generating synthetic facility IDs...")
            facility_ids = [f"FAC-{uuid.uuid4().hex[:8].upper()}" for _ in range(5)]
        else:
            facility_ids = self.facility_ids
        
        # Generate creator IDs from existing personnel
        if not self.personnel_ids or len(self.personnel_ids) == 0:
            raise ValueError("Personnel data must be generated before production schedules. No personnel IDs available for creators.")
            
        # Use existing personnel IDs - prefer those in planning roles
        creator_ids = []
        if hasattr(self, 'personnel_df') and self.personnel_df is not None:
            # Try to find personnel in planning-related departments
            planning_personnel = self.personnel_df[
                self.personnel_df['department'].isin(['Production', 'Supply Chain', 'Engineering'])
            ]
            if len(planning_personnel) >= 5:
                creator_ids = planning_personnel['personnel_id'].tolist()
            else:
                # If not enough planning personnel, use any available
                creator_ids = self.personnel_df['personnel_id'].tolist()
        else:
            creator_ids = self.personnel_ids

        if len(creator_ids) == 0:
            raise ValueError("No personnel available to assign as schedule creators")
        
        # Define schedule types
        schedule_types = ["Master", "Detailed", "Production", "Campaign", "Weekly", "Monthly", "Quarterly"]
        
        # Generate data structure
        data = {
            "schedule_id": [f"PS-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_schedules)],
            "schedule_name": [],
            "schedule_type": [],
            "facility_id": [],
            "start_date": [],
            "end_date": [],
            "creation_date": [],
            "created_by": [],
            "status": [],
            "revision": [],
            "freeze_horizon_days": [],
            "notes": []
        }
        
        # Define schedule statuses
        statuses = ["Draft", "Approved", "In Progress", "Completed", "Cancelled", "Superseded"]
        
        # Generate data for each production schedule
        for i in range(num_schedules):
            # Generate schedule name (descriptive)
            schedule_type = random.choice(schedule_types)
            
            # Create a date component for the name
            if schedule_type in ["Weekly", "Production"]:
                # Weekly schedules - named by week number
                week_num = random.randint(1, 52)
                year = random.randint(datetime.now().year - 1, datetime.now().year + 1)
                schedule_name = f"{schedule_type} Schedule - Week {week_num}, {year}"
            elif schedule_type == "Monthly":
                # Monthly schedules - named by month
                month_names = ["January", "February", "March", "April", "May", "June", 
                            "July", "August", "September", "October", "November", "December"]
                month = random.choice(month_names)
                year = random.randint(datetime.now().year - 1, datetime.now().year + 1)
                schedule_name = f"{month} {year} Production Schedule"
            elif schedule_type == "Quarterly":
                # Quarterly schedules
                quarter = random.randint(1, 4)
                year = random.randint(datetime.now().year - 1, datetime.now().year + 1)
                schedule_name = f"Q{quarter} {year} Production Plan"
            elif schedule_type == "Campaign":
                # Campaign schedules - named by product or campaign
                product_name = random.choice(self.products_df['product_name'].tolist())
                schedule_name = f"{product_name} Production Campaign"
            else:
                # Other schedule types
                time_period = random.choice(["Short-term", "Mid-term", "Long-term"])
                year = random.randint(datetime.now().year - 1, datetime.now().year + 1)
                schedule_name = f"{time_period} {schedule_type} Schedule {year}"
            
            data["schedule_name"].append(schedule_name)
            data["schedule_type"].append(schedule_type)
            
            # Assign facility
            data["facility_id"].append(random.choice(facility_ids))
            
            # Generate schedule dates based on type
            time_range_days = (end_time - start_time).days
            start_offset = random.randint(0, time_range_days - 1)
            schedule_start = start_time + timedelta(days=start_offset)
            
            # Schedule duration depends on type
            if schedule_type == "Weekly":
                duration_days = 7
            elif schedule_type == "Monthly":
                duration_days = 30
            elif schedule_type == "Quarterly":
                duration_days = 90
            elif schedule_type == "Campaign":
                duration_days = random.randint(14, 60)  # 2-8 weeks
            elif schedule_type == "Master":
                duration_days = random.randint(180, 365)  # 6-12 months
            else:
                duration_days = random.randint(30, 120)  # 1-4 months
                
            schedule_end = schedule_start + timedelta(days=duration_days)
            
            # Ensure end date is within range
            if schedule_end > end_time:
                schedule_end = end_time
                
            data["start_date"].append(schedule_start.strftime("%Y-%m-%d"))
            data["end_date"].append(schedule_end.strftime("%Y-%m-%d"))
            
            # Creation date is typically before start date
            creation_offset = random.randint(7, 60)  # 1-8 weeks before
            creation_date = schedule_start - timedelta(days=creation_offset)
            
            # Ensure creation date is not before data range start
            if creation_date < start_time:
                creation_date = start_time
                
            data["creation_date"].append(creation_date.strftime("%Y-%m-%d"))
            
            # Assign creator
            data["created_by"].append(random.choice(creator_ids))
            
            # Determine status based on dates
            current_date = datetime.now()
            
            if schedule_start > current_date:
                # Future schedules are typically Draft or Approved
                status = random.choice(["Draft", "Approved"])
            elif schedule_end < current_date:
                # Past schedules are Completed or Superseded
                status = random.choice(["Completed", "Completed", "Superseded"])  # Weight toward completed
            else:
                # Current schedules are In Progress
                status = "In Progress"
                
            # Some schedules might be cancelled
            if random.random() < 0.05:  # 5% chance
                status = "Cancelled"
                
            data["status"].append(status)
            
            # Set revision
            if status in ["Draft", "Approved"]:
                revision = random.randint(1, 3)  # Newer revisions
            elif status == "In Progress":
                revision = random.randint(1, 5)  # Might have several revisions
            else:
                revision = random.randint(1, 10)  # Completed schedules might have many revisions
                
            data["revision"].append(revision)
            
            # Set freeze horizon (period during which schedule cannot be changed)
            if schedule_type in ["Master", "Quarterly"]:
                freeze_horizon = random.randint(30, 60)  # 1-2 months
            elif schedule_type in ["Monthly", "Campaign"]:
                freeze_horizon = random.randint(14, 30)  # 2-4 weeks
            else:
                freeze_horizon = random.randint(3, 14)  # 3-14 days
                
            data["freeze_horizon_days"].append(freeze_horizon)
            
            # Generate notes (mostly empty)
            if random.random() < 0.3:  # 30% chance of having notes
                notes_options = [
                    "Adjusted for material availability",
                    "Optimized for equipment efficiency",
                    "Consolidated for resource utilization",
                    "Modified to accommodate rush orders",
                    "Updated based on inventory levels",
                    "Revised to match supplier deliveries",
                    "Balanced for labor utilization",
                    "Coordinated with maintenance schedule",
                    "Aligned with quality testing capacity",
                    "Considering seasonal demand factors"
                ]
                data["notes"].append(random.choice(notes_options))
            else:
                data["notes"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "production_schedules.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.production_schedules_df = df
        self.production_schedule_ids = df["schedule_id"].tolist()
        
        print(f"Successfully generated {len(df)} production schedule records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_personnel(self, num_personnel=50):
        """
        Generate synthetic data for the Personnel table.
        
        Parameters:
        - num_personnel: Number of personnel records to generate
        
        Returns:
        - DataFrame containing the generated personnel data
        """
        # Define departments
        departments = [
            "Production", "Quality", "Maintenance", "Supply Chain", "Engineering",
            "R&D", "Administration", "Finance", "IT", "Human Resources"
        ]
        
        # Define job titles by department
        job_titles = {
            "Production": ["Production Operator", "Line Lead", "Shift Supervisor", "Production Manager", "Manufacturing Director"],
            "Quality": ["QC Technician", "Quality Analyst", "QA Specialist", "Quality Manager", "Quality Director"],
            "Maintenance": ["Maintenance Technician", "Mechanical Engineer", "Electrical Engineer", "Maintenance Supervisor", "Maintenance Manager"],
            "Supply Chain": ["Warehouse Associate", "Logistics Coordinator", "Inventory Analyst", "Supply Chain Specialist", "Supply Chain Manager"],
            "Engineering": ["Process Engineer", "Manufacturing Engineer", "Project Engineer", "Engineering Supervisor", "Engineering Manager"],
            "R&D": ["R&D Scientist", "Research Associate", "Product Developer", "R&D Manager", "R&D Director"],
            "Administration": ["Administrative Assistant", "Office Coordinator", "Executive Assistant", "Office Manager", "Administrative Director"],
            "Finance": ["Accounting Clerk", "Financial Analyst", "Cost Accountant", "Controller", "Finance Director"],
            "IT": ["IT Support Specialist", "Systems Administrator", "Network Engineer", "Application Developer", "IT Manager"],
            "Human Resources": ["HR Assistant", "HR Specialist", "Recruiter", "HR Manager", "HR Director"]
        }
        
        # Define status options
        statuses = ["Active", "Inactive", "On Leave", "Terminated"]
        status_weights = [0.85, 0.05, 0.05, 0.05]  # Mostly active
        
        # Generate data structure
        data = {
            "personnel_id": [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_personnel)],
            "first_name": [],
            "last_name": [],
            "job_title": [],
            "department": [],
            "email": [],
            "phone": [],
            "facility_id": [],  # We'll keep this for reference but not output it
            "manager_id": [],
            "hire_date": [],
            "status": []
        }
        
        # First names pool
        first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
            "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", 
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
            "Matthew", "Margaret", "Anthony", "Betty", "Mark", "Sandra", "Donald", "Ashley",
            "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
            "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa"
        ]
        
        # Last names pool
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
            "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell"
        ]
        
        # Create organizational hierarchy
        # First, create executives and managers
        num_executives = min(5, num_personnel // 10)
        num_managers = min(10, num_personnel // 5)
        num_staff = num_personnel - num_executives - num_managers
        
        # Create a list to store personnel info for hierarchy setup
        hierarchy_info = []
        
        # Generate data for each personnel record
        for i in range(num_personnel):
            # Select first and last name
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            data["first_name"].append(first_name)
            data["last_name"].append(last_name)
            
            # Determine role level
            if i < num_executives:
                level = "Executive"
            elif i < num_executives + num_managers:
                level = "Manager"
            else:
                level = "Staff"
            
            # Select department
            department = random.choice(departments)
            data["department"].append(department)
            
            # Select job title based on department and level
            if level == "Executive":
                if department in ["Production", "R&D", "Finance", "HR", "IT"]:
                    title = f"{department} Director"
                else:
                    title = f"VP of {department}"
            elif level == "Manager":
                if department in job_titles:
                    # Use the last (most senior) job title from the department
                    title = job_titles[department][-1]
                else:
                    title = f"{department} Manager"
            else:
                if department in job_titles:
                    # Use one of the non-manager job titles
                    title = random.choice(job_titles[department][:-1])
                else:
                    title = f"{department} Specialist"
            
            data["job_title"].append(title)
            
            # Generate email
            email_domain = "company.com"
            email = f"{first_name.lower()}.{last_name.lower()}@{email_domain}"
            data["email"].append(email)
            
            # Generate phone (ensuring it's formatted properly)
            phone = f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            data["phone"].append(phone)
            
            # Assign to facility
            if self.facility_ids:
                # More senior people are more likely to be at headquarters (first facility)
                if level == "Executive" and random.random() < 0.8:
                    facility_id = self.facility_ids[0]
                else:
                    facility_id = random.choice(self.facility_ids)
                data["facility_id"].append(facility_id)
            else:
                data["facility_id"].append("")
            
            # Generate hire date (more senior people tend to have been hired earlier)
            current_year = datetime.now().year
            if level == "Executive":
                years_employed = random.randint(5, 20)
            elif level == "Manager":
                years_employed = random.randint(3, 15)
            else:
                years_employed = random.randint(0, 10)
                
            hire_year = current_year - years_employed
            hire_month = random.randint(1, 12)
            hire_day = random.randint(1, 28)  # Using 28 to avoid month-end issues
            hire_date = datetime(hire_year, hire_month, hire_day).strftime("%Y-%m-%d")
            data["hire_date"].append(hire_date)
            
            # Set status
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Store info for hierarchy setup
            hierarchy_info.append({
                "id": i,
                "personnel_id": data["personnel_id"][i],
                "level": level,
                "department": department
            })
        
        # Set manager relationships
        for i in range(num_personnel):
            if hierarchy_info[i]["level"] == "Executive":
                # Executives don't have managers in this dataset
                data["manager_id"].append("")
            elif hierarchy_info[i]["level"] == "Manager":
                # Managers report to executives in the same or related departments
                department = hierarchy_info[i]["department"]
                possible_managers = [p for p in hierarchy_info if p["level"] == "Executive"]
                if possible_managers:
                    manager = random.choice(possible_managers)
                    data["manager_id"].append(manager["personnel_id"])
                else:
                    data["manager_id"].append("")
            else:
                # Staff report to managers in the same department
                department = hierarchy_info[i]["department"]
                possible_managers = [p for p in hierarchy_info if p["level"] == "Manager" and p["department"] == department]
                if not possible_managers:
                    possible_managers = [p for p in hierarchy_info if p["level"] == "Manager"]
                
                if possible_managers:
                    manager = random.choice(possible_managers)
                    data["manager_id"].append(manager["personnel_id"])
                else:
                    data["manager_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Create a new DataFrame with only the columns from the DDL
        ddl_columns = ["personnel_id", "first_name", "last_name", "job_title", "department", 
                    "email", "phone", "hire_date", "status", "manager_id"]
        
        output_df = df[ddl_columns]
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "personnel.csv")
        output_df.to_csv(output_file, index=False)
        
        # Store the full df for later use
        self.personnel_df = df
        self.personnel_ids = df["personnel_id"].tolist()
        
        print(f"Successfully generated {len(df)} personnel records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_scheduled_production(self, num_records=None):
        """
        Generate synthetic data for the ScheduledProduction table from ISA-95 Level 4.
        
        Parameters:
        - num_records: Number of scheduled production records to generate (auto-calculated if None)
        
        Returns:
        - DataFrame containing the generated scheduled production data
        """
        if self.production_schedules_df is None or len(self.production_schedules_df) == 0:
            print("Error: No production schedules data available.")
            return None
            
        if self.products_df is None or len(self.products_df) == 0:
            print("Error: No products data available.")
            return None
        
        # Load work order IDs if available
        if self.work_order_ids:
            work_order_ids = self.work_order_ids
        else:
            print("Generating synthetic work order IDs...")
            work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(100)]
        
        # Load equipment IDs if available
        if self.equipment_ids:
            equipment_ids = self.equipment_ids
        else:
            print("Generating synthetic equipment IDs...")
            equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Load customer order IDs if available
        customer_order_ids = []
        if hasattr(self, 'customer_orders_df') and self.customer_orders_df is not None and len(self.customer_orders_df) > 0:
            customer_order_ids = self.customer_orders_df['order_id'].tolist()
        
        if not customer_order_ids:
            print("Generating synthetic customer order IDs...")
            customer_order_ids = [f"CO-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Generate data structure
        data = {
            "scheduled_id": [],
            "schedule_id": [],
            "product_id": [],
            "work_order_id": [],
            "scheduled_quantity": [],
            "start_date": [],
            "end_date": [],
            "equipment_id": [],
            "priority": [],
            "status": [],
            "order_id": []
        }
        
        # Generate scheduled production items for each production schedule
        for _, schedule in self.production_schedules_df.iterrows():
            schedule_id = schedule['schedule_id']
            schedule_start = pd.to_datetime(schedule['start_date'])
            schedule_end = pd.to_datetime(schedule['end_date'])
            schedule_status = schedule['status']
            
            # Determine number of production items for this schedule
            # More detailed schedules have more items
            if schedule['schedule_type'] in ["Detailed", "Weekly"]:
                num_items = random.randint(10, 30)
            elif schedule['schedule_type'] == "Campaign":
                num_items = random.randint(5, 15)
            else:
                num_items = random.randint(5, 20)
            
            # Generate production items
            for _ in range(num_items):
                # Create unique scheduled production ID
                scheduled_id = f"SP-{uuid.uuid4().hex[:8].upper()}"
                data["scheduled_id"].append(scheduled_id)
                data["schedule_id"].append(schedule_id)
                
                # Select product
                product = self.products_df.sample(1).iloc[0]
                data["product_id"].append(product['product_id'])
                
                # Assign work order (some items might not have work orders yet)
                if schedule_status in ["In Progress", "Completed"] and random.random() < 0.9:
                    # Most in-progress or completed schedule items have work orders
                    data["work_order_id"].append(random.choice(work_order_ids))
                elif schedule_status == "Approved" and random.random() < 0.5:
                    # Some approved schedule items have work orders
                    data["work_order_id"].append(random.choice(work_order_ids))
                else:
                    data["work_order_id"].append("")
                
                # Generate scheduled quantity
                scheduled_quantity = random.randint(100, 10000)
                data["scheduled_quantity"].append(scheduled_quantity)
                
                # Generate start and end dates
                # Distribute items across the schedule period
                schedule_days = (schedule_end - schedule_start).days
                if schedule_days <= 0:
                    schedule_days = 1  # Ensure at least 1 day
                    
                item_start_offset = random.randint(0, max(0, schedule_days - 1))
                item_start = schedule_start + timedelta(days=item_start_offset)
                
                # Item duration depends on quantity and complexity
                if 'product_family' in product and product['product_family'] in ["Pharmaceutical", "Chemical"]:
                    # Complex products take longer
                    item_duration = random.randint(3, 14)  # 3-14 days
                else:
                    item_duration = random.randint(1, 7)  # 1-7 days
                    
                item_end = item_start + timedelta(days=item_duration)
                
                # Ensure end date is within schedule
                if item_end > schedule_end:
                    item_end = schedule_end
                    
                data["start_date"].append(item_start.strftime("%Y-%m-%d"))
                data["end_date"].append(item_end.strftime("%Y-%m-%d"))
                
                # Assign equipment
                data["equipment_id"].append(random.choice(equipment_ids))
                
                # Set priority
                data["priority"].append(random.randint(1, 5))  # 1=highest, 5=lowest
                
                # Set status based on schedule status and dates
                current_date = datetime.now()
                
                if schedule_status == "Cancelled":
                    item_status = "Cancelled"
                elif schedule_status == "Draft":
                    item_status = "Planned"
                elif schedule_status == "Approved":
                    if item_start > current_date:
                        item_status = "Scheduled"
                    else:
                        item_status = random.choice(["Scheduled", "Released"])
                elif schedule_status == "In Progress":
                    if item_start > current_date:
                        item_status = "Scheduled"
                    elif item_end < current_date:
                        item_status = random.choice(["Completed", "Completed", "Canceled"])
                    else:
                        item_status = random.choice(["Released", "In Progress", "Held"])
                elif schedule_status in ["Completed", "Superseded"]:
                    if random.random() < 0.9:  # 90% chance
                        item_status = "Completed"
                    else:
                        item_status = random.choice(["Canceled", "Partially Completed"])
                else:
                    item_status = "Planned"
                    
                data["status"].append(item_status)
                
                # Link to customer order (if applicable)
                if random.random() < 0.7:  # 70% linked to order
                    data["order_id"].append(random.choice(customer_order_ids))
                else:
                    data["order_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "scheduled_production.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.scheduled_production_df = df
        
        print(f"Successfully generated {len(df)} scheduled production records for {len(self.production_schedules_df)} schedules.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_facilities(self, num_facilities=15):
        """
        Generate synthetic data for the Facilities table from ISA-95 Level 4.
        
        Parameters:
        - num_facilities: Number of facility records to generate
        
        Returns:
        - DataFrame containing the generated facilities data
        """
        # Define facility types and their probabilities
        facility_types = {
            "Manufacturing Plant": 0.5,
            "Warehouse": 0.2,
            "Distribution Center": 0.15,
            "R&D Center": 0.1,
            "Administrative Office": 0.05
        }
        
        # Define regions for address generation
        regions = {
            "North America": ["USA", "Canada", "Mexico"],
            "Europe": ["UK", "Germany", "France", "Italy", "Spain", "Netherlands"],
            "Asia": ["Japan", "China", "South Korea", "India", "Singapore", "Taiwan"],
            "Latin America": ["Brazil", "Colombia", "Argentina", "Chile", "Peru"],
            "Oceania": ["Australia", "New Zealand"]
        }
        
        # Region probabilities
        region_weights = {
            "North America": 0.4,
            "Europe": 0.25,
            "Asia": 0.2,
            "Latin America": 0.1,
            "Oceania": 0.05
        }
        
        # Generate manager IDs
        if not self.personnel_ids or len(self.personnel_ids) == 0:
            raise ValueError("Personnel data must be generated before facilities. No personnel IDs available.")
            
        # Select managers from existing personnel
        # Prefer personnel with manager/director titles
        potential_managers = []
        if hasattr(self, 'personnel_df') and self.personnel_df is not None:
            # Get personnel with management roles
            management_roles = self.personnel_df[
                self.personnel_df['job_title'].str.contains('Manager|Director|Supervisor|Lead', case=False, na=False)
            ]
            if len(management_roles) >= num_facilities:
                potential_managers = management_roles['personnel_id'].tolist()
            else:
                # If not enough managers, use all available personnel
                potential_managers = self.personnel_df['personnel_id'].tolist()
        else:
            potential_managers = self.personnel_ids

        # Ensure we have enough managers
        if len(potential_managers) < num_facilities:
            raise ValueError(f"Not enough personnel ({len(potential_managers)}) to assign as facility managers ({num_facilities})")
            
        # Select managers for facilities (can reuse same manager for multiple facilities)
        manager_ids = []
        for i in range(num_facilities):
            if i < len(potential_managers):
                manager_ids.append(potential_managers[i])
            else:
                # Reuse managers if we have more facilities than managers
                manager_ids.append(potential_managers[i % len(potential_managers)])
        
        # Generate data structure
        data = {
            "facility_id": [f"FAC-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_facilities)],
            "facility_name": [],
            "facility_type": [],
            "address": [],
            "manager_id": [],
            "operating_hours": [],
            "status": [],
            "parent_facility_id": []
        }
        
        # Last names for city generation
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", 
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", 
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"
        ]
        
        # Generate a list of facility IDs for potential parent relationships
        all_facility_ids = data["facility_id"].copy()
        
        # Select some facilities to be headquarters or main facilities (no parent)
        num_main_facilities = min(3, num_facilities)  # Up to 3 main facilities or less if fewer total facilities
        main_facility_indices = random.sample(range(num_facilities), num_main_facilities)
        potential_parent_ids = [all_facility_ids[i] for i in main_facility_indices]
        
        # Generate data for each facility
        for i in range(num_facilities):
            # Select facility type (weighted random)
            facility_type = random.choices(
                list(facility_types.keys()), 
                weights=list(facility_types.values())
            )[0]
            data["facility_type"].append(facility_type)
            
            # Generate facility name
            # For manufacturing plants, use format like "City Manufacturing Plant"
            # For warehouses, use format like "Regional Distribution Center"
            # For offices, use format like "Corporate Headquarters"
            
            # Generate a region and country
            region = random.choices(
                list(region_weights.keys()), 
                weights=list(region_weights.values())
            )[0]
            country = random.choice(regions[region])
            
            # Generate a city name (simplified)
            city_prefixes = ["New", "East", "West", "North", "South", "Central", "Upper", "Lower", "Port", "Lake", "Mount"]
            city_suffixes = ["ville", "burg", "town", "field", "ford", "port", "bridge", "haven", "city"]
            
            if random.random() < 0.3:  # 30% chance of using prefix
                city = f"{random.choice(city_prefixes)} {random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            else:
                city = f"{random.choice(last_names)}{random.choice(['', random.choice(city_suffixes)])}"
            
            # Create facility name based on type
            if facility_type == "Manufacturing Plant":
                facility_name = f"{city} Manufacturing Plant"
                if random.random() < 0.3:  # 30% chance of adding specialty
                    specialties = ["Pharmaceutical", "Food", "Electronics", "Automotive", "Chemical", "Consumer Goods"]
                    facility_name = f"{city} {random.choice(specialties)} Manufacturing Plant"
            elif facility_type == "Warehouse":
                facility_name = f"{region} Distribution Warehouse"
                if random.random() < 0.5:  # 50% chance of adding location
                    facility_name = f"{city} Distribution Warehouse"
            elif facility_type == "Distribution Center":
                facility_name = f"{country} Distribution Center"
                if random.random() < 0.5:  # 50% chance of adding location
                    facility_name = f"{city} Distribution Center"
            elif facility_type == "R&D Center":
                facility_name = f"R&D Center {city}"
                if random.random() < 0.3:  # 30% chance of adding specialty
                    specialties = ["Pharmaceutical", "Formulation", "Process Development", "Analytical", "Innovation"]
                    facility_name = f"{random.choice(specialties)} R&D Center"
            else:  # Administrative Office
                if i in main_facility_indices:  # If this is a main facility
                    facility_name = f"Corporate Headquarters - {city}"
                else:
                    facility_name = f"{region} Administrative Office"
                    if random.random() < 0.5:  # 50% chance of adding location
                        facility_name = f"{city} Administrative Office"
            
            data["facility_name"].append(facility_name)
            
            # Generate street address
            street_number = random.randint(1, 9999)
            street_types = ["Street", "Avenue", "Boulevard", "Road", "Lane", "Drive", "Way", "Place", "Court"]
            street_name = f"{random.choice(last_names)} {random.choice(street_types)}"
            
            address = f"{street_number} {street_name}, {city}, {country}"
            data["address"].append(address)
            
            # Assign manager
            data["manager_id"].append(manager_ids[i % len(manager_ids)])  # Cycle through available managers
            
            # Set operating hours
            operating_hour_options = [
                "24/7", 
                "Mon-Fri: 8AM-5PM", 
                "Mon-Fri: 7AM-7PM, Sat: 8AM-12PM",
                "Mon-Sat: 6AM-10PM",
                "Mon-Fri: 6AM-6PM, Weekends: On-call"
            ]
            
            # Manufacturing plants and warehouses are more likely to have extended hours
            if facility_type in ["Manufacturing Plant", "Warehouse", "Distribution Center"]:
                hours_weights = [0.4, 0.2, 0.2, 0.15, 0.05]  # More 24/7 operations
            else:
                hours_weights = [0.05, 0.6, 0.2, 0.1, 0.05]  # More standard business hours
                
            data["operating_hours"].append(random.choices(operating_hour_options, weights=hours_weights)[0])
            
            # Set status
            statuses = ["Active", "Inactive", "Under Construction", "Under Renovation", "Planned"]
            status_weights = [0.8, 0.05, 0.05, 0.05, 0.05]  # Mostly active facilities
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Set parent facility ID (if applicable)
            if i in main_facility_indices:
                # Main facilities have no parent
                data["parent_facility_id"].append("")
            else:
                # Other facilities have a parent with 80% probability
                if random.random() < 0.8:
                    data["parent_facility_id"].append(random.choice(potential_parent_ids))
                else:
                    data["parent_facility_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Ensure the directory exists
        output_file = os.path.join(self.output_dir, "facilities.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.facilities_df = df
        self.facility_ids = df["facility_id"].tolist()
        
        print(f"Successfully generated {len(df)} facility records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_storage_locations(self, num_locations=None):
        """
        Generate synthetic data for the StorageLocations table from ISA-95 Level 4.
        
        Parameters:
        - num_locations: Number of storage location records to generate (default: auto-calculate based on facilities)
        
        Returns:
        - DataFrame containing the generated storage locations data
        """
        if self.facilities_df is None or len(self.facilities_df) == 0:
            print("Error: No facilities data available.")
            return None
        
        # Auto-calculate number of locations if not specified
        if num_locations is None:
            # Generate more storage locations for manufacturing plants and warehouses
            # and fewer for offices and R&D centers
            num_locations = 0
            for _, facility in self.facilities_df.iterrows():
                if 'facility_type' in facility:
                    if facility['facility_type'] == 'Manufacturing Plant':
                        num_locations += random.randint(10, 30)
                    elif facility['facility_type'] in ['Warehouse', 'Distribution Center']:
                        num_locations += random.randint(30, 50)
                    elif facility['facility_type'] == 'R&D Center':
                        num_locations += random.randint(5, 15)
                    else:  # Administrative Office
                        num_locations += random.randint(1, 5)
                else:
                    num_locations += random.randint(10, 20)  # Default if type not available
        
        # Define storage location types
        location_types = {
            "Warehouse": ["Bulk Storage", "Pallet Rack", "Shelf", "Bin", "Cold Storage", "Freezer", "Controlled Substance", "Hazardous Material"],
            "Production": ["Raw Material Staging", "Work In Progress", "Finished Goods", "Line-Side", "Temporary Holding", "Quality Control Hold"],
            "Shipping": ["Shipping Dock", "Receiving Dock", "Staging Area", "Cross-Dock", "Outbound Queue", "Returns Processing"],
            "Special": ["Sample Storage", "Archive", "Quarantine", "QA Lab", "Damaged Goods", "Rejected Material", "Maintenance Supplies"]
        }
        
        # Define storage conditions
        storage_conditions = {
            "Standard": ["Ambient", "Room Temperature (15-25°C)", "Dry", "Standard Warehouse Conditions"],
            "Temperature Controlled": ["Refrigerated (2-8°C)", "Cold Room (8-15°C)", "Freezer (-20°C)", "Deep Freeze (-80°C)", "Heated (25-40°C)"],
            "Environmental Control": ["Humidity Controlled (<40% RH)", "Humidity Controlled (40-60% RH)", "Clean Room ISO Class 8", "Clean Room ISO Class 7", "Clean Room ISO Class 6"],
            "Special Conditions": ["Explosion Proof", "Fire Resistant", "ESD Protected", "Light Protected", "Nitrogen Atmosphere", "Oxygen-Free"]
        }
        
        # Generate data structure
        data = {
            "location_id": [f"LOC-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_locations)],
            "location_name": [],
            "facility_id": [],
            "location_type": [],
            "storage_conditions": [],
            "maximum_capacity": [],
            "current_utilization": [],
            "status": [],
            "parent_location_id": []
        }
        
        # Map to keep track of locations by facility
        locations_by_facility = {facility_id: [] for facility_id in self.facilities_df['facility_id']}
        
        # Generate root-level storage locations first (ones with no parent)
        facility_ids = self.facilities_df['facility_id'].tolist()
        root_locations_count = int(num_locations * 0.2)  # 20% of locations are root level
        child_locations_count = num_locations - root_locations_count
        
        # Keep track of all location IDs
        all_location_ids = data["location_id"].copy()
        root_location_ids = all_location_ids[:root_locations_count]
        
        # Generate root locations first
        for i in range(root_locations_count):
            location_id = all_location_ids[i]
            
            # Select a facility (weighted toward warehouses and manufacturing plants)
            if 'facility_type' in self.facilities_df.columns:
                facility_weights = self.facilities_df['facility_type'].apply(
                    lambda x: 3 if x in ['Warehouse', 'Distribution Center'] else 
                            (2 if x == 'Manufacturing Plant' else 1)
                )
                facility = self.facilities_df.sample(weights=facility_weights).iloc[0]
            else:
                facility = self.facilities_df.sample(1).iloc[0]
                
            facility_id = facility['facility_id']
            locations_by_facility[facility_id].append(location_id)
            data["facility_id"].append(facility_id)
            
            # Determine location type based on facility type
            if 'facility_type' in facility:
                if facility['facility_type'] in ['Warehouse', 'Distribution Center']:
                    location_category = random.choices(
                        ["Warehouse", "Shipping", "Special"],
                        weights=[0.7, 0.2, 0.1]
                    )[0]
                elif facility['facility_type'] == 'Manufacturing Plant':
                    location_category = random.choices(
                        ["Warehouse", "Production", "Shipping", "Special"],
                        weights=[0.3, 0.4, 0.2, 0.1]
                    )[0]
                elif facility['facility_type'] == 'R&D Center':
                    location_category = random.choices(
                        ["Warehouse", "Special"],
                        weights=[0.3, 0.7]
                    )[0]
                else:  # Administrative Office
                    location_category = "Special"
            else:
                location_category = random.choice(list(location_types.keys()))
                
            location_type = random.choice(location_types[location_category])
            data["location_type"].append(location_type)
            
            # Generate location name
            building = random.choice(["Building", "Block", "Wing", "Area", "Zone"])
            building_num = random.choice(["A", "B", "C", "D", "1", "2", "3", "4"])
            
            # Format location name based on type
            if location_category == "Warehouse":
                location_name = f"{building} {building_num} - {location_type}"
            elif location_category == "Production":
                location_name = f"Production {building} {building_num} - {location_type}"
            elif location_category == "Shipping":
                location_name = f"{location_type} Area {building_num}"
            else:  # Special
                location_name = f"{location_type} {building} {building_num}"
                
            data["location_name"].append(location_name)
            
            # Set storage conditions based on location type
            if location_type in ["Cold Storage", "Freezer", "Refrigerated"]:
                condition_category = "Temperature Controlled"
            elif location_type in ["Clean Room", "QA Lab", "Controlled Substance"]:
                condition_category = "Environmental Control"
            elif location_type in ["Hazardous Material", "Quarantine"]:
                condition_category = "Special Conditions"
            else:
                condition_category = random.choices(
                    list(storage_conditions.keys()),
                    weights=[0.7, 0.1, 0.1, 0.1]
                )[0]
                
            data["storage_conditions"].append(random.choice(storage_conditions[condition_category]))
            
            # Set capacity based on location type
            if location_type in ["Bulk Storage", "Pallet Rack", "Warehouse"]:
                max_capacity = random.randint(1000, 10000)
            elif location_type in ["Shelf", "Bin", "Line-Side"]:
                max_capacity = random.randint(100, 1000)
            elif location_type in ["Cold Storage", "Freezer", "Controlled Substance"]:
                max_capacity = random.randint(500, 5000)
            else:
                max_capacity = random.randint(200, 3000)
                
            data["maximum_capacity"].append(max_capacity)
            
            # Set current utilization (as a percentage)
            utilization = round(random.uniform(0.3, 0.9) * 100, 1)  # 30-90% utilized
            data["current_utilization"].append(utilization)
            
            # Set status
            statuses = ["Active", "Inactive", "Maintenance", "Full", "Reserved"]
            status_weights = [0.8, 0.05, 0.05, 0.05, 0.05]  # Mostly active
            data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Root locations have no parent
            data["parent_location_id"].append("")
        
        # Generate child locations
        for i in range(child_locations_count):
            location_id = all_location_ids[root_locations_count + i]
            
            # Select a facility and a potential parent location in that facility
            facility_id = random.choice(facility_ids)
            
            # If the facility has root locations, use one as parent
            if locations_by_facility[facility_id]:
                parent_location_id = random.choice(locations_by_facility[facility_id])
                parent_index = all_location_ids.index(parent_location_id)
                parent_location_type = data["location_type"][parent_index]
                
                # Store the facility and parent relationship
                data["facility_id"].append(facility_id)
                data["parent_location_id"].append(parent_location_id)
                
                # Child location types are derived from parent types
                if parent_location_type in ["Bulk Storage", "Pallet Rack", "Warehouse"]:
                    child_types = ["Aisle", "Bay", "Rack", "Section", "Zone"]
                    location_type = f"{random.choice(child_types)} {random.randint(1, 99):02d}"
                elif parent_location_type in ["Shelf", "Bin"]:
                    location_type = f"{parent_location_type} {random.randint(1, 999):03d}"
                elif parent_location_type in ["Cold Storage", "Freezer"]:
                    location_type = f"{parent_location_type} Section {random.randint(1, 20):02d}"
                elif parent_location_type in ["Raw Material Staging", "Finished Goods"]:
                    location_type = f"{parent_location_type} Area {random.choice(['A', 'B', 'C', 'D'])}{random.randint(1, 20):02d}"
                else:
                    # Default child naming for other parent types
                    location_type = f"Sub-Location {random.randint(1, 99):02d}"
            else:
                # If no root locations in this facility, create as a root location
                data["facility_id"].append(facility_id)
                data["parent_location_id"].append("")
                
                # Pick a random location type
                location_category = random.choice(list(location_types.keys()))
                location_type = random.choice(location_types[location_category])
            
            data["location_type"].append(location_type)
            
            # Generate location name
            if data["parent_location_id"][-1]:  # If has parent
                parent_index = all_location_ids.index(data["parent_location_id"][-1])
                parent_name = data["location_name"][parent_index]
                location_name = f"{parent_name} - {location_type}"
            else:
                building = random.choice(["Building", "Block", "Wing", "Area", "Zone"])
                building_num = random.choice(["A", "B", "C", "D", "1", "2", "3", "4"])
                location_name = f"{building} {building_num} - {location_type}"
                
            data["location_name"].append(location_name)
            
            # Set storage conditions (inherit from parent or generate new)
            if data["parent_location_id"][-1]:
                # 80% chance to inherit parent's conditions
                if random.random() < 0.8:
                    parent_index = all_location_ids.index(data["parent_location_id"][-1])
                    data["storage_conditions"].append(data["storage_conditions"][parent_index])
                else:
                    condition_category = random.choice(list(storage_conditions.keys()))
                    data["storage_conditions"].append(random.choice(storage_conditions[condition_category]))
            else:
                condition_category = random.choice(list(storage_conditions.keys()))
                data["storage_conditions"].append(random.choice(storage_conditions[condition_category]))
            
            # Set capacity (smaller for child locations)
            if data["parent_location_id"][-1]:
                parent_index = all_location_ids.index(data["parent_location_id"][-1])
                parent_capacity = data["maximum_capacity"][parent_index]
                # Child capacity is a fraction of parent capacity
                max_capacity = int(parent_capacity * random.uniform(0.05, 0.2))
            else:
                max_capacity = random.randint(100, 5000)
                
            data["maximum_capacity"].append(max_capacity)
            
            # Set current utilization (as a percentage)
            utilization = round(random.uniform(0.2, 0.95) * 100, 1)  # 20-95% utilized
            data["current_utilization"].append(utilization)
            
            # Set status (inherit from parent with some variation)
            if data["parent_location_id"][-1]:
                parent_index = all_location_ids.index(data["parent_location_id"][-1])
                parent_status = data["status"][parent_index]
                
                if parent_status == "Inactive":
                    # Inactive parents have inactive children
                    data["status"].append("Inactive")
                elif parent_status == "Maintenance":
                    # Maintenance parents likely have maintenance children
                    statuses = ["Maintenance", "Inactive", "Active"]
                    data["status"].append(random.choices(statuses, weights=[0.7, 0.2, 0.1])[0])
                else:
                    # Active parents have mostly active children
                    statuses = ["Active", "Inactive", "Maintenance", "Full", "Reserved"]
                    status_weights = [0.75, 0.05, 0.05, 0.1, 0.05]
                    data["status"].append(random.choices(statuses, weights=status_weights)[0])
            else:
                statuses = ["Active", "Inactive", "Maintenance", "Full", "Reserved"]
                status_weights = [0.8, 0.05, 0.05, 0.05, 0.05]  # Mostly active
                data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Add this location to the facility's location list
            locations_by_facility[facility_id].append(location_id)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "storage_locations.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.storage_locations_df = df
        self.location_ids = df["location_id"].tolist()
        
        print(f"Successfully generated {len(df)} storage location records across {len(self.facilities_df)} facilities.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_shifts(self, num_shifts=None):
        """
        Generate synthetic data for the Shifts table from ISA-95 Level 4.
        
        Parameters:
        - facilities_df: DataFrame containing facilities data
        - personnel_df: DataFrame containing personnel data (optional)
        - output_file: CSV file to save the data
        
        Returns:
        - DataFrame containing the generated shifts data
        """
        if self.facilities_df is None or len(self.facilities_df) == 0:
            print("Error: No facilities data available.")
            return None
        
        # Get actual facility IDs from the facilities_df
        facility_ids = self.facilities_df['facility_id'].tolist()


        # Generate supervisor IDs if personnel_df is not provided
        if not self.personnel_ids:
            print("Generating synthetic supervisor IDs...")
            supervisor_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        else:
            # Use existing personnel IDs
            supervisor_ids = self.personnel_ids[:30]
            if len(supervisor_ids) < 30:
                # If not enough, generate more
                additional_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(30 - len(supervisor_ids))]
                supervisor_ids.extend(additional_ids)
        
        # Define common shift patterns
        shift_patterns = {
            "Standard": [
                {"name": "Day Shift", "start": "08:00", "end": "16:00", "breaks": [{"start": "12:00", "end": "12:30", "type": "Lunch"}]},
                {"name": "Evening Shift", "start": "16:00", "end": "00:00", "breaks": [{"start": "20:00", "end": "20:30", "type": "Dinner"}]},
                {"name": "Night Shift", "start": "00:00", "end": "08:00", "breaks": [{"start": "04:00", "end": "04:30", "type": "Meal"}]}
            ],
            "Manufacturing": [
                {"name": "First Shift", "start": "06:00", "end": "14:00", "breaks": [{"start": "10:00", "end": "10:15", "type": "Break"}, {"start": "12:00", "end": "12:30", "type": "Lunch"}]},
                {"name": "Second Shift", "start": "14:00", "end": "22:00", "breaks": [{"start": "18:00", "end": "18:15", "type": "Break"}, {"start": "19:00", "end": "19:30", "type": "Dinner"}]},
                {"name": "Third Shift", "start": "22:00", "end": "06:00", "breaks": [{"start": "02:00", "end": "02:15", "type": "Break"}, {"start": "03:00", "end": "03:30", "type": "Meal"}]}
            ],
            "Distribution": [
                {"name": "Morning Shift", "start": "07:00", "end": "15:30", "breaks": [{"start": "10:00", "end": "10:15", "type": "Break"}, {"start": "12:00", "end": "12:30", "type": "Lunch"}]},
                {"name": "Afternoon Shift", "start": "15:00", "end": "23:30", "breaks": [{"start": "18:00", "end": "18:15", "type": "Break"}, {"start": "20:00", "end": "20:30", "type": "Dinner"}]},
                {"name": "Overnight Shift", "start": "23:00", "end": "07:30", "breaks": [{"start": "02:00", "end": "02:15", "type": "Break"}, {"start": "04:00", "end": "04:30", "type": "Meal"}]}
            ],
            "Office": [
                {"name": "Business Hours", "start": "09:00", "end": "17:00", "breaks": [{"start": "12:00", "end": "13:00", "type": "Lunch"}]},
                {"name": "Extended Hours", "start": "08:00", "end": "18:00", "breaks": [{"start": "12:30", "end": "13:30", "type": "Lunch"}]}
            ],
            "Continuous": [
                {"name": "A Shift", "start": "06:00", "end": "18:00", "breaks": [{"start": "10:00", "end": "10:15", "type": "Break"}, {"start": "14:00", "end": "14:30", "type": "Meal"}]},
                {"name": "B Shift", "start": "18:00", "end": "06:00", "breaks": [{"start": "22:00", "end": "22:15", "type": "Break"}, {"start": "02:00", "end": "02:30", "type": "Meal"}]}
            ],
            "Weekend": [
                {"name": "Weekend Day", "start": "07:00", "end": "19:00", "breaks": [{"start": "12:00", "end": "12:45", "type": "Lunch"}]},
                {"name": "Weekend Night", "start": "19:00", "end": "07:00", "breaks": [{"start": "00:00", "end": "00:45", "type": "Meal"}]}
            ]
        }
        
        # Generate data structure
        data = {
            "shift_id": [],
            "shift_name": [],
            "facility_id": [],
            "start_time": [],
            "end_time": [],
            "break_periods": [],
            "supervisor_id": [],
            "notes": []
        }
        
        # Determine which shift patterns to use for each facility
        facility_shift_patterns = {}
        
        for _, facility in self.facilities_df.iterrows():
            facility_id = facility['facility_id']
            
            # Determine appropriate shift pattern based on facility type
            if 'facility_type' in facility:
                if facility['facility_type'] == 'Manufacturing Plant':
                    if 'operating_hours' in facility and '24/7' in str(facility['operating_hours']):
                        pattern_type = random.choice(["Manufacturing", "Continuous"])
                    else:
                        pattern_type = "Manufacturing"
                elif facility['facility_type'] in ['Warehouse', 'Distribution Center']:
                    if 'operating_hours' in facility and '24/7' in str(facility['operating_hours']):
                        pattern_type = random.choice(["Distribution", "Continuous"])
                    else:
                        pattern_type = "Distribution"
                elif facility['facility_type'] == 'R&D Center':
                    pattern_type = random.choice(["Standard", "Office"])
                else:  # Administrative Office
                    pattern_type = "Office"
            else:
                # Default if facility type not available
                pattern_type = random.choice(list(shift_patterns.keys()))
            
            # Store the selected pattern type
            facility_shift_patterns[facility_id] = pattern_type
            
            # Determine number of shifts for this facility
            if pattern_type == "Office":
                num_shifts = random.randint(1, 2)  # Offices usually have 1-2 shifts
            elif pattern_type == "Continuous":
                num_shifts = 2  # Continuous operations usually have 2 shifts
            else:
                num_shifts = len(shift_patterns[pattern_type])
                
            # Add weekend shifts?
            add_weekend = random.random() < 0.4  # 40% chance of weekend shifts
            
            # Generate shifts for this facility
            for i in range(num_shifts):
                # Create unique shift ID
                shift_id = f"SHIFT-{uuid.uuid4().hex[:8].upper()}"
                data["shift_id"].append(shift_id)
                
                # Set facility ID
                data["facility_id"].append(facility_id)
                
                # Get shift template
                shift_template = shift_patterns[pattern_type][i % len(shift_patterns[pattern_type])]
                
                # Set shift name (append facility name or code for uniqueness)
                if 'facility_name' in facility:
                    facility_code = facility['facility_name'].split()[0]  # Use first word of facility name
                else:
                    facility_code = facility_id[-4:]  # Use last 4 chars of ID
                    
                shift_name = f"{shift_template['name']} ({facility_code})"
                data["shift_name"].append(shift_name)
                
                # Set start and end times
                data["start_time"].append(shift_template['start'])
                data["end_time"].append(shift_template['end'])
                
                # Set break periods as JSON string
                data["break_periods"].append(str(shift_template['breaks']))
                
                # Assign supervisor
                data["supervisor_id"].append(random.choice(supervisor_ids))
                
                # Set notes
                notes_options = [
                    "",  # Empty notes most common
                    "Cross-trained personnel required",
                    "Heavy machinery operation certification needed",
                    "Quality inspection responsibilities",
                    "Maintenance activities scheduled during this shift",
                    "Handover procedures are critical",
                    "Specialized training required",
                    "High volume production period",
                    "Security escort needed for sensitive areas"
                ]
                notes_weights = [0.7, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05]  # 70% empty notes
                data["notes"].append(random.choices(notes_options, weights=notes_weights)[0])
            
            # Add weekend shifts if applicable
            if add_weekend:
                for i in range(min(2, len(shift_patterns["Weekend"]))):
                    # Create unique shift ID
                    shift_id = f"SHIFT-{uuid.uuid4().hex[:8].upper()}"
                    data["shift_id"].append(shift_id)
                    
                    # Set facility ID
                    if facility_ids:
                        data["facility_id"].append(random.choice(facility_ids))
                    else:
                        print("Warning: No facility IDs available. Using placeholder.")
                        data["facility_id"].append("FAC-00000000") 
                    
                    # Get shift template
                    shift_template = shift_patterns["Weekend"][i]
                    
                    # Set shift name
                    if 'facility_name' in facility:
                        facility_code = facility['facility_name'].split()[0]
                    else:
                        facility_code = facility_id[-4:]
                        
                    shift_name = f"{shift_template['name']} ({facility_code})"
                    data["shift_name"].append(shift_name)
                    
                    # Set start and end times
                    data["start_time"].append(shift_template['start'])
                    data["end_time"].append(shift_template['end'])
                    
                    # Set break periods as JSON string
                    data["break_periods"].append(str(shift_template['breaks']))
                    
                    # Assign supervisor
                    data["supervisor_id"].append(random.choice(supervisor_ids))
                    
                    # Set weekend-specific notes
                    weekend_notes = [
                        "Weekend coverage",
                        "Reduced staff, cross-training required",
                        "Limited support services available",
                        "On-call maintenance only",
                        "No shipping/receiving on weekends",
                        "Security measures heightened on weekends"
                    ]
                    data["notes"].append(random.choice(weekend_notes))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "shifts.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.shifts_df = df
        self.shift_ids = df["shift_id"].tolist()
        
        print(f"Successfully generated {len(df)} shift records across {len(self.facilities_df)} facilities.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_inventory_transactions(self, num_transactions=1000, start_time=None, end_time=None):
        """
        Generate synthetic data for the InventoryTransactions table from ISA-95 Level 4.
        
        Parameters:
        - num_transactions: Number of transaction records to generate
        - start_time: Start time for transaction dates (defaults to 365 days ago)
        - end_time: End time for transaction dates (defaults to current date)
        
        Returns:
        - DataFrame containing the generated inventory transactions data
        """
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now()
        
        # Generate material IDs if not available
        if not self.material_ids:
            print("Generating synthetic material IDs...")
            self.material_ids = [f"MAT-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Generate location IDs if not available
        location_ids = []
        if hasattr(self, 'storage_locations_df') and self.storage_locations_df is not None:
            location_ids = self.storage_locations_df['location_id'].tolist()
        
        if not location_ids:
            print("Generating synthetic storage location IDs...")
            location_ids = [f"LOC-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Generate lot IDs if not available
        lot_ids = []
        if hasattr(self, 'material_lots_df') and self.material_lots_df is not None:
            lot_ids = self.material_lots_df['lot_id'].tolist()
            
            # Create a mapping of material_id to lot_ids if possible
            material_to_lots = {}
            if 'material_id' in self.material_lots_df.columns:
                for material_id in self.material_ids:
                    material_lots = self.material_lots_df[self.material_lots_df['material_id'] == material_id]
                    if len(material_lots) > 0:
                        material_to_lots[material_id] = material_lots['lot_id'].tolist()
                    else:
                        # Assign random lots if no specific lots found for this material
                        num_lots = random.randint(1, 3)
                        material_to_lots[material_id] = random.sample(lot_ids, min(num_lots, len(lot_ids)))
        
        if not lot_ids:
            print("Generating synthetic lot IDs...")
            lot_ids = [f"LOT-{uuid.uuid4().hex[:8].upper()}" for _ in range(100)]
            
            # Create a mapping of material_id to lot_ids
            material_to_lots = {}
            for material_id in self.material_ids:
                # Assign 2-5 lots to each material
                num_lots = random.randint(2, 5)
                material_to_lots[material_id] = random.sample(lot_ids, min(num_lots, len(lot_ids)))
        
        # Generate work order IDs if not available
        if not self.work_order_ids:
            print("Generating synthetic work order IDs...")
            self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Generate purchase order IDs if not available
        purchase_order_ids = []
        if hasattr(self, 'purchase_orders_df') and self.purchase_orders_df is not None:
            purchase_order_ids = self.purchase_orders_df['po_id'].tolist()
        
        if not purchase_order_ids:
            print("Generating synthetic purchase order IDs...")
            purchase_order_ids = [f"PO-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Generate operator IDs if not available
        if not self.personnel_ids:
            print("Generating synthetic operator IDs...")
            self.personnel_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        operator_ids = self.personnel_ids
        
        # Define transaction types and their probabilities
        transaction_types = {
            "Receipt": 0.2,       # Receiving materials from suppliers
            "Issue": 0.3,         # Issuing materials to production
            "Transfer": 0.25,     # Moving materials between locations
            "Adjustment": 0.05,   # Inventory count adjustments
            "Return": 0.05,       # Returns to inventory
            "Scrap": 0.05,        # Scrapping materials
            "Quality Hold": 0.05, # Placing materials on quality hold
            "Release": 0.05       # Releasing materials from hold
        }
        
        # Define reason codes by transaction type
        reason_codes = {
            "Receipt": ["Purchase Order Receipt", "Production Return", "Transfer In", "Inventory Correction"],
            "Issue": ["Production Issue", "Transfer Out", "Sample Issue", "QC Testing", "R&D Use"],
            "Transfer": ["Inventory Optimization", "Storage Consolidation", "Move to Production", "Staging for Shipping"],
            "Adjustment": ["Cycle Count", "Physical Inventory", "Damaged in Storage", "Expiration", "System Reconciliation"],
            "Return": ["Production Excess", "QC Rejection", "Customer Return", "Unused Material"],
            "Scrap": ["Expired", "Damaged", "Failed QC", "Contaminated", "Obsolete"],
            "Quality Hold": ["Out of Specification", "Pending Test Results", "Supplier Investigation", "Process Deviation"],
            "Release": ["QC Approval", "Investigation Complete", "Deviation Approved", "Rework Complete"]
        }
        
        # Generate data structure
        data = {
            "transaction_id": [f"TRX-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_transactions)],
            "transaction_type": [],
            "material_id": [],
            "lot_id": [],
            "timestamp": [],
            "quantity": [],
            "from_location_id": [],
            "to_location_id": [],
            "work_order_id": [],
            "reference_document": [],
            "operator_id": [],
            "transaction_reason": [],
            "unit_cost": []
        }
        
        # Keep track of material-location inventory for realistic transactions
        inventory = {}  # (material_id, location_id, lot_id) -> quantity
        
        # Generate data for each transaction
        for i in range(num_transactions):
            # Generate timestamp within the specified range
            time_range_seconds = int((end_time - start_time).total_seconds())
            random_seconds = random.randint(0, time_range_seconds)
            timestamp = start_time + timedelta(seconds=random_seconds)
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            
            # Select transaction type (weighted random)
            transaction_type = random.choices(
                list(transaction_types.keys()), 
                weights=list(transaction_types.values())
            )[0]
            data["transaction_type"].append(transaction_type)
            
            # Select material
            material_id = random.choice(self.material_ids)
            data["material_id"].append(material_id)
            
            # Select lot based on material
            if material_id in material_to_lots and material_to_lots[material_id]:
                lot_id = random.choice(material_to_lots[material_id])
            else:
                lot_id = random.choice(lot_ids)
            data["lot_id"].append(lot_id)
            
            # Handle from_location and to_location based on transaction type
            if transaction_type == "Receipt":
                # Receipts come from outside (no from_location) to a storage location
                from_location_id = ""
                to_location_id = random.choice(location_ids)
                
                # Reference document is typically a purchase order
                if random.random() < 0.8:  # 80% of receipts have a PO reference
                    reference_document = f"PO:{random.choice(purchase_order_ids)}"
                else:
                    reference_document = ""
                    
            elif transaction_type == "Issue":
                # Issues go from a storage location to production (no to_location)
                from_location_id = random.choice(location_ids)
                to_location_id = ""
                
                # Reference document is typically a work order
                if random.random() < 0.8:  # 80% of issues have a WO reference
                    reference_document = f"WO:{random.choice(self.work_order_ids)}"
                else:
                    reference_document = ""
                    
            elif transaction_type == "Transfer":
                # Transfers go from one storage location to another
                available_locations = list(location_ids)
                from_location_id = random.choice(available_locations)
                
                # Ensure to_location is different from from_location
                remaining_locations = [loc for loc in available_locations if loc != from_location_id]
                if remaining_locations:
                    to_location_id = random.choice(remaining_locations)
                else:
                    # If only one location available, use it for both (not ideal but prevents errors)
                    to_location_id = from_location_id
                    
                # Reference document could be various things
                ref_types = ["", f"WO:{random.choice(self.work_order_ids)}", "Transfer Order:TO-" + str(random.randint(10000, 99999))]
                reference_document = random.choice(ref_types)
                
            elif transaction_type in ["Adjustment", "Scrap"]:
                # Adjustments and scraps occur at a specific location (from_location)
                from_location_id = random.choice(location_ids)
                to_location_id = ""
                
                # Reference document could be various things
                ref_types = ["", "Count Sheet:CS-" + str(random.randint(10000, 99999)), "QC Report:QC-" + str(random.randint(10000, 99999))]
                reference_document = random.choice(ref_types)
                
            elif transaction_type == "Return":
                # Returns go from production (no from_location) to a storage location
                from_location_id = ""
                to_location_id = random.choice(location_ids)
                
                # Reference document is typically a work order
                if random.random() < 0.7:  # 70% of returns have a WO reference
                    reference_document = f"WO:{random.choice(self.work_order_ids)}"
                else:
                    reference_document = ""
                    
            elif transaction_type == "Quality Hold":
                # Quality holds change the status of inventory at a location
                from_location_id = random.choice(location_ids)
                to_location_id = from_location_id  # Same location, just changing status
                
                # Reference document is typically a QC document
                reference_document = "QC Hold:" + str(random.randint(10000, 99999))
                
            else:  # Release
                # Releases change the status of inventory at a location
                from_location_id = random.choice(location_ids)
                to_location_id = from_location_id  # Same location, just changing status
                
                # Reference document is typically a QC document
                reference_document = "QC Release:" + str(random.randint(10000, 99999))
            
            data["from_location_id"].append(from_location_id)
            data["to_location_id"].append(to_location_id)
            data["reference_document"].append(reference_document)
            
            # Determine quantity based on transaction type and maintain inventory
            inventory_key = (material_id, from_location_id, lot_id)
            
            if transaction_type in ["Receipt", "Return"]:
                # Incoming transactions can have any quantity
                quantity = round(random.uniform(10, 1000), 2)
                
                # Update inventory
                destination_key = (material_id, to_location_id, lot_id)
                if destination_key in inventory:
                    inventory[destination_key] = inventory[destination_key] + quantity
                else:
                    inventory[destination_key] = quantity
                    
            elif transaction_type in ["Issue", "Transfer", "Scrap"]:
                # Outgoing transactions need available inventory
                if inventory_key in inventory and inventory[inventory_key] > 0:
                    # Use up to 80% of available inventory
                    max_quantity = inventory[inventory_key] * 0.8
                    quantity = round(random.uniform(1, max_quantity), 2)
                    
                    # Update inventory at source
                    inventory[inventory_key] = inventory[inventory_key] - quantity
                    
                    # Update inventory at destination if applicable
                    if transaction_type == "Transfer" and to_location_id:
                        destination_key = (material_id, to_location_id, lot_id)
                        if destination_key in inventory:
                            inventory[destination_key] = inventory[destination_key] + quantity
                        else:
                            inventory[destination_key] = quantity
                else:
                    # No inventory available, create a small quantity
                    quantity = round(random.uniform(1, 100), 2)
                    
                    # Add to inventory first (anachronistic but ensures future transactions have inventory)
                    if from_location_id:
                        inventory[inventory_key] = quantity
            
            elif transaction_type == "Adjustment":
                # Adjustments can be positive or negative
                if random.random() < 0.5:  # 50% positive adjustments
                    quantity = round(random.uniform(1, 100), 2)
                    
                    # Update inventory
                    if inventory_key in inventory:
                        inventory[inventory_key] = inventory[inventory_key] + quantity
                    else:
                        inventory[inventory_key] = quantity
                else:
                    # Negative adjustment
                    if inventory_key in inventory and inventory[inventory_key] > 0:
                        # Use up to 30% of available inventory
                        max_quantity = inventory[inventory_key] * 0.3
                        quantity = -round(random.uniform(1, max_quantity), 2)
                        
                        # Update inventory
                        inventory[inventory_key] = inventory[inventory_key] + quantity  # Adding negative
                    else:
                        # No inventory available, create a small negative quantity
                        quantity = -round(random.uniform(1, 50), 2)
            
            else:  # Quality Hold or Release
                # These don't change quantity, just status
                if inventory_key in inventory and inventory[inventory_key] > 0:
                    quantity = inventory[inventory_key]  # Use the full amount in inventory
                else:
                    quantity = round(random.uniform(10, 500), 2)  # Create some quantity if none exists
            
            data["quantity"].append(quantity)
            
            # Set work order ID (for certain transaction types)
            if transaction_type in ["Issue", "Return"] and random.random() < 0.8:
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Set operator
            data["operator_id"].append(random.choice(operator_ids))
            
            # Set transaction reason
            if transaction_type in reason_codes:
                data["transaction_reason"].append(random.choice(reason_codes[transaction_type]))
            else:
                data["transaction_reason"].append("")
            
            # Set unit cost (for financial tracking)
            if transaction_type in ["Receipt", "Adjustment", "Return"]:
                # These transaction types typically record cost
                unit_cost = round(random.uniform(5, 500), 2)
            else:
                # These use the existing cost basis
                unit_cost = ""
                
            data["unit_cost"].append(unit_cost)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Sort by timestamp to create a chronological history
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Reset the index after sorting
        df = df.reset_index(drop=True)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "inventory_transactions.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.inventory_transactions_df = df
        
        print(f"Successfully generated {len(df)} inventory transaction records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_material_lots(self, num_lots=200):
        """
        Generate synthetic data for the MaterialLots table from ISA-95 Level 4.
        
        Parameters:
        - num_lots: Number of material lot records to generate
        
        Returns:
        - DataFrame containing the generated material lots data
        """
        # Generate material IDs if materials_df is not provided
        if self.materials_df is None or len(self.materials_df) == 0:
            print("Error: No materials data available. Generate materials first.")
            return None
        
        # Generate supplier IDs if not available
        if not self.supplier_ids:
            print("Generating synthetic supplier IDs...")
            self.supplier_ids = [f"SUP-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Generate storage location IDs if not available
        storage_location_ids = []
        if hasattr(self, 'storage_locations_df') and self.storage_locations_df is not None:
            storage_location_ids = self.storage_locations_df['location_id'].tolist()
        
        if not storage_location_ids:
            print("Generating synthetic storage location IDs...")
            storage_location_ids = [f"LOC-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Define status options
        statuses = ["Available", "Reserved", "In Use", "Consumed", "On Hold", "Quarantined", "Rejected"]
        status_weights = [0.6, 0.1, 0.1, 0.05, 0.05, 0.05, 0.05]  # Mostly available
        
        # Define quality status options
        quality_statuses = ["Released", "Under Test", "Approved", "Rejected", "Pending Review"]
        quality_weights = [0.7, 0.1, 0.1, 0.05, 0.05]  # Mostly released
        
        # Generate data structure
        data = {
            "lot_id": [f"LOT-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_lots)],
            "material_id": [],
            "lot_quantity": [],
            "quantity_unit": [],
            "status": [],
            "creation_date": [],
            "expiration_date": [],
            "supplier_id": [],
            "supplier_lot_id": [],
            "receipt_date": [],
            "storage_location_id": [],
            "quality_status": [],
            "cost_per_unit": [],
            "parent_lot_id": []
        }
        
        # Keep track of created lots by material for parent-child relationships
        lots_by_material = {material_id: [] for material_id in self.materials_df['material_id']}
        
        # Generate data for each material lot
        for i in range(num_lots):
            # Select material
            material = self.materials_df.sample(1).iloc[0]
            material_id = material['material_id']
            data["material_id"].append(material_id)
            
            # Determine if this is a split lot (child lot)
            is_child_lot = False
            if i > num_lots * 0.2 and lots_by_material[material_id] and random.random() < 0.2:  # 20% chance for child lots
                is_child_lot = True
                parent_lot_id = random.choice(lots_by_material[material_id])
                data["parent_lot_id"].append(parent_lot_id)
            else:
                data["parent_lot_id"].append("")
            
            # Remember this lot for potential future splits
            lots_by_material[material_id].append(data["lot_id"][i])
            
            # Set quantity based on material type
            material_type = material['material_type'] if 'material_type' in material else "Raw Material"
            
            if material_type == "Raw Material":
                if is_child_lot:
                    # Child lots are smaller
                    quantity = random.uniform(10, 200)
                else:
                    quantity = random.uniform(100, 2000)
            elif material_type == "Packaging":
                if is_child_lot:
                    quantity = random.uniform(50, 500)
                else:
                    quantity = random.uniform(500, 10000)
            elif material_type in ["WIP", "Intermediate"]:
                if is_child_lot:
                    quantity = random.uniform(5, 50)
                else:
                    quantity = random.uniform(50, 500)
            else:  # Consumable
                if is_child_lot:
                    quantity = random.uniform(1, 20)
                else:
                    quantity = random.uniform(10, 200)
                
            data["lot_quantity"].append(round(quantity, 2))
            
            # Set unit
            unit = material['unit_of_measure'] if 'unit_of_measure' in material else "kg"
            data["quantity_unit"].append(unit)
            
            # Set status
            if is_child_lot:
                # Child lots typically inherit status from parent, but we don't track that here
                # so just make them mostly available
                data["status"].append(random.choices(statuses, weights=[0.8, 0.1, 0.1, 0.0, 0.0, 0.0, 0.0])[0])
            else:
                data["status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Generate creation date (within last 2 years)
            days_ago = random.randint(1, 730)
            creation_date = datetime.now() - timedelta(days=days_ago)
            data["creation_date"].append(creation_date.strftime("%Y-%m-%d"))
            
            # Set expiration date based on material type
            if material_type == "Raw Material":
                # Raw materials typically have longer shelf life
                shelf_life_days = random.randint(365, 1825)  # 1-5 years
            elif material_type == "Packaging":
                # Packaging materials have very long shelf life
                shelf_life_days = random.randint(730, 3650)  # 2-10 years
            elif material_type in ["WIP", "Intermediate"]:
                # Intermediate products have shorter shelf life
                shelf_life_days = random.randint(30, 365)  # 1 month to 1 year
            else:  # Consumable
                # Consumables vary widely
                shelf_life_days = random.randint(90, 1095)  # 3 months to 3 years
                
            expiration_date = creation_date + timedelta(days=shelf_life_days)
            data["expiration_date"].append(expiration_date.strftime("%Y-%m-%d"))
            
            # Set supplier info
            if material_type in ["Raw Material", "Packaging", "Consumable"] and not is_child_lot:
                # External materials have supplier info
                data["supplier_id"].append(random.choice(self.supplier_ids))
                data["supplier_lot_id"].append(f"SUPLOT-{random.randint(10000, 99999)}")
                
                # Receipt date is between creation date and today
                max_receipt_days = min((datetime.now() - creation_date).days, 30)  # Within 30 days of creation
                if max_receipt_days > 0:
                    receipt_days = random.randint(0, max_receipt_days)
                else:
                    receipt_days = 0
                receipt_date = creation_date + timedelta(days=receipt_days)
                data["receipt_date"].append(receipt_date.strftime("%Y-%m-%d"))
            else:
                # Internally produced materials don't have supplier info
                data["supplier_id"].append("")
                data["supplier_lot_id"].append("")
                data["receipt_date"].append("")
            
            # Set storage location
            data["storage_location_id"].append(random.choice(storage_location_ids))
            
            # Set quality status
            if data["status"][-1] in ["On Hold", "Quarantined", "Rejected"]:
                # Problematic lots have corresponding quality status
                if data["status"][-1] == "On Hold":
                    data["quality_status"].append("Under Test")
                elif data["status"][-1] == "Quarantined":
                    data["quality_status"].append("Pending Review")
                else:  # Rejected
                    data["quality_status"].append("Rejected")
            else:
                # Normal lots have standard quality status
                data["quality_status"].append(random.choices(quality_statuses, weights=quality_weights)[0])
            
            # Set cost per unit
            if material_type == "Raw Material":
                cost = random.uniform(1, 100)
            elif material_type == "Packaging":
                cost = random.uniform(0.1, 10)
            elif material_type in ["WIP", "Intermediate"]:
                cost = random.uniform(5, 200)
            else:  # Consumable
                cost = random.uniform(0.5, 50)
                
            data["cost_per_unit"].append(round(cost, 2))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "material_lots.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.material_lots_df = df
        self.lot_ids = df["lot_id"].tolist()
        
        print(f"Successfully generated {len(df)} material lot records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_material_consumption(self, num_records=300):
        """
        Generate synthetic data for the MaterialConsumption table from ISA-95 Level 4.
        
        Parameters:
        - num_records: Number of material consumption records to generate
        
        Returns:
        - DataFrame containing the generated material consumption data
        """
        if self.material_lots_df is None or len(self.material_lots_df) == 0:
            print("Error: No material lots data available. Generate material lots first.")
            return None
        
        # Filter material lots that can be consumed (active or in process)
        consumable_lots = self.material_lots_df[
            (self.material_lots_df['status'].isin(['Active', 'In Process', 'Consumed']))
        ]
        
        if len(consumable_lots) == 0:
            print("Warning: No consumable material lots found. Using all available lots.")
            consumable_lots = self.material_lots_df
        
        # Generate batch step IDs if needed
        batch_step_ids = [f"STEP-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Generate operator IDs if needed
        if not self.personnel_ids:
            print("Generating synthetic operator IDs...")
            operator_ids = [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        else:
            operator_ids = self.personnel_ids
        
        # Make sure equipment IDs are available
        if not self.equipment_ids:
            print("Generating synthetic equipment IDs...")
            self.equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Generate data structure
        data = {
            "consumption_id": [f"CONS-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_records)],
            "lot_id": [],
            "batch_id": [],
            "work_order_id": [],
            "timestamp": [],
            "quantity": [],
            "unit": [],
            "equipment_id": [],
            "step_id": [],
            "operator_id": [],
            "planned_consumption": [],
            "consumption_variance": []
        }
        
        # Create a mapping of batches to work orders (normally this would come from Batches table)
        batch_to_work_order = {}
        for batch_id in self.batch_ids:
            batch_to_work_order[batch_id] = random.choice(self.work_order_ids)
        
        # Keep track of lot consumption to avoid over-consumption
        lot_consumption = {lot_id: 0 for lot_id in self.material_lots_df['lot_id']}
        
        # Get lot quantities and units if available
        lot_quantities = {}
        lot_units = {}
        
        for _, lot in self.material_lots_df.iterrows():
            if 'lot_id' in lot and 'lot_quantity' in lot:
                lot_quantities[lot['lot_id']] = lot['lot_quantity']
                if 'quantity_unit' in lot:
                    lot_units[lot['lot_id']] = lot['quantity_unit']
        
        # Generate timestamps distributed over the last year
        start_time = datetime.now() - timedelta(days=365)
        end_time = datetime.now()
        
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        timestamps = []
        
        for _ in range(num_records):
            random_minutes = random.randint(0, time_range_minutes)
            timestamp = start_time + timedelta(minutes=random_minutes)
            timestamps.append(timestamp)
        
        # Sort timestamps (older to newer)
        timestamps.sort()
        
        # Generate data for each consumption record
        for i in range(num_records):
            # Select lot
            if len(consumable_lots) > 0:
                # Try to find a lot that hasn't been fully consumed
                available_lots = [lot_id for lot_id in consumable_lots['lot_id'] 
                                if lot_id not in lot_consumption or 
                                (lot_id in lot_quantities and lot_consumption[lot_id] < lot_quantities[lot_id])]
                
                if available_lots:
                    lot_row = consumable_lots[consumable_lots['lot_id'].isin(available_lots)].sample(1)
                    lot_id = lot_row['lot_id'].iloc[0]
                    
                    # Use the lot's unit
                    unit = lot_row['quantity_unit'].iloc[0] if 'quantity_unit' in lot_row.columns else "kg"
                    
                    # Maximum consumption is the lot quantity
                    max_consumption = float(lot_row['lot_quantity'].iloc[0])
                    
                    # Typical consumption is a portion of the lot
                    typical_consumption = max_consumption * random.uniform(0.05, 0.9)
                else:
                    # If all lots are consumed, just pick a random one
                    lot_row = consumable_lots.sample(1)
                    lot_id = lot_row['lot_id'].iloc[0]
                    unit = lot_row['quantity_unit'].iloc[0] if 'quantity_unit' in lot_row.columns else "kg"
                    max_consumption = float(lot_row['lot_quantity'].iloc[0])
                    typical_consumption = max_consumption * random.uniform(0.05, 0.9)
            else:
                # Fallback if no consumable lots are available
                lot_id = random.choice(self.material_lots_df['lot_id'].tolist())
                unit = random.choice(["kg", "L", "units", "g", "ml", "pieces"])
                max_consumption = random.uniform(100, 5000)
                typical_consumption = max_consumption * random.uniform(0.05, 0.9)
                
            data["lot_id"].append(lot_id)
            data["unit"].append(unit)
            
            # Set timestamp
            timestamp = timestamps[i]
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            
            # Assign to batch and work order
            # Consumption records typically have both, but we'll allow some variation
            if random.random() < 0.9:  # 90% have batch
                data["batch_id"].append(random.choice(self.batch_ids))
            else:
                data["batch_id"].append("")
                
            if random.random() < 0.8:  # 80% have work order
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Assign equipment
            data["equipment_id"].append(random.choice(self.equipment_ids))
            
            # Assign batch step
            if random.random() < 0.7:  # 70% have specific step
                data["step_id"].append(random.choice(batch_step_ids))
            else:
                data["step_id"].append("")
            
            # Assign operator
            data["operator_id"].append(random.choice(operator_ids))
            
            # Generate consumption quantity
            # Actual consumption has some variance from planned
            planned_consumption = round(typical_consumption, 2)
            data["planned_consumption"].append(planned_consumption)
            
            # Actual consumption varies from planned
            variation_pct = random.normalvariate(0, 0.05)  # Normal distribution around 0 with 5% std dev
            actual_consumption = planned_consumption * (1 + variation_pct)
            actual_consumption = round(min(max_consumption, max(0, actual_consumption)), 2)
            data["quantity"].append(actual_consumption)
            
            # Update lot consumption tracking
            if lot_id in lot_consumption:
                lot_consumption[lot_id] += actual_consumption
            else:
                lot_consumption[lot_id] = actual_consumption
            
            # Calculate variance
            variance = actual_consumption - planned_consumption
            data["consumption_variance"].append(round(variance, 2))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "material_consumption.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.material_consumption_df = df
        
        print(f"Successfully generated {len(df)} material consumption records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_costs(self, num_costs=500, start_time=None, end_time=None):
        """
        Generate synthetic data for the Costs table from ISA-95 Level 4.
        
        Parameters:
        - num_costs: Number of cost records to generate
        - start_time: Start time for cost dates (defaults to 365 days ago)
        - end_time: End time for cost dates (defaults to current date)
        
        Returns:
        - DataFrame containing the generated costs data
        """
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now()
        
        # Generate work order IDs if not available
        if not self.work_order_ids:
            print("Generating synthetic work order IDs...")
            self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Generate product IDs if not available
        if not self.product_ids:
            print("Generating synthetic product IDs...")
            self.product_ids = [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Generate equipment IDs if not available
        if not self.equipment_ids:
            print("Generating synthetic equipment IDs...")
            self.equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Generate batch IDs if not available
        if not self.batch_ids:
            print("Generating synthetic batch IDs...")
            self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(40)]
        
        # Define cost types and their probabilities
        cost_types = {
            "Labor": 0.3,
            "Material": 0.3,
            "Overhead": 0.15,
            "Energy": 0.05,
            "Maintenance": 0.1,
            "Quality": 0.05,
            "Setup": 0.05
        }
        
        # Define cost categories
        cost_categories = {
            "Labor": ["Direct Labor", "Indirect Labor", "Supervision", "Quality Control", "Engineering"],
            "Material": ["Raw Material", "Packaging", "Consumables", "Spare Parts", "Chemicals"],
            "Overhead": ["Facility", "Depreciation", "Insurance", "Utilities", "IT"],
            "Energy": ["Electricity", "Gas", "Water", "Steam", "Compressed Air"],
            "Maintenance": ["Preventive", "Corrective", "Calibration", "Cleaning", "Inspection"],
            "Quality": ["Testing", "Inspection", "Rework", "Documentation", "Validation"],
            "Setup": ["Machine Setup", "Changeover", "Tooling", "Programming", "Validation"]
        }
        
        # Define cost centers
        cost_centers = ["Production", "Maintenance", "Quality", "Engineering", "Facilities", "Supply Chain", 
                    "Utilities", "R&D", "Administration"]
        
        # Define currencies
        currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CNY"]
        currency_weights = [0.6, 0.15, 0.1, 0.05, 0.05, 0.03, 0.02]  # Mostly USD
        
        # Generate data structure
        data = {
            "cost_id": [f"COST-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_costs)],
            "cost_type": [],
            "work_order_id": [],
            "product_id": [],
            "equipment_id": [],
            "batch_id": [],
            "timestamp": [],
            "amount": [],
            "currency": [],
            "cost_category": [],
            "cost_center": [],
            "planned_cost": [],
            "variance": []
        }
        
        # Generate data for each cost record
        for i in range(num_costs):
            # Select cost type (weighted random)
            cost_type = random.choices(
                list(cost_types.keys()), 
                weights=list(cost_types.values())
            )[0]
            data["cost_type"].append(cost_type)
            
            # Generate timestamp within the specified range
            time_range_seconds = int((end_time - start_time).total_seconds())
            random_seconds = random.randint(0, time_range_seconds)
            timestamp = start_time + timedelta(seconds=random_seconds)
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            
            # Assign work order, product, equipment, and batch based on cost type
            # Not all costs are associated with all entities
            
            # Work order association
            if cost_type in ["Labor", "Material", "Setup", "Quality"] and random.random() < 0.9:
                # These cost types are almost always associated with work orders
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                # Other cost types may or may not be associated with work orders
                if random.random() < 0.5:
                    data["work_order_id"].append(random.choice(self.work_order_ids))
                else:
                    data["work_order_id"].append("")
            
            # Product association
            if cost_type in ["Material", "Quality"] and random.random() < 0.9:
                # These cost types are almost always associated with products
                data["product_id"].append(random.choice(self.product_ids))
            else:
                # Other cost types may or may not be associated with products
                if random.random() < 0.4:
                    data["product_id"].append(random.choice(self.product_ids))
                else:
                    data["product_id"].append("")
            
            # Equipment association
            if cost_type in ["Maintenance", "Energy", "Setup"] and random.random() < 0.9:
                # These cost types are almost always associated with equipment
                data["equipment_id"].append(random.choice(self.equipment_ids))
            else:
                # Other cost types may or may not be associated with equipment
                if random.random() < 0.4:
                    data["equipment_id"].append(random.choice(self.equipment_ids))
                else:
                    data["equipment_id"].append("")
            
            # Batch association
            if data["work_order_id"][-1] and random.random() < 0.7:
                # If associated with a work order, likely associated with a batch
                data["batch_id"].append(random.choice(self.batch_ids))
            else:
                data["batch_id"].append("")
            
            # Generate cost amount based on cost type
            if cost_type == "Labor":
                # Labor costs are typically higher
                amount = random.uniform(100, 5000)
            elif cost_type == "Material":
                # Material costs vary widely
                amount = random.uniform(50, 10000)
            elif cost_type == "Overhead":
                # Overhead costs can be substantial
                amount = random.uniform(500, 20000)
            elif cost_type == "Energy":
                # Energy costs are moderate
                amount = random.uniform(100, 3000)
            elif cost_type == "Maintenance":
                # Maintenance costs depend on the scope
                amount = random.uniform(200, 8000)
            elif cost_type == "Quality":
                # Quality costs are typically lower
                amount = random.uniform(50, 2000)
            else:  # Setup
                # Setup costs are moderate
                amount = random.uniform(100, 3000)
                
            data["amount"].append(round(amount, 2))
            
            # Set currency (weighted random)
            data["currency"].append(random.choices(currencies, weights=currency_weights)[0])
            
            # Set cost category
            if cost_type in cost_categories:
                data["cost_category"].append(random.choice(cost_categories[cost_type]))
            else:
                data["cost_category"].append("General")
            
            # Set cost center
            if cost_type == "Labor":
                center_weights = [0.6, 0.05, 0.1, 0.05, 0.05, 0.05, 0.05, 0.05, 0.0]  # Mostly Production
            elif cost_type == "Material":
                center_weights = [0.5, 0.0, 0.05, 0.05, 0.0, 0.3, 0.0, 0.1, 0.0]  # Production or Supply Chain
            elif cost_type == "Overhead":
                center_weights = [0.2, 0.05, 0.05, 0.05, 0.3, 0.05, 0.1, 0.05, 0.15]  # Varied
            elif cost_type == "Energy":
                center_weights = [0.3, 0.05, 0.05, 0.05, 0.1, 0.0, 0.45, 0.0, 0.0]  # Utilities or Production
            elif cost_type == "Maintenance":
                center_weights = [0.1, 0.7, 0.0, 0.1, 0.1, 0.0, 0.0, 0.0, 0.0]  # Mostly Maintenance
            elif cost_type == "Quality":
                center_weights = [0.1, 0.0, 0.7, 0.1, 0.0, 0.0, 0.0, 0.1, 0.0]  # Mostly Quality
            else:  # Setup
                center_weights = [0.7, 0.1, 0.0, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0]  # Mostly Production
                
            data["cost_center"].append(random.choices(cost_centers, weights=center_weights)[0])
            
            # Set planned cost and variance
            # About 70% of costs have a planned amount
            if random.random() < 0.7:
                # Planned costs are usually close to actual but can vary
                variation = random.uniform(0.7, 1.3)  # -30% to +30%
                planned_cost = amount / variation
                data["planned_cost"].append(round(planned_cost, 2))
                
                # Calculate variance (actual - planned)
                variance = amount - planned_cost
                data["variance"].append(round(variance, 2))
            else:
                data["planned_cost"].append("")
                data["variance"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "costs.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.costs_df = df
        
        print(f"Successfully generated {len(df)} cost records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_cogs(self, num_cogs=200, start_time=None, end_time=None):
        """
        Generate synthetic data for the COGS (Cost of Goods Sold) table.
        
        Parameters:
        - num_cogs: Number of COGS records to generate
        - start_time: Start time for COGS dates (defaults to 365 days ago)
        - end_time: End time for COGS dates (defaults to current date)
        
        Returns:
        - DataFrame containing the generated COGS data
        """
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now()
        
        # Make sure we have products data
        if self.products_df is None or len(self.products_df) == 0:
            print("Error: No products data available.")
            return None
        
        # Generate batch IDs if not available
        if not self.batch_ids:
            print("Generating synthetic batch IDs...")
            self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(100)]
            
            # Create a mapping of batch to product
            batch_to_product = {}
            for batch_id in self.batch_ids:
                batch_to_product[batch_id] = random.choice(self.product_ids)
        else:
            # Create a mapping of batch to product
            batch_to_product = {}
            for batch_id in self.batch_ids:
                batch_to_product[batch_id] = random.choice(self.product_ids)
        
        # Generate work order IDs if not available
        if not self.work_order_ids:
            print("Generating synthetic work order IDs...")
            self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(80)]
            
            # Create a mapping of work order to product
            work_order_to_product = {}
            for wo_id in self.work_order_ids:
                work_order_to_product[wo_id] = random.choice(self.product_ids)
        else:
            # Create a mapping of work order to product
            work_order_to_product = {}
            for wo_id in self.work_order_ids:
                work_order_to_product[wo_id] = random.choice(self.product_ids)
        
        # Define cost categories
        cost_categories = ["Direct Materials", "Direct Labor", "Manufacturing Overhead", "Packaging", 
                        "Quality Control", "Setup", "Utilities", "Depreciation"]
        
        # Define COGS types
        cogs_types = ["Standard", "Actual", "Variance"]
        
        # Define periods (monthly, quarterly, etc.)
        period_types = ["Monthly", "Quarterly", "Annual", "Batch", "Product Run"]
        
        # Generate data structure
        data = {
            "cogs_id": [f"COGS-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_cogs)],
            "product_id": [],
            "batch_id": [],
            "work_order_id": [],
            "period_type": [],
            "period_start_date": [],
            "period_end_date": [],
            "cogs_type": [],
            "direct_materials_cost": [],
            "direct_labor_cost": [],
            "manufacturing_overhead_cost": [],
            "packaging_cost": [],
            "quality_cost": [],
            "other_cost": [],
            "total_cogs": [],
            "units_produced": [],
            "cost_per_unit": [],
            "currency": [],
            "calculation_date": [],
            "notes": []
        }
        
        # Define product costing profiles based on product family
        # This defines the relative proportion of each cost category for different product types
        costing_profiles = {
            "Pharmaceutical": {
                "direct_materials": (0.3, 0.5),    # 30-50% of total cost
                "direct_labor": (0.1, 0.2),        # 10-20% of total cost
                "manufacturing_overhead": (0.15, 0.25), # 15-25% of total cost
                "packaging": (0.05, 0.1),          # 5-10% of total cost
                "quality": (0.1, 0.2),             # 10-20% of total cost
                "other": (0.05, 0.1)               # 5-10% of total cost
            },
            "Food & Beverage": {
                "direct_materials": (0.4, 0.6),    # 40-60% of total cost
                "direct_labor": (0.1, 0.2),        # 10-20% of total cost
                "manufacturing_overhead": (0.1, 0.2), # 10-20% of total cost
                "packaging": (0.1, 0.15),          # 10-15% of total cost
                "quality": (0.05, 0.1),            # 5-10% of total cost
                "other": (0.05, 0.1)               # 5-10% of total cost
            },
            "Chemical": {
                "direct_materials": (0.5, 0.65),   # 50-65% of total cost
                "direct_labor": (0.05, 0.15),      # 5-15% of total cost
                "manufacturing_overhead": (0.15, 0.25), # 15-25% of total cost
                "packaging": (0.05, 0.1),          # 5-10% of total cost
                "quality": (0.05, 0.1),            # 5-10% of total cost
                "other": (0.05, 0.1)               # 5-10% of total cost
            },
            "Electronics": {
                "direct_materials": (0.5, 0.7),    # 50-70% of total cost
                "direct_labor": (0.1, 0.2),        # 10-20% of total cost
                "manufacturing_overhead": (0.1, 0.2), # 10-20% of total cost
                "packaging": (0.03, 0.07),         # 3-7% of total cost
                "quality": (0.05, 0.1),            # 5-10% of total cost
                "other": (0.03, 0.1)               # 3-10% of total cost
            },
            "Automotive": {
                "direct_materials": (0.6, 0.75),   # 60-75% of total cost
                "direct_labor": (0.1, 0.15),       # 10-15% of total cost
                "manufacturing_overhead": (0.1, 0.15), # 10-15% of total cost
                "packaging": (0.01, 0.05),         # 1-5% of total cost
                "quality": (0.05, 0.1),            # 5-10% of total cost
                "other": (0.05, 0.1)               # 5-10% of total cost
            },
            "Consumer Goods": {
                "direct_materials": (0.4, 0.55),   # 40-55% of total cost
                "direct_labor": (0.15, 0.25),      # 15-25% of total cost
                "manufacturing_overhead": (0.1, 0.2), # 10-20% of total cost
                "packaging": (0.1, 0.2),           # 10-20% of total cost
                "quality": (0.05, 0.1),            # 5-10% of total cost
                "other": (0.05, 0.1)               # 5-10% of total cost
            }
        }
        
        # Create a mapping of product to base cost and family
        product_base_costs = {}
        product_families = {}
        
        for _, product in self.products_df.iterrows():
            product_id = product['product_id']
            
            # Get product family
            if 'product_family' in product:
                family = product['product_family']
            else:
                family = random.choice(list(costing_profiles.keys()))
            product_families[product_id] = family
            
            # Get base cost
            if 'base_cost' in product and pd.notna(product['base_cost']):
                product_base_costs[product_id] = product['base_cost']
            else:
                # Generate synthetic base costs
                if family == "Pharmaceutical":
                    base_cost = random.uniform(50, 5000)
                elif family == "Food & Beverage":
                    base_cost = random.uniform(5, 100)
                elif family == "Chemical":
                    base_cost = random.uniform(20, 500)
                elif family == "Electronics":
                    base_cost = random.uniform(50, 2000)
                elif family == "Automotive":
                    base_cost = random.uniform(100, 3000)
                else:  # Consumer Goods
                    base_cost = random.uniform(10, 300)
                    
                product_base_costs[product_id] = base_cost
        
        # Generate data for each COGS record
        for i in range(num_cogs):
            # Determine if this is a batch-level or product-level COGS
            is_batch_level = random.random() < 0.7  # 70% batch-level, 30% product-level
            
            if is_batch_level:
                # Batch-level COGS
                batch_id = random.choice(self.batch_ids)
                data["batch_id"].append(batch_id)
                
                # Get product associated with this batch
                if batch_id in batch_to_product:
                    product_id = batch_to_product[batch_id]
                else:
                    product_id = random.choice(self.product_ids)
                
                data["product_id"].append(product_id)
                
                # Find a work order for this batch
                # For simplicity, we'll just randomly assign one
                if random.random() < 0.8:  # 80% have work order
                    data["work_order_id"].append(random.choice(self.work_order_ids))
                else:
                    data["work_order_id"].append("")
                    
                # Batch-level is always "Batch" period type
                data["period_type"].append("Batch")
                
                # Generate random dates for the batch
                days_ago = random.randint(1, 365)
                start_date = datetime.now() - timedelta(days=days_ago)
                end_date = start_date + timedelta(days=random.randint(1, 30))
                data["period_start_date"].append(start_date.strftime("%Y-%m-%d"))
                data["period_end_date"].append(end_date.strftime("%Y-%m-%d"))
                
                # Set units produced (batch size)
                units_produced = random.randint(50, 10000)
                data["units_produced"].append(units_produced)
                
            else:
                # Product-level COGS
                product_id = random.choice(self.product_ids)
                data["product_id"].append(product_id)
                data["batch_id"].append("")  # No specific batch
                data["work_order_id"].append("")  # No specific work order
                
                # Select period type
                period_type = random.choice(["Monthly", "Quarterly", "Annual"])
                data["period_type"].append(period_type)
                
                # Generate period dates based on type
                time_range_days = (end_time - start_time).days
                start_offset = random.randint(0, time_range_days - 1)
                period_start = start_time + timedelta(days=start_offset)
                
                if period_type == "Monthly":
                    period_end = period_start + timedelta(days=30)
                elif period_type == "Quarterly":
                    period_end = period_start + timedelta(days=90)
                else:  # Annual
                    period_end = period_start + timedelta(days=365)
                    
                data["period_start_date"].append(period_start.strftime("%Y-%m-%d"))
                data["period_end_date"].append(period_end.strftime("%Y-%m-%d"))
                
                # Set units produced (product-level is typically higher)
                units_produced = random.randint(1000, 100000)
                data["units_produced"].append(units_produced)
            
            # Set COGS type
            cogs_type = random.choice(cogs_types)
            data["cogs_type"].append(cogs_type)
            
            # Calculate costs based on product family
            product_family = product_families.get(product_id, "Consumer Goods")
            profile = costing_profiles.get(product_family, costing_profiles["Consumer Goods"])
            
            # Get base cost for this product
            base_cost = product_base_costs.get(product_id, random.uniform(10, 1000))
            
            # Adjust base cost for variance
            if cogs_type == "Standard":
                # Standard costs are the baseline
                cost_multiplier = 1.0
            elif cogs_type == "Actual":
                # Actual costs vary slightly from standard
                cost_multiplier = random.uniform(0.9, 1.1)
            else:  # Variance
                # Variance shows the difference
                cost_multiplier = random.uniform(0.8, 1.2)
                
            # Calculate total COGS
            total_cost = base_cost * units_produced * cost_multiplier
            
            # Distribute costs across categories based on product family profile
            # Direct Materials
            min_pct, max_pct = profile["direct_materials"]
            direct_materials_pct = random.uniform(min_pct, max_pct)
            direct_materials_cost = total_cost * direct_materials_pct
            data["direct_materials_cost"].append(round(direct_materials_cost, 2))
            
            # Direct Labor
            min_pct, max_pct = profile["direct_labor"]
            direct_labor_pct = random.uniform(min_pct, max_pct)
            direct_labor_cost = total_cost * direct_labor_pct
            data["direct_labor_cost"].append(round(direct_labor_cost, 2))
            
            # Manufacturing Overhead
            min_pct, max_pct = profile["manufacturing_overhead"]
            overhead_pct = random.uniform(min_pct, max_pct)
            overhead_cost = total_cost * overhead_pct
            data["manufacturing_overhead_cost"].append(round(overhead_cost, 2))
            
            # Packaging
            min_pct, max_pct = profile["packaging"]
            packaging_pct = random.uniform(min_pct, max_pct)
            packaging_cost = total_cost * packaging_pct
            data["packaging_cost"].append(round(packaging_cost, 2))
            
            # Quality
            min_pct, max_pct = profile["quality"]
            quality_pct = random.uniform(min_pct, max_pct)
            quality_cost = total_cost * quality_pct
            data["quality_cost"].append(round(quality_cost, 2))
            
            # Other
            min_pct, max_pct = profile["other"]
            other_pct = random.uniform(min_pct, max_pct)
            other_cost = total_cost * other_pct
            data["other_cost"].append(round(other_cost, 2))
            
            # Recalculate total as sum of components to ensure consistency
            total_cogs = (direct_materials_cost + direct_labor_cost + overhead_cost + 
                        packaging_cost + quality_cost + other_cost)
            data["total_cogs"].append(round(total_cogs, 2))
            
            # Calculate cost per unit
            if units_produced > 0:
                cost_per_unit = total_cogs / units_produced
            else:
                cost_per_unit = 0
                
            data["cost_per_unit"].append(round(cost_per_unit, 2))
            
            # Set currency (mostly USD)
            currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
            currency_weights = [0.7, 0.1, 0.1, 0.05, 0.05]  # Mostly USD
            data["currency"].append(random.choices(currencies, weights=currency_weights)[0])
            
            # Set calculation date (typically at the end of the period)
            calc_date = datetime.strptime(data["period_end_date"][-1], "%Y-%m-%d") + timedelta(days=random.randint(1, 5))
            data["calculation_date"].append(calc_date.strftime("%Y-%m-%d"))
            
            # Set notes
            if random.random() < 0.3:  # 30% chance of having notes
                notes_options = [
                    f"Standard costing based on {product_family} category averages",
                    "Includes material price variance adjustment",
                    "Labor costs higher due to overtime",
                    "Overhead allocation based on machine hours",
                    f"Cost reconciliation for {data['period_type'][-1]} period",
                    "Adjusted for yield loss",
                    "Includes rework costs",
                    "Based on actual consumption data",
                    "Preliminary calculation pending final QC review",
                    "Includes expedited shipping costs"
                ]
                data["notes"].append(random.choice(notes_options))
            else:
                data["notes"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "cogs.csv")
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.cogs_df = df
        
        print(f"Successfully generated {len(df)} COGS records.")
        print(f"Data saved to {output_file}")
        
        return df

def main():
    """Main function to run the ISA-95 Level 4 data generator"""
    parser = argparse.ArgumentParser(description='Generate ISA-95 Level 4 data')
    parser.add_argument('--output', type=str, default='data', 
                      help='Output directory for generated data (default: data)')
    parser.add_argument('--products', type=int, default=100, 
                      help='Number of product records to generate (default: 100)')
    parser.add_argument('--materials', type=int, default=150, 
                      help='Number of material records to generate (default: 150)')
    parser.add_argument('--customers', type=int, default=100, 
                      help='Number of customer records to generate (default: 100)')
    parser.add_argument('--customer-orders', type=int, default=300, 
                      help='Number of customer order records to generate (default: 300)')
    parser.add_argument('--suppliers', type=int, default=50, 
                      help='Number of supplier records to generate (default: 50)')
    parser.add_argument('--purchase-orders', type=int, default=200, 
                      help='Number of purchase order records to generate (default: 200)')
    parser.add_argument('--production-schedules', type=int, default=20, 
                      help='Number of production schedule records to generate (default: 20)')
    parser.add_argument('--facilities', type=int, default=15, 
                      help='Number of facility records to generate (default: 15)')
    parser.add_argument('--inventory-transactions', type=int, default=1000, 
                      help='Number of inventory transaction records to generate (default: 1000)')
    parser.add_argument('--material-lots', type=int, default=200, 
                      help='Number of material lot records to generate (default: 200)')
    parser.add_argument('--material-consumption', type=int, default=300, 
                      help='Number of material consumption records to generate (default: 300)')
    parser.add_argument('--costs', type=int, default=500, 
                      help='Number of cost records to generate (default: 500)')
    parser.add_argument('--use-level3', action='store_true',
                      help='Use existing Level 3 data for consistency (default: False)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ISA95Level4DataGenerator(output_dir=args.output, level3_data_available=args.use_level3)
    
    # Start timer
    start_time = time.time()
    
    # Generate all data
    generator.generate_all_data(
        num_products=args.products,
        num_materials=args.materials,
        num_customers=args.customers,
        num_customer_orders=args.customer_orders,
        num_suppliers=args.suppliers,
        num_purchase_orders=args.purchase_orders,
        num_production_schedules=args.production_schedules,
        num_facilities=args.facilities,
        num_inventory_transactions=args.inventory_transactions,
        num_material_lots=args.material_lots,
        num_material_consumption=args.material_consumption,
        num_costs=args.costs
    )
    
    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\nTotal generation time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()