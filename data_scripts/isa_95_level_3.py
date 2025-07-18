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

class ISA95Level3DataGenerator:
    """
    Generator for ISA-95 Level 3 (Manufacturing Operations Management) data.
    
    This class generates synthetic data for all tables in Level 3:
    - Work Orders
    - Material Lots
    - Material Transactions
    - Material Consumption
    - Quality Tests
    - Quality Events
    - Resource Utilization
    - Maintenance Activities
    - Production Performance
    """
    
    def __init__(self, output_dir="data", level2_data_available=False):
        """
        Initialize the data generator.
        
        Parameters:
        - output_dir: Directory where generated data will be saved
        - level2_data_available: Whether Level 2 data is available to reference
        """
        self.output_dir = output_dir
        self.level2_data_available = level2_data_available
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Track generated data for relationships
        self.equipment_df = None
        self.facilities_df = None
        self.process_areas_df = None
        self.work_orders_df = None
        self.material_lots_df = None
        self.material_transactions_df = None
        self.material_consumption_df = None
        self.quality_tests_df = None
        self.quality_events_df = None
        self.resource_utilization_df = None
        self.maintenance_activities_df = None
        self.production_performance_df = None
        
        # Define common reference data
        self.equipment_ids = []
        self.facility_ids = []
        self.area_ids = []
        self.product_ids = []
        self.material_ids = []
        self.supplier_ids = []
        self.customer_ids = []
        self.work_order_ids = []
        self.batch_ids = []
        self.lot_ids = []
        self.personnel_ids = []
        self.shift_ids = []
        
        # Initialize reference data
        self._init_reference_data()
    
    def _init_reference_data(self):
        """Initialize reference data used across tables"""
        # Try to load Level 2 data for references if available
        self._load_level2_data()
        
        # Initialize equipment_ids if empty
        if not self.equipment_ids and self.equipment_df is not None:
            self.equipment_ids = self.equipment_df['equipment_id'].unique().tolist()

        # Create product IDs
        if not self.product_ids:
            self.product_ids = [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create material IDs
        if not self.material_ids:
            self.material_ids = [f"MAT-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create supplier IDs
        if not self.supplier_ids:
            self.supplier_ids = [f"SUP-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create customer IDs
        if not self.customer_ids:
            self.customer_ids = [f"CUST-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create work order IDs
        if not self.work_order_ids:
            self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(200)]
        
        # Create batch IDs
        if not self.batch_ids:
            self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create personnel IDs
        if not self.personnel_ids:
            self.personnel_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
            
        # Create shift IDs
        if not self.shift_ids:
            self.shift_ids = [f"SHIFT-{uuid.uuid4().hex[:8].upper()}" for _ in range(4)]
    
    def _load_level2_data(self):
        """Load existing Level 2 data if available for reference"""
        if not self.level2_data_available:
            return
        
        try:
            # Try to load equipment data
            equipment_file = os.path.join(self.output_dir, "equipment.csv")
            if os.path.exists(equipment_file):
                self.equipment_df = pd.read_csv(equipment_file)
                if 'equipment_id' in self.equipment_df.columns:
                    self.equipment_ids = self.equipment_df['equipment_id'].unique().tolist()
            
            # Try to load facilities data
            facilities_file = os.path.join(self.output_dir, "facilities.csv")
            if os.path.exists(facilities_file):
                self.facilities_df = pd.read_csv(facilities_file)
                if 'facility_id' in self.facilities_df.columns:
                    self.facility_ids = self.facilities_df['facility_id'].unique().tolist()
            
            # Try to load process areas data
            areas_file = os.path.join(self.output_dir, "process_areas.csv")
            if os.path.exists(areas_file):
                self.process_areas_df = pd.read_csv(areas_file)
                if 'area_id' in self.process_areas_df.columns:
                    self.area_ids = self.process_areas_df['area_id'].unique().tolist()
            
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
                        
            print(f"Loaded Level 2 data references")
            
        except Exception as e:
            print(f"Warning: Could not load Level 2 data: {e}")
    
    def generate_all_data(self, num_work_orders=200, num_material_lots=300, num_material_transactions=500,
                         num_material_consumptions=400, num_quality_tests=500, num_quality_events=100,
                         num_resource_utilization=1000, num_maintenance_activities=300, num_performance_records=1000):
        """
        Generate data for all Level 3 tables.
        
        Parameters:
        - num_work_orders: Number of work order records to generate
        - num_material_lots: Number of material lot records to generate
        - num_material_transactions: Number of material transaction records to generate
        - num_material_consumptions: Number of material consumption records to generate
        - num_quality_tests: Number of quality test records to generate
        - num_quality_events: Number of quality event records to generate
        - num_resource_utilization: Number of resource utilization records to generate
        - num_maintenance_activities: Number of maintenance activity records to generate
        - num_performance_records: Number of production performance records to generate
        """
        print("=== ISA-95 Level 3 Data Generation ===")
        
        # Define date ranges
        start_time = datetime.now() - timedelta(days=180)
        end_time = datetime.now()
        
        # First, try to load required equipment data
        if self.equipment_df is None:
            try:
                equipment_file = os.path.join(self.output_dir, "equipment.csv")
                self.equipment_df = pd.read_csv(equipment_file)
                print("Loaded existing equipment data")
            except Exception as e:
                print(f"Error: No equipment data available. Please run Level 2 data generation first: {e}")
                return
        
        # Generate data for each table in a logical order to maintain relationships
        print(f"\n1. Generating {num_work_orders} Work Orders...")
        self.generate_work_orders(num_work_orders, start_time, end_time)
        
        print(f"\n2. Generating {num_material_lots} Material Lots...")
        self.generate_material_lots(num_material_lots, start_time, end_time)
        
        print(f"\n3. Generating {num_material_transactions} Material Transactions...")
        self.generate_material_transactions(num_material_transactions, start_time, end_time)
        
        print(f"\n4. Generating {num_material_consumptions} Material Consumptions...")
        self.generate_material_consumptions(num_material_consumptions, start_time, end_time)
        
        print(f"\n5. Generating {num_quality_tests} Quality Tests...")
        self.generate_quality_tests(num_quality_tests, start_time, end_time)
        
        print(f"\n6. Generating {num_quality_events} Quality Events...")
        self.generate_quality_events(num_quality_events, start_time, end_time)
        
        print(f"\n7. Generating {num_maintenance_activities} Maintenance Activities...")
        self.generate_maintenance_activities(num_maintenance_activities, start_time, end_time)
        
        print(f"\n8. Generating {num_resource_utilization} Resource Utilization Records...")
        self.generate_resource_utilization(num_resource_utilization, start_time, end_time)
        
        print(f"\n9. Generating {num_performance_records} Production Performance Records...")
        self.generate_production_performance(num_performance_records, start_time, end_time)
        
        print("\nData generation complete!")
    
    def generate_work_orders(self, num_records=200, start_time=None, end_time=None):
        """
        Generate synthetic data for the WorkOrders table.
        
        Parameters:
        - num_records: Number of work order records to generate
        - start_time: Start time for work order dates
        - end_time: End time for work order dates
        
        Returns:
        - DataFrame containing the generated work orders data
        """
        if self.equipment_df is None:
            print("Error: No equipment data available.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=90)
        if end_time is None:
            end_time = datetime.now() + timedelta(days=30)
        
        # Get facility IDs if available, otherwise generate synthetic ones
        if not self.facility_ids and self.facilities_df is not None:
            self.facility_ids = self.facilities_df['facility_id'].unique().tolist()
        if not self.facility_ids:
            self.facility_ids = [f"FAC-{uuid.uuid4().hex[:8].upper()}" for _ in range(5)]
            
        # Generate customer order IDs if not available
        customer_order_ids = [f"CO-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Generate production schedule IDs if not available
        production_schedule_ids = [f"PS-{uuid.uuid4().hex[:8].upper()}" for _ in range(10)]
        
        # Define work order types and their probabilities
        work_order_types = {
            "Production": 0.6,        # Most common
            "Maintenance": 0.2,
            "Quality Check": 0.05,
            "Rework": 0.05,
            "Engineering": 0.05,
            "Cleaning": 0.03,
            "Calibration": 0.02
        }
        
        # Define work order statuses and their transition probabilities
        # Format: {status: {next_status: probability}}
        status_transitions = {
            "Planned": {"Planned": 0.2, "Released": 0.7, "Cancelled": 0.1},
            "Released": {"Released": 0.2, "In Progress": 0.75, "Cancelled": 0.05},
            "In Progress": {"In Progress": 0.3, "Completed": 0.6, "On Hold": 0.1},
            "On Hold": {"On Hold": 0.3, "In Progress": 0.6, "Cancelled": 0.1},
            "Completed": {"Completed": 0.95, "Rework": 0.05},
            "Cancelled": {"Cancelled": 1.0},  # Terminal state
            "Rework": {"Rework": 0.3, "In Progress": 0.7}
        }
        
        # Define priority levels
        priority_levels = [1, 2, 3, 4, 5]  # 1 = highest, 5 = lowest
        priority_weights = [0.1, 0.2, 0.4, 0.2, 0.1]  # Most orders are medium priority
        
        # Define possible units of measurement
        quantity_units = ["kg", "L", "units", "pallets", "boxes", "tons", "m", "m²", "m³", "batches"]
        
        # Generate work order data
        data = {
            "work_order_id": [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_records)],
            "work_order_type": [],
            "product_id": [],
            "planned_quantity": [],
            "actual_quantity": [],
            "quantity_unit": [],
            "planned_start_date": [],
            "actual_start_date": [],
            "planned_end_date": [],
            "actual_end_date": [],
            "status": [],
            "priority": [],
            "customer_order_id": [],
            "production_schedule_id": [],
            "facility_id": [],
            "planned_duration": []  # in days
        }
        
        # Generate a time distribution for work orders (weighted toward recent and near future)
        time_points = []
        time_range_days = (end_time - start_time).days
        
        for _ in range(num_records):
            # Use a beta distribution to weight toward recent and near future
            # Beta(2, 1) will weight toward the future, Beta(1, 2) toward the past
            if random.random() < 0.6:  # 60% of orders are more recent/future
                beta = random.betavariate(2, 1)
            else:
                beta = random.betavariate(1, 2)
            
            days_offset = int(beta * time_range_days)
            time_point = start_time + timedelta(days=days_offset)
            time_points.append(time_point)
        
        # Sort time points to establish a chronological sequence
        time_points.sort()
        
        # Generate data for each work order
        for i in range(num_records):
            # Select work order type (weighted random)
            work_order_type = random.choices(
                list(work_order_types.keys()), 
                weights=list(work_order_types.values())
            )[0]
            data["work_order_type"].append(work_order_type)
            
            # Assign product ID (if applicable)
            if work_order_type in ["Production", "Rework", "Quality Check"]:
                data["product_id"].append(random.choice(self.product_ids))
            else:
                data["product_id"].append("")  # No product for maintenance, cleaning, etc.
            
            # Generate quantity (if applicable)
            if work_order_type in ["Production", "Rework"]:
                # Production quantities vary by product type, but we'll use a general range
                planned_quantity = random.choice([10, 50, 100, 500, 1000, 5000]) * random.uniform(0.8, 1.2)
                planned_quantity = round(planned_quantity, 1)
                data["planned_quantity"].append(planned_quantity)
                
                # Set unit appropriate for the quantity
                quantity_unit = random.choice(quantity_units)
                data["quantity_unit"].append(quantity_unit)
                
                # Generate actual quantity based on status (to be filled in later)
                data["actual_quantity"].append(0)  # Placeholder
            else:
                data["planned_quantity"].append(0)
                data["quantity_unit"].append("")
                data["actual_quantity"].append(0)
            
            # Set base planned start date from the chronological sequence
            base_date = time_points[i]
            
            # For work orders in the past, determine status through a Markov chain simulation
            current_status = "Planned"  # All work orders start as planned
            current_date = base_date
            
            # Simulate status transitions based on time progression
            while current_date < datetime.now():
                # Get possible next statuses and their probabilities
                next_status_probs = status_transitions[current_status]
                next_statuses = list(next_status_probs.keys())
                next_probs = list(next_status_probs.values())
                
                # Select next status
                next_status = random.choices(next_statuses, weights=next_probs)[0]
                
                # If status changed, advance the date
                if next_status != current_status:
                    # Time in a status depends on the status itself
                    if current_status == "Planned":
                        days_in_status = random.randint(1, 5)  # 1-5 days in planning
                    elif current_status == "Released":
                        days_in_status = random.randint(1, 3)  # 1-3 days released before starting
                    elif current_status == "In Progress":
                        days_in_status = random.randint(3, 15)  # 3-15 days in production
                    elif current_status == "On Hold":
                        days_in_status = random.randint(2, 10)  # 2-10 days on hold
                    else:
                        days_in_status = random.randint(1, 5)  # Default
                    
                    current_date += timedelta(days=days_in_status)
                else:
                    # If status didn't change, just advance a small random amount
                    current_date += timedelta(days=random.randint(1, 3))
                
                current_status = next_status
                
                # Stop if we've reached a terminal status
                if current_status in ["Completed", "Cancelled"]:
                    break
            
            # Set the final status
            data["status"].append(current_status)
            
            # Set duration based on work order type
            if work_order_type == "Production":
                # Production orders typically take longer
                duration_days = random.randint(5, 20)
            elif work_order_type == "Maintenance":
                duration_days = random.randint(1, 5)
            elif work_order_type == "Cleaning":
                duration_days = random.randint(1, 2)
            elif work_order_type == "Calibration":
                duration_days = random.randint(1, 3)
            else:
                duration_days = random.randint(2, 10)
            
            data["planned_duration"].append(duration_days)
            
            # Set planned dates
            planned_start_date = base_date
            planned_end_date = planned_start_date + timedelta(days=duration_days)
            
            data["planned_start_date"].append(planned_start_date.strftime("%Y-%m-%d"))
            data["planned_end_date"].append(planned_end_date.strftime("%Y-%m-%d"))
            
            # Set actual dates based on status
            if current_status in ["In Progress", "On Hold", "Completed", "Rework"]:
                # Started but may not be finished
                # Add some variation to actual start date
                start_variation = random.randint(-2, 2)  # +/- 2 days from planned
                actual_start_date = planned_start_date + timedelta(days=start_variation)
                data["actual_start_date"].append(actual_start_date.strftime("%Y-%m-%d"))
                
                if current_status == "Completed":
                    # Finished - may be early, on time, or late
                    completion_variation = random.choices(
                        [-3, -2, -1, 0, 1, 2, 3, 5, 10],  # Days early(-) or late(+)
                        weights=[0.05, 0.1, 0.15, 0.3, 0.15, 0.1, 0.05, 0.05, 0.05]
                    )[0]
                    actual_end_date = planned_end_date + timedelta(days=completion_variation)
                    data["actual_end_date"].append(actual_end_date.strftime("%Y-%m-%d"))
                    
                    # Set actual quantity for completed orders
                    if data["planned_quantity"][i] > 0:
                        # Actual quantity is usually close to planned, but may vary
                        quantity_variation = random.uniform(0.9, 1.05)  # -10% to +5%
                        actual_quantity = data["planned_quantity"][i] * quantity_variation
                        data["actual_quantity"][i] = round(actual_quantity, 1)
                else:
                    # Not finished yet
                    data["actual_end_date"].append("")
            else:
                # Not started yet
                data["actual_start_date"].append("")
                data["actual_end_date"].append("")
            
            # Set priority (weighted random)
            data["priority"].append(random.choices(priority_levels, weights=priority_weights)[0])
            
            # Connect to customer order (production orders are more likely to be connected)
            if work_order_type == "Production" and random.random() < 0.8:
                data["customer_order_id"].append(random.choice(customer_order_ids))
            else:
                data["customer_order_id"].append("")
            
            # Connect to production schedule (production orders are more likely to be scheduled)
            if work_order_type == "Production" and random.random() < 0.9:
                data["production_schedule_id"].append(random.choice(production_schedule_ids))
            else:
                data["production_schedule_id"].append("")
            
            # Assign to facility
            data["facility_id"].append(random.choice(self.facility_ids))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "work_orders.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.work_orders_df = df
        self.work_order_ids = df["work_order_id"].tolist()
        
        print(f"Saved {num_records} work order records to {output_file}")
        
        return df
    
    def generate_material_lots(self, num_lots=300, start_time=None, end_time=None):
        """
        Generate synthetic data for the MaterialLots table.
        
        Parameters:
        - num_lots: Number of material lot records to generate
        - start_time: Start time for receipt dates
        - end_time: End time for receipt dates
        
        Returns:
        - DataFrame containing the generated material lots data
        """
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now()
        
        # Create storage location IDs if not available
        storage_location_ids = [f"LOC-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Define quality status options and their probabilities
        quality_statuses = {
            "Released": 0.7,          # Most lots are released
            "Under Quarantine": 0.1,
            "Rejected": 0.05,
            "Pending Test": 0.1,
            "Hold": 0.03,
            "Expired": 0.02
        }
        
        # Define possible units of measurement based on material type
        material_types = ["Raw Material", "Packaging", "Intermediate", "Bulk", "Active Ingredient", 
                         "Excipient", "Finished Good", "Component", "Additive", "Catalyst"]
        
        quantity_units = {
            "Raw Material": ["kg", "tons", "L", "m³", "drums"],
            "Packaging": ["units", "rolls", "boxes", "pallets", "sheets"],
            "Intermediate": ["kg", "L", "batches", "drums", "totes"],
            "Bulk": ["kg", "L", "m³", "tons", "batches"],
            "Active Ingredient": ["kg", "g", "mg", "L", "batches"],
            "Excipient": ["kg", "g", "L", "drums", "bags"],
            "Finished Good": ["units", "boxes", "pallets", "cases", "bottles"],
            "Component": ["units", "pieces", "sets", "packages", "boxes"],
            "Additive": ["kg", "g", "L", "drums", "bags"],
            "Catalyst": ["kg", "g", "L", "containers", "vials"]
        }
        
        # Create material type map
        material_type_map = {}
        for mat_id in self.material_ids:
            material_type_map[mat_id] = random.choice(material_types)
        
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
            "parent_lot_id": [],
            "shelf_life_days": [],
            "remaining_days": []  # Calculate remaining shelf life
        }
        
        # Track lots for potential parent-child relationships
        all_lots = data["lot_id"].copy()
        potential_parents = random.sample(all_lots, int(len(all_lots) * 0.2))  # 20% can be parents
        
        # Generate receipt dates distributed over the time range
        receipt_dates = []
        time_range_days = (end_time - start_time).days
        
        for _ in range(num_lots):
            # Use a beta distribution to weight toward more recent receipts
            if random.random() < 0.7:  # 70% of lots are more recent
                beta = random.betavariate(2, 1)
            else:
                beta = random.betavariate(1, 2)
            
            days_offset = int(beta * time_range_days)
            receipt_date = start_time + timedelta(days=days_offset)
            receipt_dates.append(receipt_date)
        
        # Sort receipt dates (older to newer)
        receipt_dates.sort()
        
        # Generate data for each material lot
        for i in range(num_lots):
            # Select material ID
            material_id = random.choice(self.material_ids)
            data["material_id"].append(material_id)
            
            # Get material type (for appropriate unit selection)
            material_type = material_type_map.get(material_id, "Raw Material")
            
            # Select quantity unit based on material type
            if material_type in quantity_units:
                unit = random.choice(quantity_units[material_type])
            else:
                unit = random.choice(["kg", "units", "L", "pallets", "pieces"])
            
            data["quantity_unit"].append(unit)
            
            # Generate lot quantity (based on unit)
            if unit in ["kg", "L"]:
                # Typically ordered in hundreds or thousands
                quantity = random.choice([100, 200, 500, 1000, 2000, 5000]) * random.uniform(0.8, 1.2)
            elif unit in ["g", "mg", "ml"]:
                # Small quantities for fine materials
                quantity = random.choice([100, 500, 1000, 5000, 10000]) * random.uniform(0.8, 1.2)
            elif unit in ["tons", "m³"]:
                # Bulk materials in smaller quantities
                quantity = random.choice([1, 2, 5, 10, 20, 50]) * random.uniform(0.8, 1.2)
            elif unit in ["units", "pieces", "bottles"]:
                # Discrete items often in multiples of packaging sizes
                quantity = random.choice([100, 500, 1000, 5000, 10000, 25000])
            elif unit in ["pallets", "cases", "boxes"]:
                # Packaged goods in smaller counts
                quantity = random.choice([5, 10, 20, 50, 100]) * random.uniform(0.8, 1.2)
            else:
                # Default quantity
                quantity = random.choice([10, 50, 100, 500, 1000]) * random.uniform(0.8, 1.2)
            
            data["lot_quantity"].append(round(quantity, 2))
            
            # Set receipt date from the generated distribution
            receipt_date = receipt_dates[i]
            data["receipt_date"].append(receipt_date.strftime("%Y-%m-%d"))
            
            # Creation date is typically shortly before receipt (manufacturing date at supplier)
            manufacturing_lead_time = random.randint(1, 30)  # 1-30 days lead time
            creation_date = receipt_date - timedelta(days=manufacturing_lead_time)
            data["creation_date"].append(creation_date.strftime("%Y-%m-%d"))
            
            # Set expiration date based on material type
            if material_type == "Raw Material":
                shelf_life_days = random.randint(365, 1825)  # 1-5 years
            elif material_type == "Active Ingredient":
                shelf_life_days = random.randint(180, 1095)  # 6 months to 3 years
            elif material_type in ["Intermediate", "Bulk"]:
                shelf_life_days = random.randint(90, 365)  # 3 months to 1 year
            elif material_type == "Finished Good":
                shelf_life_days = random.randint(180, 730)  # 6 months to 2 years
            elif material_type == "Packaging":
                shelf_life_days = random.randint(730, 3650)  # 2-10 years
            else:
                shelf_life_days = random.randint(365, 1095)  # 1-3 years
            
            data["shelf_life_days"].append(shelf_life_days)
            
            expiration_date = creation_date + timedelta(days=shelf_life_days)
            data["expiration_date"].append(expiration_date.strftime("%Y-%m-%d"))
            
            # Calculate remaining shelf life
            remaining_days = (expiration_date - datetime.now()).days
            data["remaining_days"].append(remaining_days)
            
            # Determine status (based on quantity remaining)
            if random.random() < 0.7:  # 70% are active inventory
                data["status"].append("Active")
            elif random.random() < 0.5:  # Half of the remainder are consumed
                data["status"].append("Consumed")
            else:  # The rest are reserved or in process
                data["status"].append(random.choice(["Reserved", "In Process"]))
            
            # Assign supplier (raw materials and packaging always have suppliers)
            if material_type in ["Raw Material", "Packaging", "Active Ingredient", "Excipient", "Component"]:
                data["supplier_id"].append(random.choice(self.supplier_ids))
                # Generate supplier's lot ID
                data["supplier_lot_id"].append(f"{random.choice(['L', 'B', 'S'])}{random.randint(10000, 99999)}")
            else:
                # Internal materials may not have external suppliers
                if random.random() < 0.3:  # 30% chance of having supplier even for internal materials
                    data["supplier_id"].append(random.choice(self.supplier_ids))
                    data["supplier_lot_id"].append(f"{random.choice(['L', 'B', 'S'])}{random.randint(10000, 99999)}")
                else:
                    data["supplier_id"].append("")
                    data["supplier_lot_id"].append("")
            
            # Assign storage location
            if data["status"][i] in ["Active", "Reserved"]:
                data["storage_location_id"].append(random.choice(storage_location_ids))
            else:
                # Consumed or in-process materials may not have a storage location
                data["storage_location_id"].append("")
            
            # Set quality status (weighted random)
            data["quality_status"].append(
                random.choices(list(quality_statuses.keys()), weights=list(quality_statuses.values()))[0]
            )
            
            # Generate cost per unit (based on material type)
            if material_type == "Active Ingredient":
                # Expensive materials
                cost = random.uniform(100, 5000)
            elif material_type in ["Raw Material", "Excipient", "Catalyst"]:
                # Moderate cost materials
                cost = random.uniform(5, 100)
            elif material_type in ["Packaging", "Component"]:
                # Lower cost materials
                cost = random.uniform(0.5, 10)
            elif material_type == "Finished Good":
                # Higher value products
                cost = random.uniform(20, 500)
            else:
                # Default cost range
                cost = random.uniform(1, 50)
            
            data["cost_per_unit"].append(round(cost, 2))
            
            # Determine parent lot (if any)
            # Intermediate, Bulk, and Finished Good materials are more likely to have parent lots
            if (material_type in ["Intermediate", "Bulk", "Finished Good"] and 
                data["lot_id"][i] not in potential_parents and 
                random.random() < 0.4):  # 40% chance for applicable materials
                
                # Find suitable parents (created before this lot)
                earlier_lots = [all_lots[j] for j in range(i) if receipt_dates[j] < receipt_date]
                if earlier_lots:
                    parent_id = random.choice(earlier_lots)
                    data["parent_lot_id"].append(parent_id)
                else:
                    data["parent_lot_id"].append("")
            else:
                data["parent_lot_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "material_lots.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.material_lots_df = df
        self.lot_ids = df["lot_id"].tolist()
        
        print(f"Saved {num_lots} material lot records to {output_file}")
        
        return df
    
    def generate_material_transactions(self, num_transactions=500, start_time=None, end_time=None):
        """
        Generate synthetic data for the MaterialTransactions table.
        
        Parameters:
        - num_transactions: Number of transaction records to generate
        - start_time: Start time for transaction dates
        - end_time: End time for transaction dates
        
        Returns:
        - DataFrame containing the generated material transactions data
        """
        if self.material_lots_df is None:
            print("Error: No material lots data available. Generate material lots first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now()
        
        # Get storage location IDs from material lots
        storage_locations = self.material_lots_df['storage_location_id'].unique()
        storage_locations = [loc for loc in storage_locations if pd.notna(loc) and loc != ""]
        
        if not storage_locations:
            storage_locations = [f"LOC-{uuid.uuid4().hex[:8].upper()}" for _ in range(5)]
        
        # Define transaction types and their probabilities
        transaction_types = {
            "Receipt": 0.25,          # Initial receipt from supplier
            "Issue": 0.3,             # Issue to production
            "Return": 0.05,           # Return from production
            "Transfer": 0.2,          # Move between locations
            "Adjustment": 0.1,        # Inventory adjustment
            "Consumption": 0.05,      # Material consumed
            "Scrapping": 0.05         # Disposal of material
        }
        
        # Define possible reasons for each transaction type
        transaction_reasons = {
            "Receipt": ["Initial Receipt", "Purchase Order", "Vendor Delivery", "Stock Replenishment", "Contract Manufacturing"],
            "Issue": ["Production Order", "Work Order", "Batch Production", "Line Replenishment", "Process Requirement"],
            "Return": ["Excess Material", "Quality Issue", "Process Change", "Order Cancellation", "Wrong Material"],
            "Transfer": ["Storage Optimization", "Staging for Production", "Quarantine", "Relocation", "Consolidation"],
            "Adjustment": ["Cycle Count", "Inventory Audit", "Quantity Correction", "System Reconciliation", "Physical Count"],
            "Consumption": ["Material Used", "Process Consumption", "Batch Completion", "Manufacturing Process", "Test Samples"],
            "Scrapping": ["Quality Rejection", "Expired Material", "Damaged Goods", "Contamination", "Obsolete Material"]
        }
        
        # Generate transaction data
        data = {
            "transaction_id": [f"TRAN-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_transactions)],
            "transaction_type": [],
            "lot_id": [],
            "timestamp": [],
            "quantity": [],
            "from_location_id": [],
            "to_location_id": [],
            "work_order_id": [],
            "batch_id": [],
            "operator_id": [],
            "transaction_reason": [],
            "reference_document": [],
            "month": []  # For statistics
        }
        
        # Generate operator IDs
        operator_ids = [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(10)]
        
        # Generate document reference patterns
        po_pattern = "PO-{}"
        wo_pattern = "WO-{}"
        gr_pattern = "GR-{}"
        adj_pattern = "ADJ-{}"
        
        # Generate transactions
        active_lots = self.material_lots_df[self.material_lots_df['status'].isin(['Active', 'Reserved', 'In Process'])]['lot_id'].tolist()
        consumed_lots = self.material_lots_df[self.material_lots_df['status'] == 'Consumed']['lot_id'].tolist()
        all_lots = self.material_lots_df['lot_id'].tolist()
        
        # Generate timestamps distributed over the time range
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        timestamps = []
        
        for _ in range(num_transactions):
            random_minutes = random.randint(0, time_range_minutes)
            timestamp = start_time + timedelta(minutes=random_minutes)
            timestamps.append(timestamp)
        
        # Sort timestamps (older to newer)
        timestamps.sort()
        
        # Now generate transaction data
        for i in range(num_transactions):
            # Determine transaction type (weighted random)
            transaction_type = random.choices(list(transaction_types.keys()), 
                                            weights=list(transaction_types.values()))[0]
            data["transaction_type"].append(transaction_type)
            
            # Set timestamp
            timestamp = timestamps[i]
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            data["month"].append(timestamp.strftime("%Y-%m"))
            
            # Select lot based on transaction type
            if transaction_type == "Receipt":
                # Use any lot (as if it's being received)
                lot_id = random.choice(all_lots)
            elif transaction_type in ["Issue", "Transfer", "Return", "Adjustment"]:
                # Use active lots
                if active_lots:
                    lot_id = random.choice(active_lots)
                else:
                    lot_id = random.choice(all_lots)
            elif transaction_type in ["Consumption", "Scrapping"]:
                # Prefer consumed lots for consistency, but can use any
                if consumed_lots and random.random() < 0.7:
                    lot_id = random.choice(consumed_lots)
                elif active_lots:
                    lot_id = random.choice(active_lots)
                else:
                    lot_id = random.choice(all_lots)
            else:
                lot_id = random.choice(all_lots)
            
            data["lot_id"].append(lot_id)
            
            # Get lot information
            lot_info = self.material_lots_df[self.material_lots_df['lot_id'] == lot_id].iloc[0]
            total_quantity = lot_info['lot_quantity']
            
            # Determine transaction quantity
            if transaction_type == "Receipt":
                # Receipt is typically the full quantity
                quantity = total_quantity
            elif transaction_type == "Issue":
                # Issue is typically a portion or all of the quantity
                quantity = total_quantity * random.uniform(0.1, 1.0)
            elif transaction_type == "Return":
                # Return is typically a smaller portion
                quantity = total_quantity * random.uniform(0.05, 0.3)
            elif transaction_type == "Transfer":
                # Transfer is typically the full quantity
                quantity = total_quantity
            elif transaction_type == "Adjustment":
                # Adjustment can be positive or negative
                if random.random() < 0.5:
                    # Positive adjustment
                    quantity = total_quantity * random.uniform(0.01, 0.1)
                else:
                    # Negative adjustment
                    quantity = -total_quantity * random.uniform(0.01, 0.1)
            elif transaction_type == "Consumption":
                # Consumption is typically a large portion or all
                quantity = total_quantity * random.uniform(0.5, 1.0)
            elif transaction_type == "Scrapping":
                # Scrapping can be a portion or all
                quantity = total_quantity * random.uniform(0.1, 1.0)
            
            data["quantity"].append(round(quantity, 2))
            
            # Set location information based on transaction type
            if transaction_type == "Receipt":
                # From supplier (blank) to storage
                data["from_location_id"].append("")
                data["to_location_id"].append(random.choice(storage_locations))
            elif transaction_type == "Issue":
                # From storage to production (can be blank)
                data["from_location_id"].append(lot_info['storage_location_id'] if pd.notna(lot_info['storage_location_id']) else random.choice(storage_locations))
                data["to_location_id"].append("")  # Issued to production, not a storage location
            elif transaction_type == "Return":
                # From production (blank) to storage
                data["from_location_id"].append("")
                data["to_location_id"].append(lot_info['storage_location_id'] if pd.notna(lot_info['storage_location_id']) else random.choice(storage_locations))
            elif transaction_type == "Transfer":
                # From one storage location to another
                from_loc = lot_info['storage_location_id'] if pd.notna(lot_info['storage_location_id']) else random.choice(storage_locations)
                # Ensure to_location is different from from_location
                available_to_locs = [loc for loc in storage_locations if loc != from_loc]
                to_loc = random.choice(available_to_locs) if available_to_locs else random.choice(storage_locations)
                data["from_location_id"].append(from_loc)
                data["to_location_id"].append(to_loc)
            elif transaction_type == "Adjustment":
                # Adjustment happens in the current location
                data["from_location_id"].append(lot_info['storage_location_id'] if pd.notna(lot_info['storage_location_id']) else random.choice(storage_locations))
                data["to_location_id"].append("")
            elif transaction_type in ["Consumption", "Scrapping"]:
                # From storage to nowhere (consumed/scrapped)
                data["from_location_id"].append(lot_info['storage_location_id'] if pd.notna(lot_info['storage_location_id']) else random.choice(storage_locations))
                data["to_location_id"].append("")
            
            # Associate with work order if applicable
            if transaction_type in ["Issue", "Consumption"] and self.work_order_ids and random.random() < 0.8:
                # 80% chance of having a work order for production-related transactions
                data["work_order_id"].append(random.choice(self.work_order_ids))
            elif transaction_type == "Return" and self.work_order_ids and random.random() < 0.6:
                # 60% chance of having a work order for returns
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Associate with batch if applicable
            if transaction_type in ["Issue", "Consumption", "Return"] and random.random() < 0.7:
                # 70% chance of having a batch for production-related transactions
                data["batch_id"].append(random.choice(self.batch_ids))
            else:
                data["batch_id"].append("")
            
            # Set operator
            data["operator_id"].append(random.choice(operator_ids))
            
            # Set transaction reason
            if transaction_type in transaction_reasons:
                reason = random.choice(transaction_reasons[transaction_type])
            else:
                reason = "Standard Transaction"
            
            data["transaction_reason"].append(reason)
            
            # Generate reference document
            if transaction_type == "Receipt":
                ref_doc = po_pattern.format(random.randint(10000, 99999))
            elif transaction_type in ["Issue", "Consumption"]:
                if data["work_order_id"][i]:
                    ref_doc = wo_pattern.format(data["work_order_id"][i].split('-')[-1])
                else:
                    ref_doc = wo_pattern.format(random.randint(10000, 99999))
            elif transaction_type == "Adjustment":
                ref_doc = adj_pattern.format(random.randint(10000, 99999))
            elif transaction_type == "Transfer":
                ref_doc = gr_pattern.format(random.randint(10000, 99999))
            else:
                ref_doc = f"DOC-{random.randint(10000, 99999)}"
            
            data["reference_document"].append(ref_doc)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "material_transactions.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.material_transactions_df = df
        
        print(f"Saved {num_transactions} material transaction records to {output_file}")
        
        return df
    
    def generate_material_consumptions(self, num_consumptions=400, start_time=None, end_time=None):
        """
        Generate synthetic data for the MaterialConsumption table.
        
        Parameters:
        - num_consumptions: Number of material consumption records to generate
        - start_time: Start time for consumption dates
        - end_time: End time for consumption dates
        
        Returns:
        - DataFrame containing the generated material consumption data
        """
        if self.material_lots_df is None:
            print("Error: No material lots data available. Generate material lots first.")
            return None
        
        # Ensure equipment_ids is populated
        if not self.equipment_ids and self.equipment_df is not None:
            self.equipment_ids = self.equipment_df['equipment_id'].unique().tolist()
        elif not self.equipment_ids:
            # Create synthetic equipment IDs if none are available
            print("Warning: No equipment IDs available. Generating synthetic equipment IDs.")
            self.equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        

        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now()
        
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
        operator_ids = [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        
        # Generate consumption data
        data = {
            "consumption_id": [f"CONS-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_consumptions)],
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
            "consumption_variance": [],
            "variance_pct": [],
            "month": []  # For statistics
        }
        
        # Generate timestamps distributed over the time range
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        timestamps = []
        
        for _ in range(num_consumptions):
            random_minutes = random.randint(0, time_range_minutes)
            timestamp = start_time + timedelta(minutes=random_minutes)
            timestamps.append(timestamp)
        
        # Sort timestamps (older to newer)
        timestamps.sort()
        
        # Generate data for each consumption record
        for i in range(num_consumptions):
            # Select a material lot to consume
            if len(consumable_lots) > 0:
                lot = consumable_lots.sample(1).iloc[0]
                data["lot_id"].append(lot['lot_id'])
                
                # Use the lot's unit
                unit = lot['quantity_unit']
                data["unit"].append(unit)
                
                # Maximum consumption is the lot quantity
                max_consumption = float(lot['lot_quantity'])
                
                # Typical consumption is a portion of the lot
                typical_consumption = max_consumption * random.uniform(0.05, 0.9)
            else:
                # Fallback if no lots are available
                data["lot_id"].append(f"LOT-{uuid.uuid4().hex[:8].upper()}")
                unit = random.choice(["kg", "L", "units", "g", "ml", "pieces"])
                data["unit"].append(unit)
                max_consumption = random.uniform(100, 5000)
                typical_consumption = max_consumption * random.uniform(0.05, 0.9)
            
            # Set timestamp
            timestamp = timestamps[i]
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            data["month"].append(timestamp.strftime("%Y-%m"))
            
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
            
            # Calculate variance
            variance = actual_consumption - planned_consumption
            data["consumption_variance"].append(round(variance, 2))
            
            # Calculate variance percentage
            if planned_consumption > 0:
                variance_pct = (variance / planned_consumption) * 100
            else:
                variance_pct = 0
            data["variance_pct"].append(round(variance_pct, 6))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "material_consumption.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.material_consumption_df = df
        
        print(f"Saved {num_consumptions} material consumption records to {output_file}")
        
        return df
    
    def generate_quality_tests(self, num_tests=500, start_time=None, end_time=None):
        """
        Generate synthetic data for the QualityTests table.
        
        Parameters:
        - num_tests: Number of quality test records to generate
        - start_time: Start time for test dates
        - end_time: End time for test dates
        
        Returns:
        - DataFrame containing the generated quality tests data
        """
        if self.material_lots_df is None:
            print("Error: No material lots data available. Generate material lots first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now()
        
        # Define test types and their probabilities
        test_types = {
            "Chemical Analysis": 0.2,
            "Physical Test": 0.15,
            "Visual Inspection": 0.15,
            "Microbiological": 0.1,
            "Stability Test": 0.05,
            "Dimensional Check": 0.1,
            "Functional Test": 0.1,
            "Identification Test": 0.05,
            "Impurity Test": 0.05,
            "Release Testing": 0.05
        }
        
        # Define test methods for each test type
        test_methods = {
            "Chemical Analysis": ["HPLC", "GC", "MS", "IR Spectroscopy", "UV-Vis Spectroscopy", "Titration", "pH Measurement", "Conductivity"],
            "Physical Test": ["Viscosity", "Density", "Particle Size", "Hardness", "Tensile Strength", "Melting Point", "Dissolution", "Friability"],
            "Visual Inspection": ["Appearance", "Color", "Clarity", "Foreign Particles", "Visible Defects", "Packaging Integrity"],
            "Microbiological": ["Total Plate Count", "Microbial Enumeration", "Sterility", "Endotoxin", "Bioburden", "Antimicrobial Effectiveness"],
            "Stability Test": ["Accelerated Stability", "Long-term Stability", "Photostability", "Temperature Cycling", "Stress Testing"],
            "Dimensional Check": ["Height", "Width", "Diameter", "Thickness", "Weight", "Volume", "Surface Area"],
            "Functional Test": ["Performance Test", "Operational Check", "Power Consumption", "Response Time", "Load Test", "Durability"],
            "Identification Test": ["IR Identity", "Chemical Identity", "Chromatographic Identity", "Spectral Comparison"],
            "Impurity Test": ["Related Substances", "Residual Solvents", "Heavy Metals", "Organic Impurities", "Inorganic Impurities"],
            "Release Testing": ["Final Product Test", "Certificate of Analysis", "Conformance Test", "Quality Control Release"]
        }
        
        # Define test parameters and their specifications for each test type
        test_parameters = {
            "Chemical Analysis": {
                "Assay Content": {"unit": "%", "target": 100.0, "range": 5.0},
                "pH": {"unit": "pH units", "target": 7.0, "range": 1.0},
                "Residual Solvent": {"unit": "ppm", "target": 0.0, "range": 1000.0, "upper_only": True},
                "Active Ingredient": {"unit": "mg/mL", "target": 10.0, "range": 1.0},
                "Conductivity": {"unit": "µS/cm", "target": 100.0, "range": 50.0}
            },
            "Physical Test": {
                "Viscosity": {"unit": "cP", "target": 1000.0, "range": 200.0},
                "Density": {"unit": "g/cm³", "target": 1.05, "range": 0.1},
                "Particle Size": {"unit": "µm", "target": 50.0, "range": 10.0},
                "Hardness": {"unit": "kP", "target": 12.0, "range": 3.0},
                "Dissolution": {"unit": "%", "target": 85.0, "range": 15.0, "lower_only": True}
            },
            "Visual Inspection": {
                "Appearance": {"unit": "score", "target": 5.0, "range": 1.0, "lower_only": True},
                "Color Conformity": {"unit": "score", "target": 5.0, "range": 1.0, "lower_only": True},
                "Visible Defects": {"unit": "count", "target": 0.0, "range": 3.0, "upper_only": True},
                "Label Quality": {"unit": "score", "target": 5.0, "range": 1.0, "lower_only": True}
            },
            "Microbiological": {
                "Total Aerobic Count": {"unit": "CFU/g", "target": 0.0, "range": 1000.0, "upper_only": True},
                "E. coli": {"unit": "CFU/g", "target": 0.0, "range": 0.0, "upper_only": True}, # Zero tolerance
                "Yeast & Mold": {"unit": "CFU/g", "target": 0.0, "range": 100.0, "upper_only": True},
                "Salmonella": {"unit": "presence", "target": 0.0, "range": 0.0, "upper_only": True} # Pass/fail
            },
            "Stability Test": {
                "Potency": {"unit": "%", "target": 100.0, "range": 10.0},
                "Degradation Products": {"unit": "%", "target": 0.0, "range": 2.0, "upper_only": True},
                "pH Change": {"unit": "pH units", "target": 0.0, "range": 1.0, "absolute": True}
            },
            "Dimensional Check": {
                "Length": {"unit": "mm", "target": 100.0, "range": 1.0},
                "Width": {"unit": "mm", "target": 50.0, "range": 0.5},
                "Height": {"unit": "mm", "target": 25.0, "range": 0.5},
                "Weight": {"unit": "g", "target": 500.0, "range": 15.0}
            },
            "Functional Test": {
                "Operation Time": {"unit": "seconds", "target": 60.0, "range": 10.0},
                "Power Output": {"unit": "W", "target": 1000.0, "range": 100.0},
                "Efficiency": {"unit": "%", "target": 95.0, "range": 5.0},
                "Response Time": {"unit": "ms", "target": 100.0, "range": 20.0}
            },
            "Identification Test": {
                "Identity": {"unit": "match", "target": 1.0, "range": 0.0, "lower_only": True}, # Pass/fail
                "Purity": {"unit": "%", "target": 99.0, "range": 1.0, "lower_only": True}
            },
            "Impurity Test": {
                "Individual Impurity": {"unit": "%", "target": 0.0, "range": 0.5, "upper_only": True},
                "Total Impurities": {"unit": "%", "target": 0.0, "range": 2.0, "upper_only": True},
                "Heavy Metals": {"unit": "ppm", "target": 0.0, "range": 10.0, "upper_only": True}
            },
            "Release Testing": {
                "Final Potency": {"unit": "%", "target": 100.0, "range": 5.0},
                "Final Impurities": {"unit": "%", "target": 0.0, "range": 2.0, "upper_only": True},
                "Uniformity": {"unit": "%RSD", "target": 0.0, "range": 5.0, "upper_only": True}
            }
        }
        
        # Generate test equipment IDs
        test_equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(10)]
        
        # Generate data structure
        data = {
            "test_id": [f"TEST-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_tests)],
            "test_type": [],
            "test_method": [],
            "sample_id": [],
            "product_id": [],
            "lot_id": [],
            "batch_id": [],
            "work_order_id": [],
            "timestamp": [],
            "parameter_name": [],
            "specification_target": [],
            "specification_lower_limit": [],
            "specification_upper_limit": [],
            "actual_value": [],
            "unit": [],
            "test_result": [],
            "test_equipment_id": [],
            "analyst_id": [],
            "retest_flag": [],
            "notes": [],
            "month": []  # For statistics
        }
        
        # Generate timestamps distributed over the time range
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        timestamps = []
        
        for _ in range(num_tests):
            random_minutes = random.randint(0, time_range_minutes)
            timestamp = start_time + timedelta(minutes=random_minutes)
            timestamps.append(timestamp)
        
        # Sort timestamps (older to newer)
        timestamps.sort()
        
        # Generate data for each test record
        for i in range(num_tests):
            # Select test type (weighted random)
            test_type = random.choices(list(test_types.keys()), weights=list(test_types.values()))[0]
            data["test_type"].append(test_type)
            
            # Select test method for this type
            test_method = random.choice(test_methods[test_type])
            data["test_method"].append(test_method)
            
            # Generate sample ID
            data["sample_id"].append(f"S{random.randint(100000, 999999)}")
            
            # Decide what's being tested: material lot, product, or both
            test_target = random.choice(["lot", "product", "both"])
            
            if test_target in ["lot", "both"]:
                # Test is for a material lot
                lot_id = random.choice(self.material_lots_df['lot_id'].tolist())
                data["lot_id"].append(lot_id)
            else:
                data["lot_id"].append("")
                    
            if test_target in ["product", "both"]:
                # Test is for a product
                data["product_id"].append(random.choice(self.product_ids))
            else:
                data["product_id"].append("")
            
            # Associate with batch and work order
            if random.random() < 0.7:  # 70% associated with batch
                data["batch_id"].append(random.choice(self.batch_ids))
            else:
                data["batch_id"].append("")
                
            if random.random() < 0.6:  # 60% associated with work order
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Set timestamp
            timestamp = timestamps[i]
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            data["month"].append(timestamp.strftime("%Y-%m"))
            
            # Select test parameter for this type
            parameter_name = random.choice(list(test_parameters[test_type].keys()))
            data["parameter_name"].append(parameter_name)
            
            # Get parameter specs
            param_specs = test_parameters[test_type][parameter_name]
            target_value = param_specs["target"]
            range_value = param_specs["range"]
            unit = param_specs["unit"]
            
            # Set specification limits
            upper_only = param_specs.get("upper_only", False)
            lower_only = param_specs.get("lower_only", False)
            absolute = param_specs.get("absolute", False)
            
            if upper_only:
                # Only upper limit (max allowed)
                lower_limit = ""
                upper_limit = target_value + range_value
            elif lower_only:
                # Only lower limit (min required)
                lower_limit = target_value - range_value
                upper_limit = ""
            else:
                # Both limits
                lower_limit = target_value - range_value
                upper_limit = target_value + range_value
            
            # Special handling for zero or near-zero targets
            if abs(target_value) < 0.001 and not absolute:
                lower_limit = 0.0
            
            data["specification_target"].append(target_value)
            data["specification_lower_limit"].append(lower_limit)
            data["specification_upper_limit"].append(upper_limit)
            data["unit"].append(unit)
            
            # Generate actual test value (normally distributed around target with occasional outliers)
            if random.random() < 0.05:  # 5% chance of outlier
                # Generate outlier value
                if upper_only:
                    # For upper-only specs, generate occasional high outliers
                    actual_value = target_value + (range_value * random.uniform(1.1, 2.0))
                elif lower_only:
                    # For lower-only specs, generate occasional low outliers
                    actual_value = target_value - (range_value * random.uniform(1.1, 2.0))
                else:
                    # For two-sided specs, generate outliers on either side
                    if random.random() < 0.5:
                        actual_value = target_value + (range_value * random.uniform(1.1, 1.5))
                    else:
                        actual_value = target_value - (range_value * random.uniform(1.1, 1.5))
            else:
                # Generate normal value (normally distributed around target)
                std_dev = range_value / 3.0  # 3-sigma rule: most values within spec
                actual_value = random.normalvariate(target_value, std_dev)
            
            # Handle special cases
            if unit == "presence" or unit == "match":
                # These are pass/fail tests, actual value should be 0 or 1
                if random.random() < 0.95:  # 95% pass rate
                    actual_value = 1 if unit == "match" else 0  # Match=1 is good, Presence=0 is good
                else:
                    actual_value = 0 if unit == "match" else 1
            
            # Round actual value based on the unit precision
            if "%" in unit:
                actual_value = round(actual_value, 1)  # One decimal for percentages
            elif unit in ["g/cm³", "pH units"]:
                actual_value = round(actual_value, 2)  # Two decimals for density, pH
            elif unit in ["µm", "mg/mL", "ppm"]:
                actual_value = round(actual_value, 1)  # One decimal for small measurements
            else:
                actual_value = round(actual_value, 2)  # Default precision
            
            data["actual_value"].append(actual_value)
            
            # Determine test result
            if upper_only and upper_limit != "":
                test_result = "Pass" if actual_value <= upper_limit else "Fail"
            elif lower_only and lower_limit != "":
                test_result = "Pass" if actual_value >= lower_limit else "Fail"
            elif upper_limit != "" and lower_limit != "":
                test_result = "Pass" if lower_limit <= actual_value <= upper_limit else "Fail"
            else:
                # Default for unusual cases
                test_result = "Pass" if random.random() < 0.95 else "Fail"
            
            data["test_result"].append(test_result)
            
            # Assign test equipment
            data["test_equipment_id"].append(random.choice(test_equipment_ids))
            
            # Assign analyst/inspector
            data["analyst_id"].append(random.choice(self.personnel_ids))
            
            # Set retest flag (more likely for failed tests)
            if test_result == "Fail":
                retest_flag = random.random() < 0.7  # 70% of failures get retested
            else:
                retest_flag = random.random() < 0.05  # 5% of passes get retested
            
            data["retest_flag"].append(retest_flag)
            
            # Generate notes (more detailed for failures)
            if test_result == "Fail":
                notes_options = [
                    f"Out of specification. Retest authorized.",
                    f"Value exceeds {parameter_name} limit. Investigation required.",
                    f"Failed {test_method} test. Checking calibration.",
                    f"OOS result confirmed on duplicate test.",
                    f"Deviation reported, sample under investigation."
                ]
            else:
                notes_options = [
                    f"Result within specification.",
                    f"Test completed successfully.",
                    f"Verified against standard.",
                    f"",  # Empty note for many passing tests
                    f""
                ]
            
            data["notes"].append(random.choice(notes_options))
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "quality_tests.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.quality_tests_df = df
        
        print(f"Saved {num_tests} quality test records to {output_file}")
        
        return df
    
    def generate_quality_events(self, num_events=100, start_time=None, end_time=None):
        """
        Generate synthetic data for the QualityEvents table.
        
        Parameters:
        - num_events: Number of quality event records to generate
        - start_time: Start time for event dates
        - end_time: End time for event dates
        
        Returns:
        - DataFrame containing the generated quality events data
        """
        if self.quality_tests_df is None:
            print("Error: No quality tests data available. Generate quality tests first.")
            return None
        
        # Get failed tests as potential sources for quality events
        failed_tests = self.quality_tests_df[self.quality_tests_df['test_result'] == "Fail"]
        
        if len(failed_tests) == 0:
            print("Warning: No failed quality tests found. Generating generic quality events.")
            test_based_events = False
        else:
            test_based_events = True
        
        # Define event types and their probabilities
        event_types = {
            "Deviation": 0.3,
            "Non-conformance": 0.25,
            "Quality Incident": 0.2,
            "OOS Result": 0.15,
            "Complaint": 0.1
        }
        
        # Define severity levels
        severity_levels = [1, 2, 3, 4, 5]  # 1 = minor, 5 = critical
        severity_weights = [0.3, 0.3, 0.2, 0.15, 0.05]  # Most events are lower severity
        
        # Define status options
        event_statuses = ["Open", "Under Investigation", "Corrective Action", "Closed", "Canceled"]
        
        # Root cause categories
        root_causes = [
            "Process Deviation", "Equipment Malfunction", "Human Error", "Material Quality", 
            "Environmental Factors", "Contamination", "Documentation Error", "Training Issue",
            "Supplier Quality", "Unknown"
        ]
        
        # Corrective action templates
        corrective_actions = [
            "Retrain Personnel", "Update Procedure", "Equipment Maintenance", "Material Replacement",
            "Process Modification", "Enhanced Monitoring", "Supplier Audit", "CAPA Implementation",
            "Preventive Maintenance", "Additional Testing"
        ]
        
        # Generate data structure
        data = {
            "event_id": [f"QE-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_events)],
            "event_type": [],
            "severity": [],
            "description": [],
            "detection_date": [],
            "status": [],
            "product_id": [],
            "lot_id": [],
            "batch_id": [],
            "equipment_id": [],
            "area_id": [],
            "detected_by": [],
            "assignee": [],
            "root_cause": [],
            "corrective_action": [],
            "closure_date": []
        }
        
        # Generate detection dates distributed over the time range
        if start_time is None:
            start_time = datetime.now() - timedelta(days=180)
        if end_time is None:
            end_time = datetime.now()
            
        time_range_days = (end_time - start_time).days
        detection_dates = []
        
        for _ in range(num_events):
            days_offset = random.randint(0, time_range_days)
            detection_date = start_time + timedelta(days=days_offset)
            detection_dates.append(detection_date)
        
        # Sort dates (older to newer)
        detection_dates.sort()
        
        # Generate data for each quality event
        for i in range(num_events):
            # Select event type (weighted random)
            event_type = random.choices(list(event_types.keys()), weights=list(event_types.values()))[0]
            data["event_type"].append(event_type)
            
            # Set severity (weighted random)
            severity = random.choices(severity_levels, weights=severity_weights)[0]
            data["severity"].append(severity)
            
            # Determine if event is based on failed test
            if test_based_events and random.random() < 0.7:  # 70% of events based on failed tests
                # Select a random failed test
                test = failed_tests.sample(1).iloc[0]
                
                # Use test information
                test_id = test['test_id']
                parameter = test['parameter_name']
                test_type = test['test_type']
                actual_value = test['actual_value']
                target_value = test['specification_target']
                unit = test['unit']
                
                # Create description based on test
                description = f"{event_type} for {parameter} in {test_type} test. Actual: {actual_value} {unit}, Target: {target_value} {unit}. Test ID: {test_id}"
                
                # Link to the same entities
                product_id = test['product_id']
                lot_id = test['lot_id']
                batch_id = test['batch_id']
                equipment_id = test['test_equipment_id']
                detected_by = test['analyst_id']
                
            else:
                # Generate generic quality event
                templates = [
                    f"{event_type}: {random.choice(['High', 'Low', 'Out of range', 'Unexpected'])} {random.choice(['viscosity', 'content', 'weight', 'appearance', 'dissolution'])} result",
                    f"{event_type}: {random.choice(['Foreign material', 'Contamination', 'Incorrect label', 'Missing component', 'Wrong color'])} detected",
                    f"{event_type}: {random.choice(['Process parameter', 'Equipment', 'Material', 'Documentation'])} {random.choice(['issue', 'failure', 'deviation', 'error'])}"
                ]
                description = random.choice(templates)
                
                # Random associations
                product_id = random.choice(self.quality_tests_df['product_id'].unique().tolist()) if random.random() < 0.7 else ""
                lot_id = random.choice(self.lot_ids) if self.lot_ids and random.random() < 0.8 else ""
                batch_id = random.choice(self.batch_ids) if self.batch_ids and random.random() < 0.7 else ""
                equipment_id = random.choice(self.equipment_ids) if random.random() < 0.6 else ""
                detected_by = random.choice(self.personnel_ids)
            
            data["description"].append(description)
            data["product_id"].append(product_id)
            data["lot_id"].append(lot_id)
            data["batch_id"].append(batch_id)
            data["equipment_id"].append(equipment_id)
            data["detected_by"].append(detected_by)
            
            # Generate detection date
            detection_date = detection_dates[i]
            data["detection_date"].append(detection_date.strftime("%Y-%m-%d"))
            
            # Assign process area
            data["area_id"].append(random.choice(self.area_ids) if self.area_ids and random.random() < 0.8 else "")
            
            # Assign different person as assignee
            available_assignees = [p for p in self.personnel_ids if p != detected_by]
            assignee = random.choice(available_assignees) if available_assignees else random.choice(self.personnel_ids)
            data["assignee"].append(assignee)
            
            # Determine status (time-dependent)
            days_since_detection = (datetime.now() - detection_date).days
            
            if days_since_detection < 7:
                # Recent events are typically still open
                status = random.choice(["Open", "Under Investigation"])
            elif days_since_detection < 30:
                # Medium-term events are in progress
                status = random.choice(["Under Investigation", "Corrective Action", "Open"])
            else:
                # Older events are likely closed
                status = random.choice(["Closed", "Closed", "Closed", "Corrective Action", "Canceled"])
            
            data["status"].append(status)
            
            # Set root cause and corrective action (only for investigated/closed events)
            if status in ["Corrective Action", "Closed"]:
                data["root_cause"].append(random.choice(root_causes))
                data["corrective_action"].append(random.choice(corrective_actions))
            else:
                data["root_cause"].append("")
                data["corrective_action"].append("")
            
            # Set closure date (only for closed events)
            if status == "Closed":
                # Closure date is after detection date
                min_closure_delay = 3  # Minimum 3 days to close
                max_closure_delay = min(90, days_since_detection)  # Up to 90 days or available time
                closure_days = random.randint(min_closure_delay, max(min_closure_delay, max_closure_delay))
                closure_date = detection_date + timedelta(days=closure_days)
                data["closure_date"].append(closure_date.strftime("%Y-%m-%d"))
            else:
                data["closure_date"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "quality_events.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.quality_events_df = df
        
        print(f"Saved {num_events} quality event records to {output_file}")
        
        return df
    
    def generate_maintenance_activities(self, num_activities=300, start_time=None, end_time=None):
        """
        Generate synthetic data for the MaintenanceActivities table.
        
        Parameters:
        - num_activities: Number of maintenance activity records to generate
        - start_time: Start time for activity dates
        - end_time: End time for activity dates
        
        Returns:
        - DataFrame containing the generated maintenance activities data
        """
        if self.equipment_df is None:
            print("Error: No equipment data available.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=365)
        if end_time is None:
            end_time = datetime.now() + timedelta(days=30)
        
        # Generate technician IDs
        technician_ids = [f"TECH-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Define maintenance activity types and their probabilities
        activity_types = {
            "Preventive": 0.4,       # Regular scheduled maintenance
            "Corrective": 0.3,       # Fix after failure
            "Predictive": 0.1,       # Based on condition monitoring
            "Inspection": 0.1,       # Regular checks
            "Calibration": 0.05,     # Calibrating instruments
            "Overhaul": 0.03,        # Major maintenance
            "Upgrade": 0.02          # Improving equipment
        }
        
        # Define priority levels
        priority_levels = [1, 2, 3, 4, 5]  # 1 = highest, 5 = lowest
        priority_weights = [0.1, 0.2, 0.4, 0.2, 0.1]  # Most activities are medium priority
        
        # Define maintenance activity durations (in hours) by type
        activity_durations = {
            "Preventive": (2, 8),       # 2-8 hours
            "Corrective": (1, 24),      # 1-24 hours
            "Predictive": (1, 4),       # 1-4 hours
            "Inspection": (0.5, 2),     # 30 min - 2 hours
            "Calibration": (1, 6),      # 1-6 hours
            "Overhaul": (8, 72),        # 8-72 hours
            "Upgrade": (4, 48)          # 4-48 hours
        }
        
        # Define activity statuses and their time-based probabilities
        activity_statuses = ["Planned", "Scheduled", "In Progress", "Completed", "Canceled"]
        
        # Define common descriptions by activity type
        activity_descriptions = {
            "Preventive": [
                "Routine maintenance per schedule",
                "Preventive maintenance as per manual",
                "Scheduled lubrication and inspection",
                "Regular service check",
                "Planned component replacement"
            ],
            "Corrective": [
                "Repair after failure",
                "Fix mechanical issue",
                "Replace worn component",
                "Repair electrical fault",
                "Emergency fix after breakdown"
            ],
            "Predictive": [
                "Maintenance based on vibration analysis",
                "Service based on oil analysis results",
                "Pre-emptive repair based on monitoring",
                "Condition-based maintenance",
                "Thermography-indicated maintenance"
            ],
            "Inspection": [
                "Safety inspection",
                "Regulatory compliance check",
                "Visual inspection of components",
                "Operational check",
                "Performance verification"
            ],
            "Calibration": [
                "Sensor calibration",
                "Instrument accuracy verification",
                "Scale calibration",
                "Control system tuning",
                "Measurement system adjustment"
            ],
            "Overhaul": [
                "Complete system teardown and rebuild",
                "Major component replacement",
                "Full mechanical overhaul",
                "Comprehensive service",
                "Complete system restoration"
            ],
            "Upgrade": [
                "Software update installation",
                "Hardware upgrade",
                "Performance enhancement modification",
                "Component upgrade installation",
                "Feature addition"
            ]
        }
        
        # Generate data structure
        data = {
            "activity_id": [f"MAINT-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_activities)],
            "activity_type": [],
            "equipment_id": [],
            "work_order_id": [],
            "planned_start_date": [],
            "actual_start_date": [],
            "planned_end_date": [],
            "actual_end_date": [],
            "status": [],
            "priority": [],
            "description": [],
            "technician_id": [],
            "downtime_required": [],
            "actual_downtime_minutes": [],
            "planned_duration_hours": [],
            "month": []  # For statistics
        }
        
        # Create date distribution for maintenance activities
        # More activities in recent past and near future, fewer in distant past/future
        date_weights = []
        time_range_days = (end_time - start_time).days
        
        for i in range(time_range_days):
            # Weight activities to be more common in recent times
            days_from_now = abs((start_time + timedelta(days=i) - datetime.now()).days)
            if days_from_now <= 30:
                # Recent past or near future (high density)
                weight = 1.0
            elif days_from_now <= 90:
                # Medium past/future (medium density)
                weight = 0.5
            else:
                # Distant past/future (low density)
                weight = 0.2
            date_weights.append(weight)
        
        # Normalize weights
        total_weight = sum(date_weights)
        date_weights = [w / total_weight for w in date_weights]
        
        # Generate data for each maintenance activity
        for i in range(num_activities):
            # Select activity type (weighted random)
            activity_type = random.choices(
                list(activity_types.keys()), 
                weights=list(activity_types.values())
            )[0]
            data["activity_type"].append(activity_type)
            
            # Select equipment ID (favor older equipment for more maintenance)
            if 'installation_date' in self.equipment_df.columns:
                # Convert to datetime if it's not already
                if not pd.api.types.is_datetime64_dtype(self.equipment_df['installation_date']):
                    self.equipment_df['installation_date'] = pd.to_datetime(self.equipment_df['installation_date'], errors='coerce')
                
                # Calculate equipment age
                current_date = datetime.now()
                self.equipment_df['age_days'] = (current_date - self.equipment_df['installation_date']).dt.days
                
                # Weight by age (older equipment needs more maintenance)
                weights = self.equipment_df['age_days'].fillna(365).values
                weights = weights / max(1, weights.sum())  # Normalize
                
                # Select equipment with probability proportional to age
                selected_idx = random.choices(range(len(self.equipment_df)), weights=weights)[0]
                equipment_id = self.equipment_df.iloc[selected_idx]['equipment_id']
            else:
                # If no installation date, select randomly
                equipment_id = random.choice(self.equipment_ids)
            
            data["equipment_id"].append(equipment_id)
            
            # Select work order ID
            if len(self.work_order_ids) > 0:
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append(f"WO-{uuid.uuid4().hex[:8].upper()}")
            
            # Generate planned start date
            day_idx = random.choices(range(time_range_days), weights=date_weights)[0]
            planned_start_date = start_time + timedelta(days=day_idx)
            
            # Add random hours to make times more realistic
            planned_start_date += timedelta(hours=random.randint(7, 16))  # Business hours
            
            data["planned_start_date"].append(planned_start_date.strftime("%Y-%m-%d %H:%M:%S"))
            data["month"].append(planned_start_date.strftime("%Y-%m"))
            
            # Get duration range for this activity type
            min_hours, max_hours = activity_durations[activity_type]
            
            # Generate planned duration
            planned_duration_hours = random.uniform(min_hours, max_hours)
            planned_end_date = planned_start_date + timedelta(hours=planned_duration_hours)
            
            data["planned_end_date"].append(planned_end_date.strftime("%Y-%m-%d %H:%M:%S"))
            data["planned_duration_hours"].append(round(planned_duration_hours, 6))
            
            # Determine status based on dates
            current_date = datetime.now()
            
            if planned_start_date > current_date:
                # Future activity
                if (planned_start_date - current_date).days < 7:
                    # Near future
                    status = random.choices(["Planned", "Scheduled"], weights=[0.3, 0.7])[0]
                else:
                    # More distant future
                    status = "Planned"
                
                # Future activities don't have actual dates yet
                data["actual_start_date"].append("")
                data["actual_end_date"].append("")
                data["actual_downtime_minutes"].append("")
                
            elif planned_start_date <= current_date and planned_end_date > current_date:
                # Current activity
                if random.random() < 0.8:  # 80% chance it started on time
                    status = "In Progress"
                    
                    # Activity started but not finished
                    actual_start_date = planned_start_date + timedelta(minutes=random.randint(-60, 60))
                    data["actual_start_date"].append(actual_start_date.strftime("%Y-%m-%d %H:%M:%S"))
                    data["actual_end_date"].append("")
                    
                    # Partial downtime so far
                    current_downtime = (current_date - actual_start_date).total_seconds() / 60
                    data["actual_downtime_minutes"].append(round(current_downtime))
                    
                else:
                    # Activity delayed
                    status = random.choices(["Planned", "Scheduled"], weights=[0.3, 0.7])[0]
                    data["actual_start_date"].append("")
                    data["actual_end_date"].append("")
                    data["actual_downtime_minutes"].append("")
                    
            else:
                # Past activity
                if random.random() < 0.9:  # 90% chance it was completed
                    status = "Completed"
                    
                    # Actual start date might vary from planned
                    start_variation_minutes = random.randint(-120, 120)  # +/- 2 hours
                    actual_start_date = planned_start_date + timedelta(minutes=start_variation_minutes)
                    
                    # Actual duration might vary from planned
                    duration_variation = random.normalvariate(1.0, 0.2)  # Mean 1.0, std dev 0.2
                    actual_duration_hours = max(0.1, planned_duration_hours * duration_variation)
                    actual_end_date = actual_start_date + timedelta(hours=actual_duration_hours)
                    
                    data["actual_start_date"].append(actual_start_date.strftime("%Y-%m-%d %H:%M:%S"))
                    data["actual_end_date"].append(actual_end_date.strftime("%Y-%m-%d %H:%M:%S"))
                    
                    # Calculate actual downtime
                    actual_downtime = actual_duration_hours * 60  # Convert to minutes
                    data["actual_downtime_minutes"].append(round(actual_downtime))
                    
                else:
                    # Activity was canceled
                    status = "Canceled"
                    data["actual_start_date"].append("")
                    data["actual_end_date"].append("")
                    data["actual_downtime_minutes"].append("")
            
            data["status"].append(status)
            
            # Set priority (weighted random)
            priority = random.choices(priority_levels, weights=priority_weights)[0]
            
            # For corrective maintenance, increase priority (more urgent)
            if activity_type == "Corrective" and priority > 2:
                priority -= 1
                
            data["priority"].append(priority)
            
            # Set description
            if activity_type in activity_descriptions:
                description = random.choice(activity_descriptions[activity_type])
            else:
                description = f"{activity_type} maintenance activity"
                
            data["description"].append(description)
            
            # Assign technician
            data["technician_id"].append(random.choice(technician_ids))
            
            # Determine if downtime is required
            # Certain activity types almost always require downtime
            if activity_type in ["Corrective", "Overhaul", "Upgrade"]:
                downtime_required = random.random() < 0.95  # 95% require downtime
            elif activity_type in ["Preventive", "Calibration"]:
                downtime_required = random.random() < 0.7  # 70% require downtime
            else:
                downtime_required = random.random() < 0.3  # 30% require downtime
                
            data["downtime_required"].append(downtime_required)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "maintenance_activities.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.maintenance_activities_df = df
        
        print(f"Saved {num_activities} maintenance activity records to {output_file}")
        
        return df
    
    def generate_resource_utilization(self, num_records=1000, start_time=None, end_time=None):
        """
        Generate synthetic data for the ResourceUtilization table.
        
        Parameters:
        - num_records: Number of resource utilization records to generate
        - start_time: Start time for utilization dates
        - end_time: End time for utilization dates
        
        Returns:
        - DataFrame containing a sample of the generated resource utilization data
        """
        if self.equipment_df is None:
            print("Error: No equipment data available.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()
        
        # Create resource IDs from available data
        resource_ids = []
        resource_types = []
        
        # Add equipment resources
        for _, equipment in self.equipment_df.iterrows():
            resource_ids.append(equipment['equipment_id'])
            resource_types.append("Equipment")
        
        # Add personnel resources
        for i in range(20):
            resource_ids.append(f"PERS-{uuid.uuid4().hex[:8].upper()}")
            resource_types.append("Personnel")
        
        # Add material resources if available
        if self.material_lots_df is not None:
            # Only include active materials
            active_materials = self.material_lots_df[self.material_lots_df['status'] == 'Active']
            if len(active_materials) > 0:
                for _, material in active_materials.iterrows():
                    resource_ids.append(material['lot_id'])
                    resource_types.append("Material")
        else:
            # Add synthetic materials
            for i in range(15):
                resource_ids.append(f"LOT-{uuid.uuid4().hex[:8].upper()}")
                resource_types.append("Material")
        
        # Add utility resources (synthetic)
        utility_types = ["Electricity", "Water", "Steam", "Compressed Air", "Cooling Water", "Natural Gas", "Nitrogen"]
        for utility in utility_types:
            resource_ids.append(f"UTIL-{utility.upper().replace(' ', '')}")
            resource_types.append("Utility")
        
        # Create a resource map for lookup
        resource_map = dict(zip(resource_ids, resource_types))
        
        # Define downtime reasons by resource type
        downtime_reasons = {
            "Equipment": [
                "Preventive Maintenance", "Breakdown", "Setup/Changeover", "Calibration", 
                "Cleaning", "Operator Break", "Material Shortage", "Quality Issue", 
                "Scheduled Maintenance", "Tool Change", "Software Update", "Power Outage"
            ],
            "Personnel": [
                "Break", "Training", "Meeting", "Shift Change", "Absence", 
                "Documentation", "Administrative Task", "Support Activity"
            ],
            "Material": [
                "Quality Hold", "Awaiting Test Results", "Inventory Count", 
                "Transfer in Progress", "Shortage", "Replenishment"
            ],
            "Utility": [
                "Maintenance", "Supply Interruption", "External Outage", 
                "Capacity Limit", "Pressure Drop", "Temperature Deviation"
            ]
        }
        
        # Define data structure
        data = {
            "timestamp": [],
            "resource_id": [],
            "resource_type": [],
            "order_id": [],
            "utilization_percentage": [],
            "planned_utilization": [],
            "actual_utilization": [],
            "availability_status": [],
            "downtime": [],
            "downtime_reason": [],
            "day": []  # For statistics
        }
        
        # Generate timestamp sequence
        time_interval_minutes = 60  # Hourly intervals
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        num_intervals = min(num_records, time_range_minutes // time_interval_minutes)
        
        timestamps = [
            start_time + timedelta(minutes=i * time_interval_minutes)
            for i in range(num_intervals)
        ]
        
        # Generate utilization data
        records_generated = 0
        
        # For each timestamp, generate utilization for a subset of resources
        for timestamp in timestamps:
            # Determine how many resources to include at this timestamp
            # (not all resources are tracked at every interval)
            num_resources = random.randint(10, min(50, len(resource_ids)))
            
            # Select random resources to track at this interval
            selected_resources = random.sample(resource_ids, num_resources)
            
            # For each selected resource, generate utilization data
            for resource_id in selected_resources:
                resource_type = resource_map[resource_id]
                
                # Determine if associated with a work order
                if random.random() < 0.7:  # 70% associated with work order
                    order_id = random.choice(self.work_order_ids)
                else:
                    order_id = ""
                
                # Generate utilization data based on resource type
                if resource_type == "Equipment":
                    # Equipment tends to have higher utilization
                    planned_utilization = random.uniform(60, 95)
                    
                    # Determine if equipment is down
                    if random.random() < 0.1:  # 10% chance of downtime
                        availability_status = "Down"
                        actual_utilization = 0.0
                        downtime = time_interval_minutes
                        downtime_reason = random.choice(downtime_reasons["Equipment"])
                    else:
                        # Variation from planned (normally distributed)
                        variation = random.normalvariate(0, 10)  # Mean 0, std dev 10 percentage points
                        actual_utilization = max(0, min(100, planned_utilization + variation))
                        
                        if actual_utilization < 5:
                            availability_status = "Idle"
                            downtime = time_interval_minutes
                            downtime_reason = "No Production Scheduled"
                        else:
                            availability_status = "Running"
                            downtime = 0
                            downtime_reason = ""
                
                elif resource_type == "Personnel":
                    # Personnel utilization tends to be more varied
                    planned_utilization = random.uniform(50, 90)
                    
                    # Determine if personnel is unavailable
                    if random.random() < 0.15:  # 15% chance of unavailability
                        availability_status = "Unavailable"
                        actual_utilization = 0.0
                        downtime = time_interval_minutes
                        downtime_reason = random.choice(downtime_reasons["Personnel"])
                    else:
                        # Variation from planned (more variable than equipment)
                        variation = random.normalvariate(0, 15)  # Mean 0, std dev 15 percentage points
                        actual_utilization = max(0, min(100, planned_utilization + variation))
                        
                        if actual_utilization < 10:
                            availability_status = "Available"
                            downtime = time_interval_minutes
                            downtime_reason = "Waiting for Assignment"
                        else:
                            availability_status = "Assigned"
                            downtime = 0
                            downtime_reason = ""
                
                elif resource_type == "Material":
                    # Material utilization is typically lower and spiky
                    planned_utilization = random.uniform(20, 60)
                    
                    # Determine if material is unavailable
                    if random.random() < 0.05:  # 5% chance of unavailability
                        availability_status = "On Hold"
                        actual_utilization = 0.0
                        downtime = time_interval_minutes
                        downtime_reason = random.choice(downtime_reasons["Material"])
                    else:
                        # Materials often have bursts of usage
                        if random.random() < 0.3:  # 30% chance of high usage
                            actual_utilization = random.uniform(70, 100)
                            availability_status = "In Use"
                            downtime = 0
                            downtime_reason = ""
                        else:
                            actual_utilization = random.uniform(0, planned_utilization)
                            if actual_utilization < 5:
                                availability_status = "Available"
                                downtime = time_interval_minutes
                                downtime_reason = "Not Required"
                            else:
                                availability_status = "In Use"
                                downtime = 0
                                downtime_reason = ""
                
                else:  # Utility
                    # Utilities typically have high availability but variable usage
                    planned_utilization = random.uniform(30, 70)
                    
                    # Determine if utility is unavailable
                    if random.random() < 0.03:  # 3% chance of outage
                        availability_status = "Outage"
                        actual_utilization = 0.0
                        downtime = time_interval_minutes
                        downtime_reason = random.choice(downtime_reasons["Utility"])
                    else:
                        # Utilities can have peak usage periods
                        hour_of_day = timestamp.hour
                        
                        # Higher usage during standard working hours
                        if 8 <= hour_of_day <= 17:
                            usage_factor = random.uniform(0.8, 1.2)
                        else:
                            usage_factor = random.uniform(0.5, 0.9)
                            
                        actual_utilization = min(100, planned_utilization * usage_factor)
                        availability_status = "Available"
                        downtime = 0
                        downtime_reason = ""
                
                # Calculate utilization percentage (actual vs. planned)
                if planned_utilization > 0:
                    utilization_percentage = (actual_utilization / planned_utilization) * 100
                else:
                    utilization_percentage = 0.0
                
                # Round values for cleaner data
                planned_utilization = round(planned_utilization, 1)
                actual_utilization = round(actual_utilization, 1)
                utilization_percentage = round(utilization_percentage, 1)
                
                # Add to data
                data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                data["resource_id"].append(resource_id)
                data["resource_type"].append(resource_type)
                data["order_id"].append(order_id)
                data["utilization_percentage"].append(utilization_percentage)
                data["planned_utilization"].append(planned_utilization)
                data["actual_utilization"].append(actual_utilization)
                data["availability_status"].append(availability_status)
                data["downtime"].append(downtime)
                data["downtime_reason"].append(downtime_reason)
                data["day"].append(timestamp.strftime("%Y-%m-%d"))  # For grouping by day
                
                records_generated += 1
                
                # If we've hit our target number of records, stop
                if records_generated >= num_records:
                    break
            
            # If we've hit our target number of records, stop
            if records_generated >= num_records:
                break
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "resource_utilization.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.resource_utilization_df = df
        
        print(f"Saved {records_generated} resource utilization records to {output_file}")
        
        return df
    
    def generate_production_performance(self, num_periods=1000, start_time=None, end_time=None):
        """
        Generate synthetic data for the ProductionPerformance table.
        
        Parameters:
        - num_periods: Number of performance records to generate
        - start_time: Start time for performance data
        - end_time: End time for performance data
        
        Returns:
        - DataFrame containing the generated production performance data
        """
        if self.equipment_df is None:
            print("Error: No equipment data available.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=90)
        if end_time is None:
            end_time = datetime.now()
        
        # Define time period 
        time_period = "Hour"
        period_minutes = 60  # 60 minutes per hour
        
        # Define equipment categories for different performance profiles
        # Extract equipment types if available, otherwise create synthetic categories
        if 'equipment_type' in self.equipment_df.columns:
            equipment_categories = self.equipment_df['equipment_type'].unique().tolist()
        else:
            equipment_categories = [
                "Production", "Packaging", "Assembly", "Processing", 
                "Machining", "Filling", "Testing", "Utility"
            ]
        
        # Create performance profiles by equipment category
        performance_profiles = {}
        
        for category in equipment_categories:
            # Base performance parameters
            if category in ["Production", "Processing"]:
                # Process equipment typically has high availability, variable performance
                profile = {
                    "availability": {"base": 0.92, "std": 0.05},
                    "performance": {"base": 0.85, "std": 0.08},
                    "quality": {"base": 0.98, "std": 0.02},
                    "cycle_time": {"base": 120, "std": 20}
                }
            elif category in ["Packaging", "Filling"]:
                # Packaging equipment has moderate availability, high performance
                profile = {
                    "availability": {"base": 0.88, "std": 0.07},
                    "performance": {"base": 0.9, "std": 0.05},
                    "quality": {"base": 0.99, "std": 0.01},
                    "cycle_time": {"base": 30, "std": 5}
                }
            elif category in ["Assembly", "Machining"]:
                # Assembly and machining have lower availability, high quality
                profile = {
                    "availability": {"base": 0.85, "std": 0.08},
                    "performance": {"base": 0.8, "std": 0.1},
                    "quality": {"base": 0.995, "std": 0.005},
                    "cycle_time": {"base": 180, "std": 30}
                }
            elif category == "Testing":
                # Testing equipment has high availability, consistent performance
                profile = {
                    "availability": {"base": 0.95, "std": 0.03},
                    "performance": {"base": 0.9, "std": 0.04},
                    "quality": {"base": 0.999, "std": 0.001},
                    "cycle_time": {"base": 60, "std": 10}
                }
            else:
                # Default profile
                profile = {
                    "availability": {"base": 0.9, "std": 0.06},
                    "performance": {"base": 0.85, "std": 0.07},
                    "quality": {"base": 0.98, "std": 0.02},
                    "cycle_time": {"base": 90, "std": 15}
                }
            
            performance_profiles[category] = profile
        
        # Default production rate by category (units per hour)
        production_rates = {
            "Production": {"base": 100, "std": 20},
            "Processing": {"base": 120, "std": 25},
            "Packaging": {"base": 500, "std": 50},
            "Filling": {"base": 600, "std": 60},
            "Assembly": {"base": 80, "std": 15},
            "Machining": {"base": 40, "std": 10},
            "Testing": {"base": 200, "std": 30},
            "Utility": {"base": 50, "std": 10}
        }
        
        # Calculate time points for performance records
        time_range_minutes = int((end_time - start_time).total_seconds() / 60)
        
        # Determine how many time periods we can fit in the range
        max_periods = time_range_minutes // period_minutes
        
        if max_periods < num_periods:
            print(f"Warning: Time range can only fit {max_periods} {time_period} periods.")
            print(f"Reducing requested number of periods from {num_periods} to {max_periods}.")
            num_periods = max_periods
        
        # Select random time points within the range
        period_starts = []
        
        # Create evenly spaced periods
        for i in range(num_periods):
            if i < max_periods:
                # For periods that fit within the range, space them evenly
                period_start = start_time + timedelta(minutes=i * period_minutes)
                period_starts.append(period_start)
            else:
                # If we need more periods than the range allows, start reusing time points
                # with different equipment
                period_start = period_starts[i % max_periods]
                period_starts.append(period_start)
        
        # Generate data structure
        data = {
            "performance_id": [f"PERF-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_periods)],
            "equipment_id": [],
            "work_order_id": [],
            "shift_id": [],
            "timestamp": [],
            "time_period": [],
            "availability_percent": [],
            "performance_percent": [],
            "quality_percent": [],
            "oee_percent": [],
            "production_count": [],
            "reject_count": [],
            "downtime_minutes": [],
            "cycle_time_seconds": [],
            "day": []  # For statistics
        }
        
        # Generate performance data for each period
        for i in range(num_periods):
            # Select equipment (with replacement)
            equipment = self.equipment_df.sample(1).iloc[0]
            equipment_id = equipment['equipment_id']
            
            # Get equipment category
            if 'equipment_type' in equipment.index:
                category = equipment['equipment_type']
            else:
                category = random.choice(equipment_categories)
            
            data["equipment_id"].append(equipment_id)
            
            # Assign work order (80% of records have work orders)
            if random.random() < 0.8:
                data["work_order_id"].append(random.choice(self.work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Assign shift ID based on time of day
            timestamp = period_starts[i]
            hour_of_day = timestamp.hour
            
            # Morning shift (6am-2pm), Afternoon shift (2pm-10pm), Night shift (10pm-6am)
            if 6 <= hour_of_day < 14:
                shift_id = self.shift_ids[0]  # Morning shift
            elif 14 <= hour_of_day < 22:
                shift_id = self.shift_ids[1]  # Afternoon shift
            else:
                shift_id = self.shift_ids[2]  # Night shift
            
            data["shift_id"].append(shift_id)
            
            # Set timestamp
            data["timestamp"].append(timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            data["day"].append(timestamp.strftime("%Y-%m-%d"))
            
            # Set time period
            data["time_period"].append(time_period)
            
            # Get performance profile for this equipment category
            if category in performance_profiles:
                profile = performance_profiles[category]
            else:
                # Use default profile if category not found
                profile = performance_profiles[equipment_categories[0]]
            
            # Generate performance metrics with some time correlation
            # This creates more realistic patterns where consecutive periods have similar performance
            
            # Determine if this is a "bad day" (10% chance)
            bad_day = random.random() < 0.1
            
            # Generate availability percentage
            if bad_day:
                # Lower availability on "bad days"
                availability = max(0, min(100, 100 * random.normalvariate(
                    profile["availability"]["base"] * 0.7,  # 30% reduction on bad days
                    profile["availability"]["std"]
                )))
            else:
                availability = max(0, min(100, 100 * random.normalvariate(
                    profile["availability"]["base"],
                    profile["availability"]["std"]
                )))
            
            # Generate performance percentage
            if bad_day:
                # Lower performance on "bad days"
                performance = max(0, min(100, 100 * random.normalvariate(
                    profile["performance"]["base"] * 0.8,  # 20% reduction on bad days
                    profile["performance"]["std"]
                )))
            else:
                performance = max(0, min(100, 100 * random.normalvariate(
                    profile["performance"]["base"],
                    profile["performance"]["std"]
                )))
            
            # Generate quality percentage
            if bad_day:
                # Lower quality on "bad days"
                quality = max(0, min(100, 100 * random.normalvariate(
                    profile["quality"]["base"] * 0.9,  # 10% reduction on bad days
                    profile["quality"]["std"] * 1.5  # More variability on bad days
                )))
            else:
                quality = max(0, min(100, 100 * random.normalvariate(
                    profile["quality"]["base"],
                    profile["quality"]["std"]
                )))
            
            # Calculate OEE (Overall Equipment Effectiveness)
            oee = (availability * performance * quality) / 10000  # Convert from percentage
            
            # Round metrics to 1 decimal place
            availability = round(availability, 1)
            performance = round(performance, 1)
            quality = round(quality, 1)
            oee = round(oee, 1)
            
            data["availability_percent"].append(availability)
            data["performance_percent"].append(performance)
            data["quality_percent"].append(quality)
            data["oee_percent"].append(oee)
            
            # Calculate production counts based on availability, performance, and time period
            if category in production_rates:
                base_rate = production_rates[category]["base"]
                rate_std = production_rates[category]["std"]
            else:
                base_rate = 100
                rate_std = 20
            
            # Adjust production rate based on performance
            rate_factor = performance / 100
            
            # Add some random variation
            production_rate = random.normalvariate(base_rate * rate_factor, rate_std * rate_factor)
            
            # Scale by time period (default is 1 hour)
            production_count = int(production_rate * (availability / 100))
            
            # Calculate rejects based on quality percentage
            reject_rate = 1 - (quality / 100)
            reject_count = int(production_count * reject_rate)
            
            # Adjust production count to be gross production (including rejects)
            production_count += reject_count
            
            data["production_count"].append(production_count)
            data["reject_count"].append(reject_count)
            
            # Calculate downtime based on availability
            downtime_minutes = period_minutes * (1 - availability / 100)
            data["downtime_minutes"].append(round(downtime_minutes))
            
            # Calculate cycle time
            base_cycle_time = profile["cycle_time"]["base"]
            cycle_std = profile["cycle_time"]["std"]
            
            # Adjust cycle time based on performance (lower performance = higher cycle time)
            cycle_factor = 100 / performance
            cycle_time = random.normalvariate(base_cycle_time * cycle_factor, cycle_std)
            
            # Ensure cycle time is positive
            cycle_time = max(1, round(cycle_time))
            
            data["cycle_time_seconds"].append(cycle_time)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "production_performance.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.production_performance_df = df
        
        print(f"Saved {len(df)} production performance records to {output_file}")
        
        return df


def main():
    """Main function to run the data generator"""
    parser = argparse.ArgumentParser(description='Generate ISA-95 Level 3 data')
    parser.add_argument('--output', type=str, default='data', 
                      help='Output directory for generated data (default: data)')
    parser.add_argument('--work-orders', type=int, default=200, 
                      help='Number of work order records to generate (default: 200)')
    parser.add_argument('--material-lots', type=int, default=300, 
                      help='Number of material lot records to generate (default: 300)')
    parser.add_argument('--material-transactions', type=int, default=500, 
                      help='Number of material transaction records to generate (default: 500)')
    parser.add_argument('--material-consumptions', type=int, default=400, 
                      help='Number of material consumption records to generate (default: 400)')
    parser.add_argument('--quality-tests', type=int, default=500, 
                      help='Number of quality test records to generate (default: 500)')
    parser.add_argument('--quality-events', type=int, default=100, 
                      help='Number of quality event records to generate (default: 100)')
    parser.add_argument('--resource-utilization', type=int, default=1000, 
                      help='Number of resource utilization records to generate (default: 1000)')
    parser.add_argument('--maintenance-activities', type=int, default=300, 
                      help='Number of maintenance activity records to generate (default: 300)')
    parser.add_argument('--performance-records', type=int, default=1000, 
                      help='Number of production performance records to generate (default: 1000)')
    parser.add_argument('--use-level2', action='store_true',
                      help='Use existing Level 2 data for consistency (default: False)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ISA95Level3DataGenerator(output_dir=args.output, level2_data_available=args.use_level2)
    
    # Start timer
    start_time = time.time()
    
    # Generate all data
    generator.generate_all_data(
        num_work_orders=args.work_orders,
        num_material_lots=args.material_lots,
        num_material_transactions=args.material_transactions,
        num_material_consumptions=args.material_consumptions,
        num_quality_tests=args.quality_tests,
        num_quality_events=args.quality_events,
        num_resource_utilization=args.resource_utilization,
        num_maintenance_activities=args.maintenance_activities,
        num_performance_records=args.performance_records
    )
    
    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\nTotal generation time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()