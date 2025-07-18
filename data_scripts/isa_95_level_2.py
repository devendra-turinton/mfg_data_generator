import pandas as pd
import numpy as np
import uuid
import os
import csv
from datetime import datetime, timedelta
import random
import time
import argparse

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)

class ISA95Level2DataGenerator:
    """
    Generator for ISA-95 Level 2 (Production Management) data.
    
    This class generates synthetic data for all tables in Level 2:
    - Equipment
    - EquipmentStates
    - Alarms
    - ProcessParameters
    - Recipes & BatchSteps
    - Batches & BatchExecution
    - Facilities & ProcessAreas
    """
    
    def __init__(self, output_dir="data", level1_data_available=False):
        """
        Initialize the data generator.
        
        Parameters:
        - output_dir: Directory where generated data will be saved
        - level1_data_available: Whether Level 1 data is available to reference
        """
        self.output_dir = output_dir
        self.level1_data_available = level1_data_available
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Track generated data for relationships
        self.equipment_df = None
        self.equipment_states_df = None
        self.alarms_df = None
        self.process_parameters_df = None
        self.recipes_df = None
        self.batch_steps_df = None
        self.batches_df = None
        self.batch_execution_df = None
        self.facilities_df = None
        self.process_areas_df = None
        
        # Define common reference data
        self.equipment_ids = []
        self.area_ids = []
        self.batch_ids = []
        self.recipe_ids = []
        self.work_order_ids = []
        self.product_ids = []
        self.operator_ids = []
        self.personnel_ids = []
        
        # Initialize reference data
        self._init_reference_data()
    
    def _init_reference_data(self):
        """Initialize reference data used across tables"""
        # Try to load Level 1 data for references if available
        self._load_level1_data()
        
        # Create equipment IDs if not loaded from Level 1
        if not self.equipment_ids:
            self.equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create process area IDs
        self.area_ids = [f"AREA-{uuid.uuid4().hex[:8].upper()}" for _ in range(10)]
        
        # Create batch IDs
        self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create recipe IDs
        self.recipe_ids = [f"RECIPE-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create work order IDs
        self.work_order_ids = [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create product IDs
        self.product_ids = [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create operator IDs
        self.operator_ids = [f"OP-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create personnel IDs (for authors, approvers, managers)
        self.personnel_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
    
    def _load_level1_data(self):
        """Load existing Level 1 data if available for reference"""
        if not self.level1_data_available:
            return
        
        try:
            # Try to load sensors data
            sensors_file = os.path.join(self.output_dir, "sensors.csv")
            if os.path.exists(sensors_file):
                sensors_df = pd.read_csv(sensors_file)
                if 'equipment_id' in sensors_df.columns:
                    self.equipment_ids.extend(sensors_df['equipment_id'].unique())
            
            # Try to load actuators data
            actuators_file = os.path.join(self.output_dir, "actuators.csv")
            if os.path.exists(actuators_file):
                actuators_df = pd.read_csv(actuators_file)
                if 'equipment_id' in actuators_df.columns:
                    self.equipment_ids.extend(actuators_df['equipment_id'].unique())
            
            # Try to load control loops data
            control_loops_file = os.path.join(self.output_dir, "control_loops.csv")
            if os.path.exists(control_loops_file):
                control_loops_df = pd.read_csv(control_loops_file)
                if 'equipment_id' in control_loops_df.columns:
                    self.equipment_ids.extend(control_loops_df['equipment_id'].unique())
            
            # Remove duplicates
            self.equipment_ids = list(set(self.equipment_ids))
            
            print(f"Loaded {len(self.equipment_ids)} equipment IDs from Level 1 data")
            
        except Exception as e:
            print(f"Warning: Could not load Level 1 data: {e}")
            self.equipment_ids = []
    
    def generate_all_data(self, num_equipment=150, num_areas=37, num_facilities=5, 
                         num_recipes=50, num_batches=100):
        """
        Generate data for all Level 2 tables.
        
        Parameters:
        - num_equipment: Number of equipment records to generate
        - num_areas: Number of process areas to generate
        - num_facilities: Number of facilities to generate
        - num_recipes: Number of recipes to generate
        - num_batches: Number of batches to generate
        """
        print("=== ISA-95 Level 2 Data Generation ===")
        
        # Define date ranges
        start_time = datetime.now() - timedelta(days=30)
        end_time = datetime.now()
        
        # Generate data for each table in a logical order to maintain relationships
        
        print(f"\n1. Generating {num_facilities} Facilities...")
        self.generate_facilities(num_facilities)
        
        print(f"\n2. Generating {num_areas} Process Areas...")
        self.generate_process_areas(num_areas)
        
        print(f"\n3. Generating {num_equipment} Equipment...")
        self.generate_equipment(num_equipment)
        
        print(f"\n4. Generating Equipment States...")
        self.generate_equipment_states(start_time, end_time)
        
        print(f"\n5. Generating Alarms...")
        self.generate_alarms(start_time, end_time)
        
        print(f"\n6. Generating {num_recipes} Recipes...")
        self.generate_recipes(num_recipes)
        
        print(f"\n7. Generating Batch Steps...")
        self.generate_batch_steps()
        
        print(f"\n8. Generating {num_batches} Batches...")
        self.generate_batches(num_batches, start_time, end_time)
        
        print(f"\n9. Generating Batch Execution data...")
        self.generate_batch_execution(start_time, end_time)
        
        print(f"\n10. Generating Process Parameters...")
        self.generate_process_parameters(start_time, end_time)
        
        print("\nData generation complete!")
    
    def generate_facilities(self, num_facilities=5):
        """
        Generate synthetic data for the Facilities table.
        
        Parameters:
        - num_facilities: Number of facility records to generate
        
        Returns:
        - DataFrame containing the generated facilities data
        """
        # Define possible values for categorical fields
        facility_types = ["Manufacturing Plant", "Warehouse", "Distribution Center", 
                        "R&D Center", "Processing Plant", "Assembly Plant", "Packaging Facility"]
        
        facility_statuses = ["Operational", "Under Maintenance", "Expanding", "Reduced Capacity", "Shutdown"]
        status_weights = [0.8, 0.1, 0.05, 0.03, 0.02]  # Mostly operational
        
        operating_hours = ["24/7", "Mon-Fri: 06:00-22:00", "Mon-Sat: 08:00-20:00", 
                          "Three Shifts: 06:00-14:00, 14:00-22:00, 22:00-06:00",
                          "Two Shifts: 07:00-19:00, 19:00-07:00"]
        
        # City locations (for address generation)
        cities = ["Chicago, IL", "Houston, TX", "Phoenix, AZ", "Philadelphia, PA", 
                 "San Antonio, TX", "San Diego, CA", "Dallas, TX", "San Jose, CA",
                 "Indianapolis, IN", "Jacksonville, FL", "Columbus, OH", "Charlotte, NC"]
        
        # Generate facility data
        data = {
            "facility_id": [f"FAC-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_facilities)],
            "facility_name": [],
            "facility_type": [],
            "address": [],
            "manager_id": [],  # Will be filled in later if personnel data is available
            "operating_hours": [],
            "status": [],
            "parent_facility_id": []
        }
        
        # Create a hierarchy with some facilities having parent facilities
        # First, identify which facilities will be parents (about 20%)
        potential_parents = []
        if num_facilities > 1:
            potential_parents = random.sample(data["facility_id"], max(1, int(num_facilities * 0.2)))
        
        # Generate facility names and other data
        for i in range(num_facilities):
            facility_id = data["facility_id"][i]
            
            # Generate facility type
            facility_type = random.choice(facility_types)
            data["facility_type"].append(facility_type)
            
            # Generate facility name (based on type and location)
            city = random.choice(cities)
            city_short = city.split(',')[0]  # Just the city name, not state
            
            name_formats = [
                f"{city_short} {facility_type}",
                f"{city_short} {facility_type} {i+1}",
                f"Plant {i+1} - {city_short}",
                f"{random.choice(['North', 'South', 'East', 'West'])} {city_short} {facility_type}",
                f"{city_short} Industrial {facility_type}"
            ]
            
            facility_name = random.choice(name_formats)
            data["facility_name"].append(facility_name)
            
            # Generate address
            street_number = random.randint(100, 9999)
            street_names = ["Main St", "Industrial Pkwy", "Commerce Dr", "Manufacturing Blvd", 
                            "Technology Dr", "Innovation Way", "Production Ave", "Enterprise Rd"]
            street_name = random.choice(street_names)
            address = f"{street_number} {street_name}, {city}"
            data["address"].append(address)
            
            # Operating hours
            data["operating_hours"].append(random.choice(operating_hours))
            
            # Status (weighted random)
            data["status"].append(random.choices(facility_statuses, weights=status_weights)[0])
            
            # Manager ID (will be filled in later)
            data["manager_id"].append("")
            
            # Determine parent facility (if any)
            # Top-level facilities have no parent
            if facility_id not in potential_parents and len(potential_parents) > 0 and random.random() < 0.3:
                # 30% chance of having a parent if not a potential parent itself
                parent_id = random.choice(potential_parents)
                data["parent_facility_id"].append(parent_id)
            else:
                data["parent_facility_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "facilities.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.facilities_df = df
        
        print(f"Successfully generated {len(df)} facility records.")
        print(f"Data saved to {output_file}")
        
        return df
    
    def generate_process_areas(self, num_areas=37):
        """
        Generate synthetic data for the ProcessAreas table.
        
        Parameters:
        - num_areas: Total number of process areas to generate
        
        Returns:
        - DataFrame containing the generated process areas data
        """
        if self.facilities_df is None or len(self.facilities_df) == 0:
            print("Error: No facilities data available. Generate facilities first.")
            return None
        
        # Define possible values for categorical fields
        area_types = [
            "Production", "Packaging", "Filling", "Mixing", "Reaction", "Distillation",
            "Fermentation", "Filtration", "Drying", "Granulation", "Tableting", "Assembly",
            "Testing", "Quality Control", "Warehousing", "Utilities", "Maintenance"
        ]
        
        environmental_classifications = [
            "General Manufacturing", "Clean Room Class 100,000", "Clean Room Class 10,000", 
            "Clean Room Class 1,000", "Clean Room Class 100", "Controlled Humidity",
            "Controlled Temperature", "Explosion Proof", "Corrosive Environment", 
            "High Temperature", "Cold Storage", "Sterile", "Aseptic", "Hazardous Material"
        ]
        
        # Use personnel IDs if available or generate new ones
        area_manager_ids = self.personnel_ids.copy() if self.personnel_ids else [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Now generate process areas for each facility
        data = {
            "area_id": [],
            "area_name": [],
            "facility_id": [],
            "area_type": [],
            "area_manager_id": [],
            "environmental_classification": [],
            "parent_area_id": []
        }
        
        # Track areas for potential parent-child relationships
        all_areas = []
        
        # Calculate areas per facility
        facilities = self.facilities_df['facility_id'].tolist()
        num_facilities = len(facilities)
        
        # Distribute areas among facilities
        areas_per_facility = {}
        remaining_areas = num_areas
        
        for i, facility_id in enumerate(facilities):
            if i == num_facilities - 1:
                # Last facility gets all remaining areas
                areas_per_facility[facility_id] = remaining_areas
            else:
                # Assign a proportion of areas to this facility
                facility_areas = max(1, int(num_areas * (1/num_facilities) * random.uniform(0.7, 1.3)))
                facility_areas = min(facility_areas, remaining_areas - (num_facilities - i - 1))
                areas_per_facility[facility_id] = facility_areas
                remaining_areas -= facility_areas
        
        # Process each facility
        for facility_id, num_facility_areas in areas_per_facility.items():
            # Generate area data for this facility
            facility_areas = []
            
            for i in range(num_facility_areas):
                area_id = f"AREA-{uuid.uuid4().hex[:8].upper()}"
                all_areas.append(area_id)
                facility_areas.append(area_id)
                
                data["area_id"].append(area_id)
                data["facility_id"].append(facility_id)
                
                # Determine area type (with some weighting toward production areas)
                if random.random() < 0.6:  # 60% chance of production-related area
                    area_type = random.choice([
                        "Production", "Packaging", "Filling", "Mixing", "Reaction", 
                        "Distillation", "Fermentation", "Filtration", "Drying", 
                        "Granulation", "Tableting", "Assembly"
                    ])
                else:
                    area_type = random.choice([
                        "Testing", "Quality Control", "Warehousing", "Utilities", "Maintenance"
                    ])
                
                data["area_type"].append(area_type)
                
                # Generate area name
                area_name = f"{area_type} Area {i+1}" if random.random() < 0.5 else f"{area_type} {i+1}"
                data["area_name"].append(area_name)
                
                # Assign area manager
                data["area_manager_id"].append(random.choice(area_manager_ids))
                
                # Determine environmental classification (based on area type)
                if area_type in ["Quality Control", "Testing", "Production", "Filling"]:
                    # More likely to have controlled environments
                    env_class = random.choice([
                        "Clean Room Class 100,000", "Clean Room Class 10,000", 
                        "Controlled Humidity", "Controlled Temperature"
                    ])
                elif area_type in ["Reaction", "Distillation", "Fermentation"]:
                    # More likely to have hazardous environments
                    env_class = random.choice([
                        "Explosion Proof", "Corrosive Environment", "High Temperature", 
                        "Hazardous Material"
                    ])
                elif area_type == "Packaging":
                    # Packaging areas often have controlled conditions
                    env_class = random.choice([
                        "Controlled Humidity", "Controlled Temperature", "General Manufacturing"
                    ])
                else:
                    # Other areas more likely to be general
                    env_class = random.choice([
                        "General Manufacturing", "Controlled Humidity", "Controlled Temperature"
                    ])
                
                data["environmental_classification"].append(env_class)
                
                # Initially no parent area
                data["parent_area_id"].append("")
            
            # Now create hierarchical relationships within this facility's areas
            # About 40% of areas will have a parent
            if len(facility_areas) > 1:
                potential_parents = random.sample(facility_areas, max(1, int(len(facility_areas) * 0.3)))
                
                for i, area_id in enumerate(facility_areas):
                    # Skip areas that are potential parents
                    if area_id in potential_parents:
                        continue
                    
                    # 40% chance of having a parent
                    if random.random() < 0.4:
                        parent_id = random.choice(potential_parents)
                        
                        # Update the parent_area_id (need to find the index in the main data list)
                        idx = data["area_id"].index(area_id)
                        data["parent_area_id"][idx] = parent_id
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "process_areas.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.process_areas_df = df
        
        # Update area_ids for future reference
        self.area_ids = df["area_id"].tolist()
        
        print(f"Successfully generated {len(df)} process area records.")
        print(f"Data saved to {output_file}")
        
        return df
    
    def generate_equipment(self, num_records=150):
        """
        Generate synthetic data for the Equipment table.
        
        Parameters:
        - num_records: Number of equipment records to generate
        
        Returns:
        - DataFrame containing the generated equipment data
        """
        # Define possible values for categorical fields
        equipment_types = [
            "Reactor", "Mixer", "Pump", "Compressor", "Heat Exchanger", "Distillation Column",
            "Filter", "Dryer", "Tank", "Furnace", "Conveyor", "Mill", "Centrifuge", "Evaporator",
            "Crystallizer", "Extruder", "Boiler", "Blender", "Separator", "Packaging Machine",
            "CNC Machine", "Robot", "Injection Molder", "Press", "Welder", "Assembly Station",
            "Filling Machine", "Labeling Machine", "Testing Station", "Sterilizer"
        ]
        
        # Create subtypes for more specific classification
        equipment_subtypes = {
            "Reactor": ["Batch Reactor", "Continuous Stirred Tank Reactor", "Plug Flow Reactor", "Fluidized Bed Reactor"],
            "Mixer": ["Ribbon Blender", "Paddle Mixer", "High Shear Mixer", "Tumble Mixer", "V-Blender"],
            "Pump": ["Centrifugal Pump", "Positive Displacement Pump", "Diaphragm Pump", "Peristaltic Pump"],
            "Compressor": ["Reciprocating Compressor", "Rotary Screw Compressor", "Centrifugal Compressor"],
            "Heat Exchanger": ["Shell and Tube", "Plate", "Spiral", "Air Cooled"],
            "Distillation Column": ["Batch Distillation", "Continuous Distillation", "Vacuum Distillation"],
            "Filter": ["Bag Filter", "Cartridge Filter", "Plate and Frame Filter", "Rotary Drum Filter"],
            "Dryer": ["Spray Dryer", "Rotary Dryer", "Fluidized Bed Dryer", "Vacuum Dryer"],
            "Tank": ["Storage Tank", "Mixing Tank", "Buffer Tank", "Pressure Vessel"],
            "Furnace": ["Electric Furnace", "Gas Furnace", "Induction Furnace"],
            "Conveyor": ["Belt Conveyor", "Roller Conveyor", "Screw Conveyor", "Pneumatic Conveyor"],
            "Mill": ["Ball Mill", "Hammer Mill", "Roller Mill", "Colloid Mill"],
            "Centrifuge": ["Disc Centrifuge", "Basket Centrifuge", "Decanter Centrifuge"],
            "Evaporator": ["Falling Film Evaporator", "Rising Film Evaporator", "Multiple Effect Evaporator"],
            "Crystallizer": ["Batch Crystallizer", "Continuous Crystallizer", "Cooling Crystallizer"],
            "Extruder": ["Single Screw Extruder", "Twin Screw Extruder", "Multi-Screw Extruder"],
            "Boiler": ["Fire Tube Boiler", "Water Tube Boiler", "Electric Boiler"],
            "Blender": ["V-Blender", "Double Cone Blender", "Ribbon Blender", "Paddle Blender"],
            "Separator": ["Cyclone Separator", "Magnetic Separator", "Electrostatic Separator"],
            "Packaging Machine": ["Form-Fill-Seal Machine", "Case Packer", "Wrapping Machine", "Cartoner"],
            "CNC Machine": ["CNC Mill", "CNC Lathe", "CNC Router", "CNC Plasma Cutter"],
            "Robot": ["Articulated Robot", "SCARA Robot", "Delta Robot", "Collaborative Robot"],
            "Injection Molder": ["Hydraulic Injection Molder", "Electric Injection Molder", "Hybrid Injection Molder"],
            "Press": ["Hydraulic Press", "Mechanical Press", "Pneumatic Press"],
            "Welder": ["Arc Welder", "MIG Welder", "TIG Welder", "Spot Welder"],
            "Assembly Station": ["Manual Assembly Station", "Semi-Automated Assembly Station", "Fully Automated Assembly Station"],
            "Filling Machine": ["Volumetric Filler", "Gravity Filler", "Piston Filler", "Vacuum Filler"],
            "Labeling Machine": ["Pressure Sensitive Labeler", "Cut and Stack Labeler", "Sleeve Labeler"],
            "Testing Station": ["Visual Inspection Station", "Dimensional Testing Station", "Functional Testing Station"],
            "Sterilizer": ["Steam Sterilizer", "Ethylene Oxide Sterilizer", "Radiation Sterilizer", "Chemical Sterilizer"]
        }
        
        manufacturers = [
            "Siemens", "ABB", "Emerson", "Honeywell", "Schneider Electric", "Yokogawa", 
            "Rockwell Automation", "GE", "Mitsubishi Electric", "Omron", "Endress+Hauser",
            "Festo", "SMC", "Bürkert", "Danfoss", "Alfa Laval", "Sulzer", "Andritz", 
            "Atlas Copco", "SPX Flow", "Grundfos", "ITT", "Flowserve", "KSB", "Weir",
            "Metso Outotec", "Thermo Fisher Scientific", "WIKA", "Eaton", "Phoenix Contact"
        ]
        
        statuses = ["Running", "Idle", "Maintenance", "Fault", "Standby", "Offline", "Startup", "Shutdown"]
        status_weights = [0.6, 0.2, 0.05, 0.05, 0.05, 0.02, 0.02, 0.01]  # Weights for each status
        
        # Use existing equipment IDs if available, otherwise generate new ones
        existing_equipment_ids = self.equipment_ids.copy() if self.equipment_ids else []
        
        if existing_equipment_ids and len(existing_equipment_ids) > 0:
            existing_equipment = list(existing_equipment_ids)
            # Calculate how many new equipment records to generate
            num_new_equipment = max(0, num_records - len(existing_equipment))
            print(f"Using {len(existing_equipment)} existing equipment IDs.")
            print(f"Generating {num_new_equipment} additional equipment records.")
        else:
            existing_equipment = []
            num_new_equipment = num_records
            print(f"No existing equipment IDs found. Generating {num_new_equipment} new equipment records.")
        
        # Generate new equipment IDs if needed
        new_equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_new_equipment)]
        
        # Combine existing and new equipment IDs
        all_equipment_ids = existing_equipment + new_equipment_ids
        
        # Ensure we don't exceed the requested number
        all_equipment_ids = all_equipment_ids[:num_records]
        
        # Update the master equipment_ids list
        self.equipment_ids = all_equipment_ids.copy()
        
        # Generate equipment data
        data = {
            "equipment_id": all_equipment_ids,
            "equipment_name": [],
            "equipment_type": [],
            "area_id": [],
            "manufacturer": [],
            "model_number": [],
            "serial_number": [],
            "installation_date": [],
            "last_maintenance_date": [],
            "next_maintenance_date": [],
            "equipment_status": [],
            "parent_equipment_id": []
        }
        
        # Create hierarchical structure with some equipment having parent equipment
        # First, identify which equipment will be parents (about 20%)
        potential_parents = random.sample(all_equipment_ids, int(len(all_equipment_ids) * 0.2))
        
        # Create a dictionary to track parent-child relationships
        equipment_hierarchy = {}
        
        # Now generate the details for each equipment
        for eq_id in all_equipment_ids:
            # Select equipment type
            eq_type = random.choice(equipment_types)
            data["equipment_type"].append(eq_type)
            
            # Select subtype if available
            if eq_type in equipment_subtypes:
                subtype = random.choice(equipment_subtypes[eq_type])
                # Create a descriptive name
                eq_name = f"{subtype} {random.randint(100, 999)}"
            else:
                eq_name = f"{eq_type} {random.randint(100, 999)}"
            
            data["equipment_name"].append(eq_name)
            
            # Assign to a process area
            if self.area_ids:
                data["area_id"].append(random.choice(self.area_ids))
            else:
                data["area_id"].append("")
            
            # Select manufacturer
            manufacturer = random.choice(manufacturers)
            data["manufacturer"].append(manufacturer)
            
            # Generate model number
            model_prefix = manufacturer[:3].upper()
            data["model_number"].append(f"{model_prefix}-{eq_type[:2].upper()}{random.randint(1000, 9999)}")
            
            # Generate serial number
            data["serial_number"].append(f"SN{random.randint(100000, 999999)}")
            
            # Generate installation date (1-10 years ago)
            install_date = datetime.now() - timedelta(days=random.randint(365, 3650))
            data["installation_date"].append(install_date.strftime("%Y-%m-%d"))
            
            # Generate last maintenance date (0-6 months ago)
            last_maint_date = datetime.now() - timedelta(days=random.randint(0, 180))
            data["last_maintenance_date"].append(last_maint_date.strftime("%Y-%m-%d"))
            
            # Generate next maintenance date (0-6 months in future)
            next_maint_date = datetime.now() + timedelta(days=random.randint(1, 180))
            data["next_maintenance_date"].append(next_maint_date.strftime("%Y-%m-%d"))
            
            # Generate status (weighted towards running and idle)
            data["equipment_status"].append(random.choices(statuses, weights=status_weights)[0])
            
            # Determine parent equipment (if any)
            # About 30% of equipment will have a parent
            if eq_id not in potential_parents and random.random() < 0.3:
                # Ensure we don't create circular references
                available_parents = [p for p in potential_parents if p != eq_id and p not in equipment_hierarchy.get(eq_id, [])]
                if available_parents:
                    parent_id = random.choice(available_parents)
                    data["parent_equipment_id"].append(parent_id)
                    
                    # Update hierarchy
                    if parent_id not in equipment_hierarchy:
                        equipment_hierarchy[parent_id] = []
                    equipment_hierarchy[parent_id].append(eq_id)
                else:
                    data["parent_equipment_id"].append("")
            else:
                data["parent_equipment_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "equipment.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.equipment_df = df
        
        print(f"Successfully generated {len(df)} equipment records.")
        print(f"Data saved to {output_file}")
        
        return df
    
    def generate_equipment_states(self, start_time=None, end_time=None, states_per_equipment=20):
        """
        Generate synthetic equipment state data based on the equipment table.
        
        Parameters:
        - start_time: Start time for state history (defaults to 7 days ago)
        - end_time: End time for state history (defaults to now)
        - states_per_equipment: Average number of state transitions per equipment
        
        Returns:
        - DataFrame containing a sample of the generated states data
        """
        if self.equipment_df is None or len(self.equipment_df) == 0:
            print("Error: No equipment data available. Generate equipment first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        # Create batch IDs and work order IDs to simulate relationships
        batch_ids = self.batch_ids.copy() if self.batch_ids else [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        work_order_ids = self.work_order_ids.copy() if self.work_order_ids else [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create operator IDs
        operator_ids = self.operator_ids.copy() if self.operator_ids else [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        
        # Define possible equipment states and transitions
        equipment_states = {
            # General states for most equipment
            "general": [
                "Running", "Idle", "Setup", "Cleaning", "Maintenance", "Fault", 
                "Shutdown", "Standby", "Startup", "Emergency Stop"
            ],
            # Specific states for reactors
            "Reactor": [
                "Running", "Idle", "Charging", "Reaction", "Discharging", "Cleaning", 
                "Heating", "Cooling", "Maintenance", "Fault", "Standby"
            ],
            # Specific states for pumps
            "Pump": [
                "Running", "Idle", "Startup", "Shutdown", "Maintenance", "Fault", 
                "Standby", "Cavitation", "Overheating"
            ],
            # Specific states for mixers
            "Mixer": [
                "Running", "Idle", "Loading", "Mixing", "Unloading", "Cleaning", 
                "Maintenance", "Fault", "Standby"
            ],
            # Specific states for tanks
            "Tank": [
                "Filling", "Holding", "Emptying", "Idle", "Cleaning", "Maintenance", 
                "Fault", "Agitating", "Heating", "Cooling"
            ],
            # Specific states for packaging machines
            "Packaging Machine": [
                "Running", "Idle", "Setup", "Loading Material", "Fault", "Maintenance", 
                "Standby", "Adjusting", "Cleaning", "Warming Up"
            ],
            # Specific states for CNC machines
            "CNC Machine": [
                "Running", "Idle", "Setup", "Tool Change", "Program Loading", "Maintenance", 
                "Fault", "Homing", "Material Loading", "Finished"
            ]
        }
        
        # Define state transition probabilities from current state to next state
        # Higher probability for common transitions, lower for rare ones
        transition_probabilities = {
            "Running": {"Running": 0.7, "Idle": 0.1, "Fault": 0.05, "Maintenance": 0.05, "Cleaning": 0.05, "Shutdown": 0.05},
            "Idle": {"Running": 0.5, "Idle": 0.2, "Setup": 0.1, "Maintenance": 0.1, "Shutdown": 0.1},
            "Setup": {"Running": 0.8, "Idle": 0.1, "Fault": 0.1},
            "Cleaning": {"Idle": 0.5, "Running": 0.3, "Maintenance": 0.2},
            "Maintenance": {"Idle": 0.6, "Running": 0.3, "Fault": 0.1},
            "Fault": {"Maintenance": 0.5, "Idle": 0.3, "Shutdown": 0.2},
            "Shutdown": {"Startup": 0.5, "Idle": 0.5},
            "Standby": {"Running": 0.6, "Idle": 0.3, "Shutdown": 0.1},
            "Startup": {"Running": 0.8, "Fault": 0.1, "Idle": 0.1},
            "Emergency Stop": {"Maintenance": 0.6, "Idle": 0.3, "Shutdown": 0.1},
            # Reactor specific
            "Charging": {"Reaction": 0.8, "Fault": 0.1, "Idle": 0.1},
            "Reaction": {"Discharging": 0.7, "Fault": 0.1, "Cooling": 0.2},
            "Discharging": {"Cleaning": 0.6, "Idle": 0.4},
            "Heating": {"Reaction": 0.7, "Fault": 0.1, "Idle": 0.2},
            "Cooling": {"Discharging": 0.7, "Idle": 0.3},
            # Pump specific
            "Cavitation": {"Fault": 0.6, "Maintenance": 0.4},
            "Overheating": {"Shutdown": 0.5, "Maintenance": 0.5},
            # Mixer specific
            "Loading": {"Mixing": 0.9, "Fault": 0.1},
            "Mixing": {"Unloading": 0.8, "Fault": 0.1, "Idle": 0.1},
            "Unloading": {"Cleaning": 0.5, "Idle": 0.5},
            # Tank specific
            "Filling": {"Holding": 0.8, "Agitating": 0.1, "Fault": 0.1},
            "Holding": {"Emptying": 0.6, "Agitating": 0.2, "Heating": 0.1, "Cooling": 0.1},
            "Emptying": {"Cleaning": 0.5, "Idle": 0.5},
            "Agitating": {"Holding": 0.5, "Emptying": 0.3, "Fault": 0.2},
            "Heating": {"Holding": 0.8, "Fault": 0.2},
            # Packaging machine specific
            "Loading Material": {"Running": 0.8, "Fault": 0.2},
            "Adjusting": {"Running": 0.9, "Fault": 0.1},
            "Warming Up": {"Running": 0.9, "Fault": 0.1},
            # CNC machine specific
            "Tool Change": {"Running": 0.9, "Fault": 0.1},
            "Program Loading": {"Running": 0.8, "Homing": 0.2},
            "Homing": {"Running": 0.9, "Fault": 0.1},
            "Material Loading": {"Running": 0.9, "Idle": 0.1},
            "Finished": {"Idle": 0.8, "Material Loading": 0.2}
        }
        
        # Define typical state durations in minutes
        state_durations = {
            "Running": (60, 480),        # 1-8 hours
            "Idle": (15, 120),           # 15 min - 2 hours
            "Setup": (20, 90),           # 20-90 min
            "Cleaning": (30, 120),       # 30 min - 2 hours
            "Maintenance": (60, 240),    # 1-4 hours
            "Fault": (15, 240),          # 15 min - 4 hours
            "Shutdown": (5, 30),         # 5-30 min
            "Standby": (10, 120),        # 10 min - 2 hours
            "Startup": (5, 30),          # 5-30 min
            "Emergency Stop": (5, 60),   # 5-60 min
            # Reactor specific
            "Charging": (15, 90),        # 15-90 min
            "Reaction": (60, 360),       # 1-6 hours
            "Discharging": (15, 60),     # 15-60 min
            "Heating": (15, 120),        # 15 min - 2 hours
            "Cooling": (30, 180),        # 30 min - 3 hours
            # Pump specific
            "Cavitation": (5, 30),       # 5-30 min
            "Overheating": (10, 45),     # 10-45 min
            # Mixer specific
            "Loading": (10, 45),         # 10-45 min
            "Mixing": (30, 240),         # 30 min - 4 hours
            "Unloading": (10, 45),       # 10-45 min
            # Tank specific
            "Filling": (20, 120),        # 20 min - 2 hours
            "Holding": (60, 720),        # 1-12 hours
            "Emptying": (20, 120),       # 20 min - 2 hours
            "Agitating": (30, 180),      # 30 min - 3 hours
            # Packaging machine specific
            "Loading Material": (5, 30), # 5-30 min
            "Adjusting": (10, 45),       # 10-45 min
            "Warming Up": (10, 30),      # 10-30 min
            # CNC machine specific
            "Tool Change": (2, 15),      # 2-15 min
            "Program Loading": (1, 5),   # 1-5 min
            "Homing": (1, 5),            # 1-5 min
            "Material Loading": (5, 20), # 5-20 min
            "Finished": (1, 10)          # 1-10 min
        }
        
        # Define transition reasons
        transition_reasons = {
            "Running": ["Production schedule", "Normal operation", "Process started", "Resumed after break"],
            "Idle": ["Production complete", "Waiting for materials", "Break time", "Shift change", "No orders"],
            "Setup": ["Product changeover", "New batch preparation", "Recipe change", "Tooling change"],
            "Cleaning": ["Scheduled cleaning", "Product changeover", "Contamination prevention", "End of batch"],
            "Maintenance": ["Scheduled maintenance", "Preventative service", "Component replacement", "Calibration"],
            "Fault": ["Error detected", "Component failure", "Safety interlock", "Process deviation", "Power issue"],
            "Shutdown": ["End of shift", "Planned downtime", "Weekend shutdown", "Holiday shutdown", "Energy saving"],
            "Standby": ["Waiting for upstream process", "Energy saving mode", "Temporary pause", "Break time"],
            "Startup": ["Beginning of shift", "Power restored", "After maintenance", "Morning startup"],
            "Emergency Stop": ["Safety button pressed", "Hazard detected", "Operator emergency", "Control system trigger"],
            # Additional specific reasons
            "Charging": ["Raw material loading", "Batch start", "Recipe preparation"],
            "Reaction": ["Process running", "Chemical reaction", "Temperature reached"],
            "Discharging": ["Batch complete", "Transfer to storage", "Moving to next stage"],
            "Heating": ["Temperature ramp-up", "Process requirement", "Preparation phase"],
            "Cooling": ["Temperature reduction", "Process complete", "Preparation for discharge"],
            "Cavitation": ["Insufficient inlet pressure", "Air in system", "Pump issue"],
            "Overheating": ["Excessive load", "Cooling failure", "Friction issue"],
            "Loading": ["Material addition", "Batch preparation", "Recipe start"],
            "Mixing": ["Blending process", "Homogenization", "Formula requirement"],
            "Unloading": ["Process complete", "Transfer operation", "Batch finished"],
            "Filling": ["Inventory replenishment", "Process start", "Batch preparation"],
            "Holding": ["Process requirement", "Waiting for test results", "Stabilization period"],
            "Emptying": ["Transfer to production", "Tank cleaning preparation", "Process complete"],
            "Agitating": ["Prevent settling", "Improve mixing", "Maintain suspension"],
            "Tool Change": ["Different operation", "Tool wear", "Program requirement"],
            "Program Loading": ["New part", "Production change", "Updated program"],
            "Homing": ["Reference position", "Startup procedure", "After emergency stop"],
            "Material Loading": ["New workpiece", "Batch start", "Production run"],
            "Finished": ["Operation complete", "Program end", "Batch finished"]
        }
        
        # Prepare the output file with CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "equipment_states.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'state_id', 'equipment_id', 'state_name', 'start_timestamp', 
                'end_timestamp', 'duration_seconds', 'previous_state', 
                'transition_reason', 'operator_id', 'batch_id', 'work_order_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            print(f"Generating equipment state history...")
            states_count = 0
            
            # Process each equipment
            for _, equipment in self.equipment_df.iterrows():
                equipment_id = equipment['equipment_id']
                equipment_type = equipment['equipment_type']
                
                # Get equipment status from equipment table
                current_status = equipment['equipment_status'] if 'equipment_status' in equipment.index else "Running"
                
                # Determine appropriate states for this equipment type
                if equipment_type in equipment_states:
                    possible_states = equipment_states[equipment_type]
                else:
                    possible_states = equipment_states["general"]
                
                # Vary the number of state transitions per equipment to make it more realistic
                # Some equipment changes state frequently, others rarely
                equipment_state_count = max(2, int(random.normalvariate(states_per_equipment, states_per_equipment/4)))
                
                # Initialize with a random state or use current status if it's a valid state
                if current_status in possible_states:
                    current_state = current_status
                else:
                    current_state = random.choice(possible_states)
                
                # Initialize time to the start time
                current_time = start_time
                
                # Generate state transitions for this equipment
                for i in range(equipment_state_count):
                    # Create a unique state ID
                    state_id = f"STATE-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Determine state duration based on typical ranges
                    if current_state in state_durations:
                        min_duration, max_duration = state_durations[current_state]
                    else:
                        min_duration, max_duration = 30, 120  # Default 30 min - 2 hours
                    
                    # Add some randomness to duration
                    duration_minutes = random.uniform(min_duration, max_duration)
                    
                    # Convert to seconds
                    duration_seconds = duration_minutes * 60
                    
                    # Calculate end time
                    state_end_time = current_time + timedelta(seconds=duration_seconds)
                    
                    # If we've exceeded the end time, truncate and finish
                    if state_end_time > end_time:
                        state_end_time = end_time
                        duration_seconds = (state_end_time - current_time).total_seconds()
                    
                    # Determine if this state is associated with a batch
                    has_batch = random.random() < 0.7  # 70% chance of having a batch
                    if has_batch:
                        batch_id = random.choice(batch_ids)
                        # If there's a batch, higher chance of having a work order
                        if random.random() < 0.8:  # 80% chance of having a work order if there's a batch
                            work_order_id = random.choice(work_order_ids)
                        else:
                            work_order_id = ""
                    else:
                        batch_id = ""
                        work_order_id = ""
                    
                    # Determine if an operator was involved
                    # Higher chance for manual states like Setup, Maintenance
                    if current_state in ["Setup", "Maintenance", "Cleaning", "Emergency Stop"]:
                        operator_chance = 0.9  # 90% chance
                    else:
                        operator_chance = 0.3  # 30% chance
                    
                    if random.random() < operator_chance:
                        operator_id = random.choice(operator_ids)
                    else:
                        operator_id = ""
                    
                    # Determine transition reason
                    if current_state in transition_reasons:
                        reason = random.choice(transition_reasons[current_state])
                    else:
                        reason = "Normal operation"
                    
                    # Write the state record to the CSV
                    writer.writerow({
                        'state_id': state_id,
                        'equipment_id': equipment_id,
                        'state_name': current_state,
                        'start_timestamp': current_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'end_timestamp': state_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                        'duration_seconds': int(duration_seconds),
                        'previous_state': "" if i == 0 else previous_state,
                        'transition_reason': reason,
                        'operator_id': operator_id,
                        'batch_id': batch_id,
                        'work_order_id': work_order_id
                    })
                    
                    states_count += 1
                    
                    # Remember the current state for the next record
                    previous_state = current_state
                    
                    # Move to the next time point
                    current_time = state_end_time
                    
                    # If we've reached the end time, stop generating states for this equipment
                    if current_time >= end_time:
                        break
                    
                    # Determine the next state based on transition probabilities
                    if current_state in transition_probabilities:
                        next_state_probs = transition_probabilities[current_state]
                        
                        # Filter to only include states valid for this equipment
                        valid_next_states = {s: p for s, p in next_state_probs.items() if s in possible_states}
                        
                        # If no valid transitions, pick a random valid state
                        if not valid_next_states:
                            current_state = random.choice(possible_states)
                        else:
                            # Normalize probabilities
                            total_prob = sum(valid_next_states.values())
                            normalized_probs = {s: p/total_prob for s, p in valid_next_states.items()}
                            
                            # Select next state based on probabilities
                            next_state = random.choices(
                                list(normalized_probs.keys()), 
                                weights=list(normalized_probs.values()),
                                k=1
                            )[0]
                            
                            current_state = next_state
                    else:
                        # If no defined transitions, pick a random state
                        current_state = random.choice(possible_states)
                
                # Display progress for every 10 equipment
                if states_count % 500 == 0:
                    print(f"  Generated {states_count} state records so far...")
        
        print(f"Successfully generated {states_count} equipment state records.")
        print(f"Data saved to {output_file}")
        
        # Read a sample for statistics and return
        sample_df = pd.read_csv(output_file, nrows=1000)
        self.equipment_states_df = sample_df  # Store a sample for reference
        
        return sample_df
    
    def generate_alarms(self, start_time=None, end_time=None, num_alarms_per_equipment=10):
        """
        Generate synthetic alarm data based on the equipment table.
        
        Parameters:
        - start_time: Start time for alarm history (defaults to 7 days ago)
        - end_time: End time for alarm history (defaults to now)
        - num_alarms_per_equipment: Average number of alarms per equipment
        
        Returns:
        - DataFrame containing a sample of the generated alarms data
        """
        if self.equipment_df is None or len(self.equipment_df) == 0:
            print("Error: No equipment data available. Generate equipment first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        # Try to extract time range from equipment states if available
        if self.equipment_states_df is not None and len(self.equipment_states_df) > 0:
            try:
                # Convert string timestamps to datetime
                self.equipment_states_df['start_timestamp'] = pd.to_datetime(self.equipment_states_df['start_timestamp'])
                self.equipment_states_df['end_timestamp'] = pd.to_datetime(self.equipment_states_df['end_timestamp'])
                
                # Get time range from equipment states
                states_start = self.equipment_states_df['start_timestamp'].min()
                states_end = self.equipment_states_df['end_timestamp'].max()
                
                # Use equipment states time range if available
                if not pd.isna(states_start) and not pd.isna(states_end):
                    start_time = states_start
                    end_time = states_end
                    print(f"Using time range from equipment states: {start_time} to {end_time}")
            except Exception as e:
                print(f"Warning: Could not extract time range from equipment states: {e}")
        
        # Create batch IDs and work order IDs to simulate relationships
        batch_ids = self.batch_ids.copy() if self.batch_ids else [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        work_order_ids = self.work_order_ids.copy() if self.work_order_ids else [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create operator IDs
        operator_ids = self.operator_ids.copy() if self.operator_ids else [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        
        # Define alarm types and details based on equipment types
        alarm_types = {
            # General alarms for most equipment
            "general": [
                "High Temperature", "Low Temperature", "High Pressure", "Low Pressure", 
                "High Flow", "Low Flow", "High Level", "Low Level", "Power Failure", 
                "Communication Error", "Sensor Fault", "Control Deviation", 
                "Emergency Stop", "Safety Interlock", "Maintenance Due"
            ],
            # Specific alarms for reactors
            "Reactor": [
                "High Temperature", "Low Temperature", "High Pressure", "Low Pressure", 
                "Agitator Failure", "Cooling System Failure", "Heating System Failure", 
                "Pressure Relief Valve", "Reaction Rate Deviation", "pH Deviation",
                "Reactor Integrity", "Exothermic Reaction", "Feed Rate Deviation"
            ],
            # Specific alarms for pumps
            "Pump": [
                "Cavitation", "High Vibration", "Low Flow", "No Flow", "Seal Failure", 
                "Motor Overload", "High Temperature", "Discharge Pressure High", 
                "Suction Pressure Low", "Efficiency Low", "Bearing Temperature High"
            ],
            # Specific alarms for mixers
            "Mixer": [
                "High Torque", "Low Torque", "High Temperature", "Vibration High", 
                "Motor Overload", "Shaft Misalignment", "Bearing Failure", 
                "Speed Deviation", "Mixer Imbalance", "Seal Failure"
            ],
            # Specific alarms for tanks
            "Tank": [
                "High Level", "Low Level", "High Temperature", "Low Temperature", 
                "High Pressure", "Leak Detected", "Agitator Failure", "Cooling System Failure", 
                "Heating System Failure", "Overflow Risk", "Empty Tank", "Contamination Risk"
            ],
            # Specific alarms for packaging machines
            "Packaging Machine": [
                "Material Jam", "Label Misalignment", "Out of Material", "Code Reader Failure", 
                "Seal Quality", "Film Break", "Conveyor Failure", "Temperature Deviation", 
                "Print Quality", "Package Count Error", "Machine Stop"
            ],
            # Specific alarms for CNC machines
            "CNC Machine": [
                "Tool Wear", "Axis Error", "Program Error", "Spindle Overload", 
                "Feed Rate Error", "Tool Change Error", "Coolant Low", "Material Error", 
                "Positioning Error", "Collision Risk", "Emergency Stop"
            ]
        }
        
        # Define priority levels and their distribution
        priority_levels = {
            1: "Critical",    # ~5%
            2: "High",        # ~15%
            3: "Medium",      # ~30%
            4: "Low",         # ~50%
        }
        
        priority_weights = [0.05, 0.15, 0.3, 0.5]
        
        # Define typical alarm durations (time to acknowledgment and resolution)
        # Format: (min_ack_minutes, max_ack_minutes, min_resolve_minutes, max_resolve_minutes)
        alarm_durations = {
            1: (1, 10, 10, 120),     # Critical: Quick ack (1-10min), resolve in 10min-2hrs
            2: (5, 30, 20, 240),     # High: Ack in 5-30min, resolve in 20min-4hrs
            3: (10, 120, 30, 480),   # Medium: Ack in 10min-2hrs, resolve in 30min-8hrs
            4: (30, 240, 60, 720)    # Low: Ack in 30min-4hrs, resolve in 1-12hrs
        }
        
        # Define alarm messages for each alarm type
        alarm_messages = {
            "High Temperature": [
                "Temperature exceeds safe operating limits", 
                "High temperature alarm - check cooling system",
                "Temperature above setpoint by {value}°C",
                "Overheating detected - verify cooling function"
            ],
            "Low Temperature": [
                "Temperature below minimum operating limits",
                "Low temperature alarm - check heating system",
                "Temperature below setpoint by {value}°C",
                "Insufficient heating detected - verify heaters"
            ],
            "High Pressure": [
                "Pressure exceeds maximum safe limit",
                "High pressure alarm - check relief valve",
                "Pressure above setpoint by {value} bar",
                "Excessive pressure detected - risk of damage"
            ],
            "Low Pressure": [
                "Pressure below minimum operating limit",
                "Low pressure alarm - check supply pressure",
                "Pressure below setpoint by {value} bar",
                "Insufficient pressure for operation"
            ],
            "High Flow": [
                "Flow rate exceeds maximum limit",
                "High flow alarm - check control valve",
                "Flow above setpoint by {value} m³/h",
                "Excessive flow detected - verify valve position"
            ],
            "Low Flow": [
                "Flow rate below minimum limit",
                "Low flow alarm - check for blockage",
                "Flow below setpoint by {value} m³/h",
                "Insufficient flow detected - verify pump operation"
            ],
            "High Level": [
                "Level exceeds maximum safe limit",
                "High level alarm - risk of overflow",
                "Level above setpoint by {value}%",
                "Excessive level detected - check outlet valve"
            ],
            "Low Level": [
                "Level below minimum operating limit",
                "Low level alarm - check supply",
                "Level below setpoint by {value}%",
                "Insufficient level detected - verify inlet flow"
            ],
            "Power Failure": [
                "Power supply interruption detected",
                "Power failure alarm - switching to backup",
                "Main power loss - check electrical supply",
                "Power quality issue detected"
            ],
            "Communication Error": [
                "Communication with control system lost",
                "Network communication failure",
                "Data transmission error - check connections",
                "Communication timeout - device not responding"
            ],
            "Sensor Fault": [
                "Sensor reading outside valid range",
                "Sensor calibration error detected",
                "Sensor failure - maintenance required",
                "Invalid sensor data - check wiring"
            ],
            "Control Deviation": [
                "Process variable deviating from setpoint",
                "Control loop unable to maintain setpoint",
                "PID control deviation exceeds {value}%",
                "Sustained control error detected"
            ],
            "Emergency Stop": [
                "Emergency stop button activated",
                "Emergency shutdown initiated",
                "E-stop circuit triggered - check safety devices",
                "Safety system activated emergency stop"
            ],
            "Safety Interlock": [
                "Safety interlock triggered - access violation",
                "Guard door opened during operation",
                "Safety circuit interrupted - check interlocks",
                "Safety barrier breach detected"
            ],
            "Maintenance Due": [
                "Scheduled maintenance overdue",
                "Service interval exceeded by {value} hours",
                "Maintenance required - performance degraded",
                "Preventative maintenance reminder"
            ]
        }
        
        # Define default messages for alarm types not specifically listed
        default_alarm_messages = [
            "{alarm_type} detected - check equipment",
            "{alarm_type} alarm activated",
            "{alarm_type} condition requires attention",
            "Alert: {alarm_type} on {equipment_name}"
        ]
        
        # Prepare the output file with CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "alarms.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'alarm_id', 'alarm_name', 'equipment_id', 'alarm_type', 'priority',
                'activation_timestamp', 'acknowledgment_timestamp', 'acknowledgment_operator_id',
                'resolution_timestamp', 'alarm_message', 'alarm_value', 'setpoint_value',
                'batch_id', 'work_order_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            print(f"Generating alarm history...")
            alarms_count = 0
            
            # Process each equipment
            for _, equipment in self.equipment_df.iterrows():
                equipment_id = equipment['equipment_id']
                equipment_type = equipment['equipment_type']
                equipment_name = equipment['equipment_name'] if 'equipment_name' in equipment.index else f"Equipment {equipment_id}"
                
                # Determine appropriate alarm types for this equipment type
                if equipment_type in alarm_types:
                    possible_alarms = alarm_types[equipment_type]
                else:
                    possible_alarms = alarm_types["general"]
                
                # Vary the number of alarms per equipment to make it more realistic
                # Some equipment has more alarms than others
                # Use a Poisson distribution around the average
                equipment_alarm_count = max(0, int(np.random.poisson(num_alarms_per_equipment)))
                
                # Generate alarms for this equipment
                for i in range(equipment_alarm_count):
                    # Create a unique alarm ID
                    alarm_id = f"ALARM-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Select alarm type
                    alarm_type = random.choice(possible_alarms)
                    
                    # Create alarm name
                    alarm_name = f"{equipment_name} - {alarm_type}"
                    
                    # Determine priority (weighted random)
                    priority = random.choices(list(priority_levels.keys()), weights=priority_weights)[0]
                    
                    # Generate activation timestamp
                    activation_time = start_time + (end_time - start_time) * random.random()
                    
                    # Determine acknowledgment and resolution times
                    min_ack, max_ack, min_resolve, max_resolve = alarm_durations[priority]
                    
                    ack_delay_minutes = random.uniform(min_ack, max_ack)
                    resolve_delay_minutes = random.uniform(min_resolve, max_resolve)
                    
                    # Some alarms may not be acknowledged or resolved yet
                    if random.random() < 0.05:  # 5% of alarms not acknowledged
                        acknowledgment_time = None
                        resolution_time = None
                        acknowledgment_operator_id = ""
                    else:
                        acknowledgment_time = activation_time + timedelta(minutes=ack_delay_minutes)
                        
                        # Check if acknowledgment time is beyond end time
                        if acknowledgment_time > end_time:
                            acknowledgment_time = None
                            resolution_time = None
                            acknowledgment_operator_id = ""
                        else:
                            # Assign operator who acknowledged
                            acknowledgment_operator_id = random.choice(operator_ids)
                            
                            # Determine resolution time
                            if random.random() < 0.1:  # 10% of acknowledged alarms not resolved
                                resolution_time = None
                            else:
                                resolution_time = acknowledgment_time + timedelta(minutes=resolve_delay_minutes)
                                
                                # Check if resolution time is beyond end time
                                if resolution_time > end_time:
                                    resolution_time = None
                    
                    # Generate alarm values
                    if alarm_type in ["High Temperature", "Low Temperature"]:
                        setpoint = random.uniform(50, 150)
                        deviation = random.uniform(5, 30) * (1 if "High" in alarm_type else -1)
                        alarm_value = setpoint + deviation
                    elif alarm_type in ["High Pressure", "Low Pressure"]:
                        setpoint = random.uniform(2, 10)
                        deviation = random.uniform(0.5, 3) * (1 if "High" in alarm_type else -1)
                        alarm_value = setpoint + deviation
                    elif alarm_type in ["High Flow", "Low Flow"]:
                        setpoint = random.uniform(10, 100)
                        deviation = random.uniform(5, 30) * (1 if "High" in alarm_type else -1)
                        alarm_value = max(0, setpoint + deviation)
                    elif alarm_type in ["High Level", "Low Level"]:
                        setpoint = random.uniform(40, 80)
                        deviation = random.uniform(10, 40) * (1 if "High" in alarm_type else -1)
                        alarm_value = max(0, min(100, setpoint + deviation))
                    elif alarm_type == "Control Deviation":
                        setpoint = random.uniform(50, 150)
                        deviation = random.uniform(10, 50)
                        alarm_value = setpoint + deviation
                    else:
                        # For other alarm types, no specific value
                        alarm_value = ""
                        setpoint = ""
                    
                    # Generate alarm message
                    if alarm_type in alarm_messages:
                        message_template = random.choice(alarm_messages[alarm_type])
                        # Replace {value} with the deviation if present
                        if "{value}" in message_template and alarm_value != "" and setpoint != "":
                            try:
                                deviation_val = abs(float(alarm_value) - float(setpoint))
                                message = message_template.replace("{value}", f"{deviation_val:.1f}")
                            except (ValueError, TypeError):
                                message = message_template.replace("{value}", "significant")
                        else:
                            message = message_template
                    else:
                        message_template = random.choice(default_alarm_messages)
                        message = message_template.replace("{alarm_type}", alarm_type).replace("{equipment_name}", equipment_name)
                    
                    # Determine if this alarm is associated with a batch
                    has_batch = random.random() < 0.7  # 70% chance of having a batch
                    if has_batch:
                        batch_id = random.choice(batch_ids)
                        # If there's a batch, higher chance of having a work order
                        if random.random() < 0.8:  # 80% chance of having a work order if there's a batch
                            work_order_id = random.choice(work_order_ids)
                        else:
                            work_order_id = ""
                    else:
                        batch_id = ""
                        work_order_id = ""
                    
                    # Format timestamps as strings (None remains None)
                    activation_timestamp = activation_time.strftime("%Y-%m-%d %H:%M:%S") if activation_time else None
                    acknowledgment_timestamp = acknowledgment_time.strftime("%Y-%m-%d %H:%M:%S") if acknowledgment_time else None
                    resolution_timestamp = resolution_time.strftime("%Y-%m-%d %H:%M:%S") if resolution_time else None
                    
                    # Write the alarm record to the CSV
                    writer.writerow({
                        'alarm_id': alarm_id,
                        'alarm_name': alarm_name,
                        'equipment_id': equipment_id,
                        'alarm_type': alarm_type,
                        'priority': priority,
                        'activation_timestamp': activation_timestamp,
                        'acknowledgment_timestamp': acknowledgment_timestamp,
                        'acknowledgment_operator_id': acknowledgment_operator_id,
                        'resolution_timestamp': resolution_timestamp,
                        'alarm_message': message,
                        'alarm_value': alarm_value,
                        'setpoint_value': setpoint,
                        'batch_id': batch_id,
                        'work_order_id': work_order_id
                    })
                    
                    alarms_count += 1
                    if alarms_count % 1000 == 0:
                        print(f"  Generated {alarms_count:,} alarms so far...")

    def generate_process_parameters(self, start_time=None, end_time=None, num_parameters_per_equipment=5, samples_per_parameter=100):
        """
        Generate synthetic process parameters data based on the equipment table.
        
        Parameters:
        - start_time: Start time for parameter history (defaults to 7 days ago)
        - end_time: End time for parameter history (defaults to now)
        - num_parameters_per_equipment: Average number of parameters per equipment
        - samples_per_parameter: Number of time-series samples per parameter
        
        Returns:
        - DataFrame containing a sample of the generated process parameters data
        """
        if self.equipment_df is None or len(self.equipment_df) == 0:
            print("Error: No equipment data available. Generate equipment first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        # Define parameter types based on equipment types
        parameter_types = {
            # General parameters for most equipment
            "general": [
                {"name": "Temperature", "unit": "°C", "range": (20, 150), "precision": 1},
                {"name": "Pressure", "unit": "bar", "range": (1, 15), "precision": 2},
                {"name": "Flow Rate", "unit": "m³/h", "range": (0, 100), "precision": 1},
                {"name": "Level", "unit": "%", "range": (0, 100), "precision": 1},
                {"name": "Speed", "unit": "rpm", "range": (0, 3000), "precision": 0},
                {"name": "Power", "unit": "kW", "range": (0, 500), "precision": 1},
                {"name": "Vibration", "unit": "mm/s", "range": (0, 15), "precision": 2},
                {"name": "Current", "unit": "A", "range": (0, 100), "precision": 1},
                {"name": "Voltage", "unit": "V", "range": (0, 480), "precision": 0},
                {"name": "Efficiency", "unit": "%", "range": (50, 100), "precision": 1}
            ],
            # Add equipment-specific parameter types similar to the notebook
            # (Include Reactor, Pump, Heat Exchanger, Tank, Dryer parameters)
        }
        
        # Add the parameter type definitions for specific equipment from the notebook
        
        # Define control modes
        control_modes = ["Auto", "Manual", "Cascade", "Supervised", "Off"]
        control_mode_weights = [0.7, 0.15, 0.1, 0.04, 0.01]
        
        # Get recipe and batch IDs if available
        recipe_ids = self.recipe_ids if hasattr(self, 'recipe_ids') and self.recipe_ids else []
        batch_ids = self.batch_ids if hasattr(self, 'batch_ids') and self.batch_ids else []
        
        # Create new IDs if needed
        if not recipe_ids:
            recipe_ids = [f"RECIPE-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        if not batch_ids:
            batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create output file
        output_file = os.path.join(self.output_dir, "process_parameters.csv")
        
        # CSV writer for memory efficiency
        with open(output_file, 'w', newline='') as csvfile:
            # Define fieldnames and writer
            fieldnames = [
                'parameter_id', 'parameter_name', 'equipment_id', 'timestamp', 
                'setpoint_value', 'actual_value', 'deviation', 'unit', 
                'upper_control_limit', 'lower_control_limit', 'upper_spec_limit', 
                'lower_spec_limit', 'control_mode', 'recipe_id', 'batch_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Generate parameter records
            print(f"Generating process parameters data...")
            parameters_count = 0
            
            # Code for generating parameters for each equipment
            # Follow the notebook pattern for generating trends, setpoints, etc.
            
            # Return count of generated records
        
        print(f"Successfully generated {parameters_count} process parameter records.")
        print(f"Data saved to {output_file}")
        
        # Return a sample of the data
        return pd.read_csv(output_file, nrows=1000)
    
    def generate_recipes(self, num_recipes=50):
        """
        Generate synthetic data for the Recipes table.
        
        Parameters:
        - num_recipes: Number of recipe records to generate
        
        Returns:
        - DataFrame containing the generated recipes data
        """
        # Create product and personnel IDs if needed
        product_ids = self.product_ids.copy() if hasattr(self, 'product_ids') and self.product_ids else []
        personnel_ids = self.personnel_ids.copy() if hasattr(self, 'personnel_ids') and self.personnel_ids else []
        
        # Generate new IDs if needed
        if not product_ids:
            product_ids = [f"PROD-{uuid.uuid4().hex[:8].upper()}" for _ in range(int(num_recipes * 0.7))]
        
        if not personnel_ids:
            personnel_ids = [f"PERS-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Define recipe types, statuses
        recipe_types = ["Batch", "Continuous", "Discrete", "Testing", "Cleaning", "Validation"]
        recipe_statuses = ["Active", "In Review", "Approved", "Obsolete", "Draft", "Testing"]
        status_weights = [0.6, 0.1, 0.15, 0.05, 0.05, 0.05]  # Mostly active recipes
        
        # Get equipment types for recipe requirements
        equipment_types = []
        if self.equipment_df is not None and len(self.equipment_df) > 0:
            equipment_types = self.equipment_df['equipment_type'].unique().tolist()
        
        if not equipment_types:
            equipment_types = ["Reactor", "Mixer", "Tank", "Blender", "Extruder", "Distillation Column", 
                            "Robot", "Assembly Station", "Testing Station", "Heat Exchanger"]
        
        # Generate recipe data dictionary
        data = {
            "recipe_id": [f"RECIPE-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_recipes)],
            "recipe_name": [],
            "product_id": [],
            "version": [],
            "status": [],
            "author": [],
            "creation_date": [],
            "approval_date": [],
            "approved_by": [],
            "recipe_type": [],
            "equipment_requirement": [],
            "expected_duration_minutes": [],
            "recipe_description": []
        }
        
        # Product-related recipe names
        product_prefixes = ["Production", "Manufacturing", "Processing", "Assembly", "Synthesis", "Formulation"]
        
        # Cleaning recipe names
        cleaning_prefixes = ["CIP", "Cleaning", "Sanitization", "Sterilization", "Flush", "Purge"]
        cleaning_suffixes = ["Procedure", "Protocol", "Sequence", "Cycle"]
        
        # Testing recipe names
        testing_prefixes = ["Test", "Validation", "Verification", "Qualification", "Calibration"]
        testing_suffixes = ["Protocol", "Procedure", "Method", "Sequence"]
        
        # Recipe description templates
        description_templates = [
            "Standard {type} recipe for {product} production using {equipment} equipment.",
            "{type} recipe designed for optimal {product} quality with defined process parameters.",
            "Validated {type} recipe for consistent {product} manufacturing in {equipment} environment.",
            "Optimized {type} process for {product} with reduced cycle time and improved yield.",
            "Regulatory approved {type} recipe for {product} meeting all quality requirements.",
            "{type} recipe with enhanced process control for premium {product} production."
        ]
        
        # Generate data for each recipe
        for i in range(num_recipes):
            # Assign a product ID (some recipes may be for cleaning, testing, etc. without a product)
            if random.random() < 0.8:  # 80% of recipes are for products
                product_id = random.choice(product_ids)
                data["product_id"].append(product_id)
                
                # For product recipes, create product-based names
                prefix = random.choice(product_prefixes)
                # Extract a product code or ID suffix to use in the name
                product_code = product_id.split('-')[-1][:4]
                recipe_name = f"{prefix} Recipe {product_code}-{random.randint(100, 999)}"
                
                # Recipe type (mostly batch for product recipes)
                if random.random() < 0.7:
                    recipe_type = "Batch"
                else:
                    recipe_type = random.choice(["Continuous", "Discrete"])
            else:
                # Non-product recipes (cleaning, testing, etc.)
                data["product_id"].append("")
                
                # Determine if it's a cleaning or testing recipe
                if random.random() < 0.5:
                    # Cleaning recipe
                    prefix = random.choice(cleaning_prefixes)
                    suffix = random.choice(cleaning_suffixes)
                    recipe_name = f"{prefix} {suffix} {random.randint(100, 999)}"
                    recipe_type = random.choice(["Cleaning", "Validation"])
                else:
                    # Testing recipe
                    prefix = random.choice(testing_prefixes)
                    suffix = random.choice(testing_suffixes)
                    recipe_name = f"{prefix} {suffix} {random.randint(100, 999)}"
                    recipe_type = random.choice(["Testing", "Validation"])
            
            data["recipe_name"].append(recipe_name)
            data["recipe_type"].append(recipe_type)
            
            # Version numbering (major.minor)
            major_version = random.randint(1, 3)
            minor_version = random.randint(0, 9)
            data["version"].append(f"{major_version}.{minor_version}")
            
            # Status (weighted random)
            data["status"].append(random.choices(recipe_statuses, weights=status_weights)[0])
            
            # Author (personnel who created the recipe)
            data["author"].append(random.choice(personnel_ids))
            
            # Creation date (1-18 months ago)
            creation_days_ago = random.randint(30, 540)
            creation_date = datetime.now() - timedelta(days=creation_days_ago)
            data["creation_date"].append(creation_date.strftime("%Y-%m-%d"))
            
            # Approval date and approver
            if data["status"][i] in ["Approved", "Active", "Obsolete"]:
                # Approved recipes have approval dates after creation
                approval_days_ago = random.randint(0, min(creation_days_ago - 1, 30))
                approval_date = datetime.now() - timedelta(days=approval_days_ago)
                data["approval_date"].append(approval_date.strftime("%Y-%m-%d"))
                
                # Approver is a different person than author
                available_approvers = [p for p in personnel_ids if p != data["author"][i]]
                data["approved_by"].append(random.choice(available_approvers))
            else:
                # Unapproved recipes
                data["approval_date"].append("")
                data["approved_by"].append("")
            
            # Equipment requirement
            # Select a suitable equipment class for this recipe
            if recipe_type == "Batch":
                possible_classes = ["Reactor", "Mixer", "Tank", "Blender"]
            elif recipe_type == "Continuous":
                possible_classes = ["Extruder", "Distillation Column", "Reactor", "Heat Exchanger"]
            elif recipe_type == "Discrete":
                possible_classes = ["CNC Machine", "Robot", "Injection Molder", "Assembly Station"]
            elif recipe_type == "Cleaning":
                possible_classes = ["Reactor", "Tank", "Mixer", "Filter", "Heat Exchanger"]
            elif recipe_type == "Testing":
                possible_classes = ["Testing Station", "Reactor", "Mixer"]
            else:  # Validation
                possible_classes = ["Reactor", "Tank", "Mixer", "Testing Station"]
            
            # Filter to classes that exist in our equipment types
            available_classes = [c for c in possible_classes if c in equipment_types]
            
            if available_classes:
                equipment_class = random.choice(available_classes)
                data["equipment_requirement"].append(equipment_class)
            else:
                # Default to any available equipment class
                if equipment_types:
                    data["equipment_requirement"].append(random.choice(equipment_types))
                else:
                    data["equipment_requirement"].append("Any")
            
            # Expected duration
            if recipe_type == "Batch":
                # Batch processes typically take longer
                duration = random.randint(60, 480)  # 1-8 hours
            elif recipe_type == "Continuous":
                # Continuous processes typically run for very long periods
                duration = random.randint(480, 4320)  # 8-72 hours
            elif recipe_type == "Discrete":
                # Discrete manufacturing typically has shorter cycles
                duration = random.randint(10, 120)  # 10 min - 2 hours
            elif recipe_type == "Cleaning":
                # Cleaning processes are usually shorter
                duration = random.randint(30, 120)  # 30 min - 2 hours
            else:  # Testing and Validation
                duration = random.randint(60, 240)  # 1-4 hours
            
            data["expected_duration_minutes"].append(duration)
            
            # Generate recipe description
            template = random.choice(description_templates)
            recipe_type_desc = recipe_type.lower()
            product_desc = "product" if data["product_id"][i] == "" else f"product {product_id.split('-')[-1]}"
            equipment_desc = data["equipment_requirement"][i].lower()
            
            description = template.format(
                type=recipe_type_desc,
                product=product_desc,
                equipment=equipment_desc
            )
            
            data["recipe_description"].append(description)
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "recipes.csv")
        df.to_csv(output_file, index=False)
        
        # Store recipe IDs for reference by other methods
        self.recipe_ids = df["recipe_id"].tolist()
        self.recipes_df = df
        
        print(f"Successfully generated {len(df)} recipe records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_batch_steps(self, num_steps_per_recipe=10):
        """
        Generate synthetic batch steps data based on the recipes table.
        
        Parameters:
        - num_steps_per_recipe: Average number of steps per recipe
        
        Returns:
        - DataFrame containing the generated batch steps data
        """
        if self.recipes_df is None or len(self.recipes_df) == 0:
            print("Error: No recipes data available. Generate recipes first.")
            return None
        
        # Check if recipe_type column exists
        if 'recipe_type' not in self.recipes_df.columns:
            print("Error: recipes_df does not have a recipe_type column. Check the generate_recipes method.")
            return None
        
        # Filter recipes to batch/discrete types that have steps
        batch_recipes = self.recipes_df[self.recipes_df['recipe_type'].isin(['Batch', 'Discrete', 'Testing', 'Cleaning'])]
        
        if len(batch_recipes) == 0:
            print("Warning: No batch recipes found. Generating steps for all recipes instead.")
            batch_recipes = self.recipes_df
        
        # Define step types based on recipe types
        step_types = {
            "Batch": [
                "Charging", "Mixing", "Heating", "Cooling", "Reaction", "Holding", 
                "Sampling", "pH Adjustment", "Transfer", "Filtration", "Discharge"
            ],
            "Discrete": [
                "Loading", "Processing", "Assembly", "Testing", "Inspection", 
                "Machining", "Welding", "Packaging", "Labeling", "Unloading"
            ],
            "Testing": [
                "Sample Preparation", "Measurement", "Analysis", "Verification", 
                "Calibration", "Data Collection", "Reporting", "Cleanup"
            ],
            "Cleaning": [
                "Pre-rinse", "Detergent Wash", "Rinse", "Sanitization", 
                "Final Rinse", "Drying", "Inspection", "Documentation"
            ],
            "Validation": [
                "Setup", "Execution", "Data Collection", "Analysis", 
                "Verification", "Documentation", "Approval"
            ]
        }
        
        # Common steps for most recipes
        common_steps = ["Setup", "Documentation", "Cleanup"]
        
        # Generate batch steps data
        data = {
            "step_id": [],
            "recipe_id": [],
            "step_name": [],
            "step_number": [],
            "description": [],
            "expected_duration_minutes": [],
            "step_type": [],
            "equipment_requirement": [],
            "predecessor_steps": [],
            "successor_steps": []
        }
        
        # Process each recipe
        for _, recipe in batch_recipes.iterrows():
            recipe_id = recipe['recipe_id']
            recipe_type = recipe['recipe_type']
            recipe_duration = recipe['expected_duration_minutes']
            
            # Determine appropriate steps for this recipe type
            if recipe_type in step_types:
                possible_steps = step_types[recipe_type]
            else:
                possible_steps = step_types["Batch"]  # Default to batch steps
            
            # Always include some common steps
            all_possible_steps = possible_steps + common_steps
            
            # Vary the number of steps per recipe
            num_steps = max(3, int(random.normalvariate(num_steps_per_recipe, num_steps_per_recipe/4)))
            
            # Make sure we don't have more steps than possible step types
            num_steps = min(num_steps, len(all_possible_steps))
            
            # For recipes with few steps, ensure we have at least setup, main operation, and cleanup
            if num_steps <= 3:
                selected_steps = ["Setup", random.choice(possible_steps), "Cleanup"]
            else:
                # Select steps without replacement (to avoid duplicates)
                # Always include Setup as first step and Cleanup as last step
                middle_steps = random.sample(possible_steps, min(num_steps - 2, len(possible_steps)))
                if len(middle_steps) < num_steps - 2:
                    # We need to add more steps - repeat some randomly
                    additional_needed = num_steps - 2 - len(middle_steps)
                    additional_steps = random.choices(possible_steps, k=additional_needed)
                    middle_steps.extend(additional_steps)
                selected_steps = ["Setup"] + middle_steps + ["Cleanup"]
            
            # Calculate step durations (should sum approximately to recipe duration)
            # Setup and cleanup are typically shorter
            setup_duration = max(5, int(recipe_duration * 0.05))  # 5% of total time, min 5 minutes
            cleanup_duration = max(5, int(recipe_duration * 0.05))  # 5% of total time, min 5 minutes
            
            # Remaining time for operational steps
            remaining_duration = recipe_duration - (setup_duration + cleanup_duration)
            
            # Divide remaining time among operational steps
            # Use a random distribution to make some steps longer than others
            if len(selected_steps) > 2:
                # Generate random weights for duration distribution
                weights = [random.random() for _ in range(len(selected_steps) - 2)]
                total_weight = sum(weights)
                # Normalize weights
                normalized_weights = [w / total_weight for w in weights]
                # Calculate durations
                op_durations = [max(1, int(remaining_duration * w)) for w in normalized_weights]
            else:
                op_durations = []
            
            # Combine all durations
            step_durations = [setup_duration] + op_durations + [cleanup_duration]
            
            # Create step records
            step_ids = []
            for i, (step_name, duration) in enumerate(zip(selected_steps, step_durations)):
                step_id = f"STEP-{uuid.uuid4().hex[:8].upper()}"
                step_ids.append(step_id)
                
                # Determine step type (more specific classification)
                if step_name in ["Setup", "Cleanup", "Documentation"]:
                    step_type = "Utility"
                elif step_name in ["Charging", "Loading", "Transfer", "Discharge", "Unloading"]:
                    step_type = "Material Handling"
                elif step_name in ["Heating", "Cooling", "Reaction", "Holding"]:
                    step_type = "Process"
                elif step_name in ["Sampling", "Testing", "Inspection", "Analysis", "Measurement"]:
                    step_type = "Quality"
                elif step_name in ["Mixing", "Assembly", "Machining", "Welding"]:
                    step_type = "Operation"
                elif step_name in ["Pre-rinse", "Detergent Wash", "Rinse", "Sanitization", "Final Rinse", "Drying"]:
                    step_type = "Cleaning"
                else:
                    step_type = "Standard"
                
                # Create detailed step name
                detailed_step_name = f"{step_name} {i+1}" if i > 0 and i < len(selected_steps)-1 else step_name
                
                # Create step description
                if step_name == "Setup":
                    description = f"Prepare equipment and materials for {recipe_type.lower()} process."
                elif step_name == "Cleanup":
                    description = f"Clean equipment and dispose of waste materials after {recipe_type.lower()} completion."
                elif step_name == "Documentation":
                    description = f"Record process parameters and batch information for {recipe_type.lower()} record."
                else:
                    descriptions = [
                        f"Execute {step_name.lower()} operation according to standard procedure.",
                        f"Perform {step_name.lower()} step with specified parameters.",
                        f"Complete {step_name.lower()} phase of the {recipe_type.lower()} process.",
                        f"{step_name} operation with quality verification.",
                        f"Standard {step_name.lower()} procedure for {recipe_type.lower()} recipe."
                    ]
                    description = random.choice(descriptions)
                
                # Equipment requirement (inherit from recipe)
                equipment_requirement = recipe['equipment_requirement']
                
                data["step_id"].append(step_id)
                data["recipe_id"].append(recipe_id)
                data["step_name"].append(detailed_step_name)
                data["step_number"].append(i + 1)
                data["description"].append(description)
                data["expected_duration_minutes"].append(duration)
                data["step_type"].append(step_type)
                data["equipment_requirement"].append(equipment_requirement)
                
                # Set predecessor steps (leave blank for first step)
                if i == 0:
                    data["predecessor_steps"].append("")
                else:
                    data["predecessor_steps"].append(step_ids[i-1])
                
                # Set successor steps (leave blank for last step)
                if i == len(selected_steps) - 1:
                    data["successor_steps"].append("")
                else:
                    # Will be filled in the next iteration
                    data["successor_steps"].append("")
            
            # Fill in successor steps
            for i in range(len(step_ids) - 1):
                idx = len(data["successor_steps"]) - len(step_ids) + i
                data["successor_steps"][idx] = step_ids[i+1]
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "batch_steps.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.batch_steps_df = df
        
        print(f"Successfully generated {len(df)} batch step records for {len(batch_recipes)} recipes.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_batches(self, num_batches=100, start_time=None, end_time=None):
        """
        Generate synthetic batch data based on the recipes table.
        
        Parameters:
        - num_batches: Number of batch records to generate
        - start_time: Start time for batch history (defaults to 30 days ago)
        - end_time: End time for batch history (defaults to now)
        
        Returns:
        - DataFrame containing the generated batches data
        """
        if self.recipes_df is None or len(self.recipes_df) == 0:
            print("Error: No recipes data available. Generate recipes first.")
            return None
                
        if self.equipment_df is None or len(self.equipment_df) == 0:
            print("Error: No equipment data available. Generate equipment first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()
        
        # Filter recipes to include only batch and discrete recipes
        batch_recipes = self.recipes_df[self.recipes_df['recipe_type'].isin(['Batch', 'Discrete'])]
        
        if len(batch_recipes) == 0:
            print("Warning: No batch recipes found. Using all available recipes.")
            batch_recipes = self.recipes_df
        
        # Generate work order IDs if not available
        work_order_ids = self.work_order_ids.copy() if self.work_order_ids else [f"WO-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Use existing operator IDs or generate new ones
        operator_ids = self.operator_ids.copy() if self.operator_ids else [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        
        # Define batch statuses and their probabilities
        batch_statuses = ["Planned", "In Progress", "Completed", "Aborted", "On Hold", "Rejected"]
        status_weights = [0.15, 0.25, 0.45, 0.05, 0.05, 0.05]  # Higher weights for active and completed
        
        # Generate batch data
        data = {
            "batch_id": [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_batches)],
            "recipe_id": [],
            "work_order_id": [],
            "product_id": [],
            "batch_size": [],
            "batch_size_unit": [],
            "planned_start_time": [],
            "actual_start_time": [],
            "planned_end_time": [],
            "actual_end_time": [],
            "batch_status": [],
            "equipment_id": [],
            "operator_id": [],
            "parent_batch_id": []
        }
        
        # Create a hierarchy with some batches having parent batches
        # First, identify which batches will be parents (about 20%)
        potential_parents = random.sample(data["batch_id"], int(num_batches * 0.2))
        
        # Units of measurement based on product type
        units = ["kg", "L", "units", "gal", "m³", "tons"]
        
        # Generate data for each batch
        for i in range(num_batches):
            batch_id = data["batch_id"][i]
            
            # Select recipe (weighted toward active recipes)
            active_recipes = batch_recipes[batch_recipes['status'] == 'Active']
            if len(active_recipes) > 0 and random.random() < 0.8:  # 80% chance of using active recipe
                recipe = active_recipes.sample(1).iloc[0]
            else:
                recipe = batch_recipes.sample(1).iloc[0]
            
            data["recipe_id"].append(recipe['recipe_id'])
            
            # Assign work order (some batches may not be associated with a work order)
            if random.random() < 0.9:  # 90% chance of having a work order
                data["work_order_id"].append(random.choice(work_order_ids))
            else:
                data["work_order_id"].append("")
            
            # Assign product (use product from recipe if available)
            if 'product_id' in recipe and pd.notna(recipe['product_id']) and recipe['product_id'] != "":
                data["product_id"].append(recipe['product_id'])
            else:
                data["product_id"].append("")
            
            # Generate batch size (random but realistic)
            if recipe['recipe_type'] == 'Batch':
                # Batch sizes tend to be in specific ranges depending on industry
                size_options = [50, 100, 200, 500, 1000, 2000, 5000]
                batch_size = random.choice(size_options) * random.uniform(0.8, 1.2)  # Add some variation
                batch_size = round(batch_size, 1)
            elif recipe['recipe_type'] == 'Discrete':
                # Discrete manufacturing typically produces in lot sizes
                size_options = [10, 25, 50, 100, 250, 500, 1000]
                batch_size = random.choice(size_options)
            else:
                # Default size range for other recipe types
                batch_size = random.randint(50, 5000)
            
            data["batch_size"].append(batch_size)
            
            # Assign appropriate unit
            unit = random.choice(units)
            data["batch_size_unit"].append(unit)
            
            # Generate batch timing
            # Create a random start time within the specified range
            time_range_minutes = int((end_time - start_time).total_seconds() / 60)
            random_minutes_offset = random.randint(0, time_range_minutes)
            planned_start_time = start_time + timedelta(minutes=random_minutes_offset)
            
            # Get expected duration from recipe
            if 'expected_duration_minutes' in recipe and pd.notna(recipe['expected_duration_minutes']):
                expected_duration = float(recipe['expected_duration_minutes'])
            else:
                expected_duration = random.randint(60, 480)  # Default 1-8 hours
            
            # Add some variation to the duration for planned vs actual
            planned_duration = expected_duration * random.uniform(0.9, 1.1)  # +/- 10%
            planned_end_time = planned_start_time + timedelta(minutes=planned_duration)
            
            data["planned_start_time"].append(planned_start_time.strftime("%Y-%m-%d %H:%M:%S"))
            data["planned_end_time"].append(planned_end_time.strftime("%Y-%m-%d %H:%M:%S"))
            
            # Determine batch status (weighted random)
            batch_status = random.choices(batch_statuses, weights=status_weights)[0]
            data["batch_status"].append(batch_status)
            
            # Set actual times based on status
            if batch_status == "Planned":
                # Hasn't started yet
                data["actual_start_time"].append("")
                data["actual_end_time"].append("")
            elif batch_status == "In Progress":
                # Started but not finished
                # Actual start time might have some deviation from planned
                start_deviation_minutes = random.randint(-60, 60)  # +/- 1 hour
                actual_start_time = planned_start_time + timedelta(minutes=start_deviation_minutes)
                
                # Ensure actual start time is not in the future
                if actual_start_time > end_time:
                    actual_start_time = end_time - timedelta(minutes=random.randint(0, 60))
                    
                data["actual_start_time"].append(actual_start_time.strftime("%Y-%m-%d %H:%M:%S"))
                data["actual_end_time"].append("")  # Still in progress
            elif batch_status in ["Completed", "Aborted", "Rejected"]:
                # Both started and finished
                start_deviation_minutes = random.randint(-60, 60)  # +/- 1 hour
                actual_start_time = planned_start_time + timedelta(minutes=start_deviation_minutes)
                
                # For actual duration, completed batches are typically close to planned
                # Aborted/rejected batches may be shorter
                if batch_status == "Completed":
                    actual_duration = planned_duration * random.uniform(0.9, 1.2)  # +/- 20%
                else:
                    # Aborted/rejected batches often finish early
                    actual_duration = planned_duration * random.uniform(0.2, 0.8)  # 20-80% of planned
                
                actual_end_time = actual_start_time + timedelta(minutes=actual_duration)
                
                # Ensure times are within overall time range
                if actual_start_time > end_time:
                    actual_start_time = end_time - timedelta(minutes=int(actual_duration) + 1)
                if actual_end_time > end_time:
                    actual_end_time = end_time
                    
                data["actual_start_time"].append(actual_start_time.strftime("%Y-%m-%d %H:%M:%S"))
                data["actual_end_time"].append(actual_end_time.strftime("%Y-%m-%d %H:%M:%S"))
            else:  # On Hold
                # Started but paused
                start_deviation_minutes = random.randint(-60, 60)  # +/- 1 hour
                actual_start_time = planned_start_time + timedelta(minutes=start_deviation_minutes)
                
                # Ensure actual start time is not in the future
                if actual_start_time > end_time:
                    actual_start_time = end_time - timedelta(minutes=random.randint(0, 60))
                    
                data["actual_start_time"].append(actual_start_time.strftime("%Y-%m-%d %H:%M:%S"))
                data["actual_end_time"].append("")  # Not finished
            
            # Assign equipment based on recipe requirement
            equipment_requirement = recipe['equipment_requirement'] if 'equipment_requirement' in recipe else None
            if equipment_requirement and equipment_requirement in self.equipment_df['equipment_type'].values:
                # Use equipment of the required type
                matching_equipment = self.equipment_df[self.equipment_df['equipment_type'] == equipment_requirement]
                data["equipment_id"].append(matching_equipment.sample(1).iloc[0]['equipment_id'])
            else:
                # Use any available equipment
                data["equipment_id"].append(self.equipment_df.sample(1).iloc[0]['equipment_id'])
            
            # Assign operator
            data["operator_id"].append(random.choice(operator_ids))
            
            # Determine parent batch (if any)
            # About 10% of batches will have a parent
            if batch_id not in potential_parents and random.random() < 0.1:
                # Ensure we don't create circular references
                available_parents = [p for p in potential_parents if p != batch_id]
                if available_parents:
                    parent_id = random.choice(available_parents)
                    data["parent_batch_id"].append(parent_id)
                else:
                    data["parent_batch_id"].append("")
            else:
                data["parent_batch_id"].append("")
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "batches.csv")
        df.to_csv(output_file, index=False)
        
        # Update batch_ids for reference by other methods
        self.batch_ids = df["batch_id"].tolist()
        self.batches_df = df
        
        print(f"Successfully generated {len(df)} batch records.")
        print(f"Data saved to {output_file}")
        
        return df

    def generate_batch_execution(self, start_time=None, end_time=None):
        """
        Generate synthetic batch step execution data based on the batches and batch steps tables.
        
        Parameters:
        - start_time: Start time for execution history (defaults to 30 days ago)
        - end_time: End time for execution history (defaults to now)
        
        Returns:
        - DataFrame containing a sample of the generated batch execution data
        """
        if self.batches_df is None or len(self.batches_df) == 0:
            print("Error: No batches data available. Generate batches first.")
            return None
                
        if self.batch_steps_df is None or len(self.batch_steps_df) == 0:
            print("Error: No batch steps data available. Generate batch steps first.")
            return None
                
        if self.equipment_df is None or len(self.equipment_df) == 0:
            print("Error: No equipment data available. Generate equipment first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()
        
        # Define step execution statuses
        step_statuses = ["Completed", "In Progress", "Pending", "Aborted", "Skipped", 
                        "Paused", "Completed with Issues", "Verified", "Reworked"]
        
        # Get operator IDs
        operator_ids = self.operator_ids.copy() if self.operator_ids else [f"OP-{uuid.uuid4().hex[:8].upper()}" for _ in range(15)]
        
        # Create CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "batch_execution.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'execution_id', 'batch_id', 'step_id', 'equipment_id', 
                'start_time', 'end_time', 'status', 'operator_id',
                'actual_duration_minutes', 'deviation_reason', 'step_parameters'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Generate execution records for each batch and its steps
            print(f"Generating batch step execution data...")
            execution_count = 0
            
            # Group batch steps by recipe
            recipe_steps = {}
            for _, step in self.batch_steps_df.iterrows():
                recipe_id = step['recipe_id']
                if recipe_id not in recipe_steps:
                    recipe_steps[recipe_id] = []
                recipe_steps[recipe_id].append(step)
            
            # Process each batch
            for _, batch in self.batches_df.iterrows():
                batch_id = batch['batch_id']
                recipe_id = batch['recipe_id'] if 'recipe_id' in batch else None
                batch_status = batch['batch_status'] if 'batch_status' in batch else "Planned"
                
                # Skip if no steps for this recipe
                if recipe_id is None or recipe_id not in recipe_steps or not recipe_steps[recipe_id]:
                    continue
                
                # Get all steps for this recipe
                steps = recipe_steps[recipe_id]
                
                # Sort steps by step number
                steps = sorted(steps, key=lambda x: x['step_number'] if 'step_number' in x else 0)
                
                # Get batch timing
                if 'planned_start_time' in batch and pd.notna(batch['planned_start_time']) and batch['planned_start_time']:
                    planned_start_time = datetime.strptime(batch['planned_start_time'], "%Y-%m-%d %H:%M:%S")
                else:
                    planned_start_time = datetime.now() - timedelta(days=random.randint(1, 30))
                    
                if 'actual_start_time' in batch and pd.notna(batch['actual_start_time']) and batch['actual_start_time']:
                    batch_start_time = datetime.strptime(batch['actual_start_time'], "%Y-%m-%d %H:%M:%S")
                else:
                    # For planned batches, use a future start time
                    batch_start_time = None
                    
                if 'actual_end_time' in batch and pd.notna(batch['actual_end_time']) and batch['actual_end_time']:
                    batch_end_time = datetime.strptime(batch['actual_end_time'], "%Y-%m-%d %H:%M:%S")
                else:
                    batch_end_time = None
                
                # Get the equipment assigned to this batch
                batch_equipment_id = batch['equipment_id'] if 'equipment_id' in batch and pd.notna(batch['equipment_id']) else None
                
                # Generate step execution records for this batch
                current_time = batch_start_time if batch_start_time else planned_start_time
                
                for i, step in enumerate(steps):
                    step_id = step['step_id']
                    expected_duration = step['expected_duration_minutes'] if 'expected_duration_minutes' in step else 30
                    
                    # Create a unique execution ID
                    execution_id = f"EXEC-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Assign the same operator as the batch or a random one
                    if 'operator_id' in batch and batch['operator_id'] and random.random() < 0.7:  # 70% chance of same operator as batch
                        operator_id = batch['operator_id']
                    else:
                        operator_id = random.choice(operator_ids)
                    
                    # Determine equipment (use batch equipment if possible)
                    if batch_equipment_id:
                        equipment_id = batch_equipment_id
                    else:
                        # Try to find equipment of required type
                        equipment_type = step['equipment_requirement'] if 'equipment_requirement' in step else None
                        if equipment_type and self.equipment_df is not None:
                            matching_equipment = self.equipment_df[self.equipment_df['equipment_type'] == equipment_type]
                            if len(matching_equipment) > 0:
                                equipment_id = matching_equipment.sample(1).iloc[0]['equipment_id']
                            else:
                                equipment_id = self.equipment_df.sample(1).iloc[0]['equipment_id']
                        else:
                            # No specific requirement, pick any equipment
                            equipment_id = self.equipment_df.sample(1).iloc[0]['equipment_id'] if len(self.equipment_df) > 0 else ""
                    
                    # Determine step execution status based on batch status
                    if batch_status == "Planned":
                        # All steps are pending for planned batches
                        status = "Pending"
                        start_time_str = ""
                        end_time_str = ""
                        actual_duration = 0
                    elif batch_status == "In Progress":
                        # Steps before current point are completed, current step in progress, rest pending
                        if i < len(steps) / 3:  # First third of steps
                            status = "Completed"
                            # Add some variation to actual duration
                            actual_duration = expected_duration * random.uniform(0.8, 1.2)  # +/- 20%
                            start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            end_time = current_time + timedelta(minutes=actual_duration)
                            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                            current_time = end_time
                        elif i < len(steps) / 2:  # Middle
                            status = "In Progress"
                            start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            end_time_str = ""
                            actual_duration = (datetime.now() - current_time).total_seconds() / 60
                        else:  # Remaining steps
                            status = "Pending"
                            start_time_str = ""
                            end_time_str = ""
                            actual_duration = 0
                    elif batch_status == "Completed":
                        # All steps are completed for completed batches
                        status = "Completed"
                        # Add some variation to actual duration
                        actual_duration = expected_duration * random.uniform(0.8, 1.2)  # +/- 20%
                        start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                        end_time = current_time + timedelta(minutes=actual_duration)
                        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                        current_time = end_time
                    elif batch_status == "Aborted":
                        # Steps before abort point are completed, abort step is aborted, rest are skipped
                        abort_point = random.randint(0, len(steps) - 1)
                        if i < abort_point:
                            status = "Completed"
                            actual_duration = expected_duration * random.uniform(0.8, 1.2)
                        elif i == abort_point:
                            status = "Aborted"
                            actual_duration = expected_duration * random.uniform(0.1, 0.5)  # Aborted early
                        else:
                            status = "Skipped"
                            actual_duration = 0
                            
                        if i <= abort_point:
                            start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            end_time = current_time + timedelta(minutes=actual_duration)
                            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                            current_time = end_time
                        else:
                            start_time_str = ""
                            end_time_str = ""
                    elif batch_status == "On Hold":
                        # Some steps completed, current step paused
                        hold_point = random.randint(1, len(steps) - 1)
                        if i < hold_point:
                            status = "Completed"
                            actual_duration = expected_duration * random.uniform(0.8, 1.2)
                            start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            end_time = current_time + timedelta(minutes=actual_duration)
                            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                            current_time = end_time
                        elif i == hold_point:
                            status = "Paused"
                            actual_duration = expected_duration * random.uniform(0.1, 0.8)  # Partially complete
                            start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                            end_time_str = ""
                        else:
                            status = "Pending"
                            actual_duration = 0
                            start_time_str = ""
                            end_time_str = ""
                    else:  # Rejected or other statuses
                        # Similar to completed but some steps might have issues
                        if random.random() < 0.8:  # 80% chance of normal completion
                            status = "Completed"
                        else:
                            status = random.choice(["Completed with Issues", "Reworked", "Verified"])
                            
                        actual_duration = expected_duration * random.uniform(0.8, 1.2)
                        start_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
                        end_time = current_time + timedelta(minutes=actual_duration)
                        end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                        current_time = end_time
                    
                    # Check if we've exceeded the batch end time (if known)
                    if batch_end_time and current_time > batch_end_time:
                        # Adjust the end time and duration
                        if status in ["Completed", "Aborted"]:
                            end_time_str = batch_end_time.strftime("%Y-%m-%d %H:%M:%S")
                            if start_time_str:
                                try:
                                    start_datetime = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
                                    actual_duration = (batch_end_time - start_datetime).total_seconds() / 60
                                except:
                                    actual_duration = 0
                    
                    # Generate deviation reason (only for some steps)
                    if status in ["Completed", "Aborted", "Paused"] and abs(actual_duration - expected_duration) > expected_duration * 0.1:
                        deviation_reasons = [
                            "Process variation", "Equipment issue", "Material quality", "Operator intervention",
                            "Quality check", "Parameter adjustment", "Waiting for upstream process",
                            "Environmental factors", "Power fluctuation", "Scheduled break"
                        ]
                        deviation_reason = random.choice(deviation_reasons)
                    else:
                        deviation_reason = ""
                    
                    # Generate step parameters (JSON string with key process parameters)
                    if status in ["Completed", "In Progress", "Paused", "Aborted"]:
                        # Create parameter names based on step type
                        step_type = step['step_type'] if 'step_type' in step else "Standard"
                        
                        if step_type == "Material Handling":
                            param_names = ["Material ID", "Quantity", "Container ID", "Verification"]
                        elif step_type == "Process":
                            param_names = ["Temperature", "Pressure", "Time", "Agitation", "pH"]
                        elif step_type == "Quality":
                            param_names = ["Sample ID", "Result", "Specification", "Deviation"]
                        elif step_type == "Operation":
                            param_names = ["Speed", "Duration", "Power", "Mode"]
                        elif step_type == "Cleaning":
                            param_names = ["Agent", "Concentration", "Temperature", "Time"]
                        else:
                            param_names = ["Parameter1", "Parameter2", "Parameter3"]
                        
                        # Generate random parameter values
                        params = {}
                        for param in param_names:
                            if "Temperature" in param:
                                params[param] = f"{random.uniform(20, 100):.1f} °C"
                            elif "Pressure" in param:
                                params[param] = f"{random.uniform(1, 10):.2f} bar"
                            elif "Time" in param:
                                params[param] = f"{random.randint(5, 120)} min"
                            elif "Quantity" in param:
                                params[param] = f"{random.uniform(10, 1000):.1f} kg"
                            elif "Speed" in param:
                                params[param] = f"{random.randint(50, 1000)} rpm"
                            elif "Concentration" in param:
                                params[param] = f"{random.uniform(1, 5):.2f} %"
                            elif "pH" in param:
                                params[param] = f"{random.uniform(2, 12):.1f}"
                            elif "ID" in param:
                                params[param] = f"{param[0]}-{random.randint(1000, 9999)}"
                            elif "Result" in param or "Verification" in param:
                                params[param] = random.choice(["Pass", "Within Spec", "Acceptable"])
                            elif "Mode" in param:
                                params[param] = random.choice(["Auto", "Manual", "Semi-Auto"])
                            elif "Agent" in param:
                                params[param] = random.choice(["CIP-100", "Caustic", "Acid", "Water"])
                            else:
                                params[param] = f"{random.uniform(0, 100):.2f}"
                        
                        # Convert to JSON string
                        import json
                        step_parameters = json.dumps(params)
                    else:
                        step_parameters = "{}"
                    
                    # Write the execution record to the CSV
                    writer.writerow({
                        'execution_id': execution_id,
                        'batch_id': batch_id,
                        'step_id': step_id,
                        'equipment_id': equipment_id,
                        'start_time': start_time_str,
                        'end_time': end_time_str,
                        'status': status,
                        'operator_id': operator_id,
                        'actual_duration_minutes': round(actual_duration) if actual_duration else "",
                        'deviation_reason': deviation_reason,
                        'step_parameters': step_parameters
                    })
                    
                    execution_count += 1
                    if execution_count % 1000 == 0:
                        print(f"  Generated {execution_count:,} batch execution records so far...")
            
            print(f"Successfully generated {execution_count} batch execution records.")
            print(f"Data saved to {output_file}")
        
        # Return a sample of the data (use try/except to handle the case of an empty file)
        try:
            return pd.read_csv(output_file, nrows=1000)
        except pd.errors.EmptyDataError:
            print("Warning: No batch execution records were generated. File is empty.")
            # Return an empty DataFrame with the expected columns
            return pd.DataFrame(columns=[
                'execution_id', 'batch_id', 'step_id', 'equipment_id', 
                'start_time', 'end_time', 'status', 'operator_id',
                'actual_duration_minutes', 'deviation_reason', 'step_parameters'
            ])


def main():
    """Main function to run the data generator"""
    parser = argparse.ArgumentParser(description='Generate ISA-95 Level 2 data')
    parser.add_argument('--output', type=str, default='data', 
                      help='Output directory for generated data (default: data)')
    parser.add_argument('--equipment', type=int, default=150, 
                      help='Number of equipment records to generate (default: 150)')
    parser.add_argument('--areas', type=int, default=37, 
                      help='Number of process areas to generate (default: 37)')
    parser.add_argument('--facilities', type=int, default=5, 
                      help='Number of facilities to generate (default: 5)')
    parser.add_argument('--recipes', type=int, default=50, 
                      help='Number of recipes to generate (default: 50)')
    parser.add_argument('--batches', type=int, default=100, 
                      help='Number of batches to generate (default: 100)')
    parser.add_argument('--use-level1', action='store_true',
                      help='Use existing Level 1 data for consistency (default: False)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ISA95Level2DataGenerator(output_dir=args.output, level1_data_available=args.use_level1)
    
    # Start timer
    start_time = time.time()
    
    # Generate all data
    generator.generate_all_data(
        num_equipment=args.equipment,
        num_areas=args.areas,
        num_facilities=args.facilities,
        num_recipes=args.recipes,
        num_batches=args.batches
    )
    
    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\nTotal generation time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()