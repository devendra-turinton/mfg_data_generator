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

class ISA95Level1DataGenerator:
    """
    Generator for ISA-95 Level 1 (Sensing & Manipulation) data.
    
    This class generates synthetic data for all tables in Level 1:
    - Sensors
    - SensorReadings
    - Actuators
    - ActuatorCommands
    - DeviceDiagnostics
    - ControlLoops
    """
    
    def __init__(self, output_dir="data"):
        """
        Initialize the data generator.
        
        Parameters:
        - output_dir: Directory where generated data will be saved
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Track generated data for relationships
        self.sensors_df = None
        self.actuators_df = None
        self.control_loops_df = None
        
        # Define common reference data
        self.equipment_ids = []
        self.batch_ids = []
        self.step_ids = []
        self.operator_ids = []
        self.equipment_state_ids = []
        
        # Initialize reference data
        self._init_reference_data()
    
    def _init_reference_data(self):
        """Initialize reference data used across tables"""
        # Create equipment IDs (usually fewer than sensors/actuators)
        self.equipment_ids = [f"EQ-{uuid.uuid4().hex[:8].upper()}" for _ in range(30)]
        
        # Create batch IDs for readings and commands
        self.batch_ids = [f"BATCH-{uuid.uuid4().hex[:8].upper()}" for _ in range(20)]
        
        # Create step IDs for commands
        self.step_ids = [f"STEP-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
        
        # Create operator IDs for manual commands
        self.operator_ids = [f"OP-{uuid.uuid4().hex[:6].upper()}" for _ in range(15)]
        
        # Create equipment state IDs
        self.equipment_state_ids = [f"STATE-{uuid.uuid4().hex[:8].upper()}" for _ in range(50)]
    
    def generate_all_data(self, num_sensors=100, num_actuators=100, readings_per_sensor=1000, 
                         commands_per_actuator=100, num_control_loops=50):
        """
        Generate data for all Level 1 tables.
        
        Parameters:
        - num_sensors: Number of sensor records to generate
        - num_actuators: Number of actuator records to generate
        - readings_per_sensor: Number of readings per sensor
        - commands_per_actuator: Number of commands per actuator
        - num_control_loops: Number of control loops to configure
        """
        print("=== ISA-95 Level 1 Data Generation ===")
        
        # Generate data for each table
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        
        print(f"\n1. Generating {num_sensors} Sensors...")
        self.generate_sensors(num_sensors)
        
        print(f"\n2. Generating {num_actuators} Actuators...")
        self.generate_actuators(num_actuators)
        
        print(f"\n3. Generating {num_sensors * readings_per_sensor} Sensor Readings...")
        self.generate_sensor_readings(readings_per_sensor, start_time, end_time)
        
        print(f"\n4. Generating {num_actuators * commands_per_actuator} Actuator Commands...")
        self.generate_actuator_commands(commands_per_actuator, start_time, end_time)
        
        print(f"\n5. Generating Device Diagnostics...")
        self.generate_device_diagnostics(start_time, end_time)
        
        print(f"\n6. Generating {num_control_loops} Control Loops...")
        self.generate_control_loops(num_control_loops)
        
        print("\nData generation complete!")
    
    def generate_sensors(self, num_records=100):
        """
        Generate synthetic data for the Sensors table.
        
        Parameters:
        - num_records: Number of sensor records to generate
        
        Returns:
        - DataFrame containing the generated sensor data
        """
        # Define possible values for categorical fields
        sensor_types = ["temperature", "pressure", "flow", "level", "ph", "conductivity", 
                      "vibration", "speed", "torque", "current", "voltage", "weight", 
                      "humidity", "oxygen", "co2", "position", "proximity", "rpm"]
        
        manufacturers = ["Siemens", "ABB", "Emerson", "Honeywell", "Endress+Hauser", 
                        "Yokogawa", "Schneider Electric", "Omron", "Rockwell Automation", 
                        "WIKA", "Vega", "ifm electronic", "Pepperl+Fuchs", "Sick AG"]
        
        statuses = ["Active", "Maintenance", "Calibration Due", "Fault", "Standby", "Offline"]
        
        # Generate sensor data
        data = {
            "sensor_id": [f"SEN-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_records)],
            "equipment_id": [random.choice(self.equipment_ids) for _ in range(num_records)],
            "sensor_type": [random.choice(sensor_types) for _ in range(num_records)],
            "manufacturer": [random.choice(manufacturers) for _ in range(num_records)],
            "model_number": [f"M{random.randint(1000, 9999)}-{random.choice(['A', 'B', 'C', 'D', 'E'])}{random.randint(10, 99)}" for _ in range(num_records)],
            "installation_date": [(datetime.now() - timedelta(days=random.randint(30, 1825))).strftime("%Y-%m-%d") for _ in range(num_records)],
            "calibration_due_date": [(datetime.now() + timedelta(days=random.randint(-30, 365))).strftime("%Y-%m-%d") for _ in range(num_records)],
            "location_x": [round(random.uniform(0, 100), 2) for _ in range(num_records)],
            "location_y": [round(random.uniform(0, 100), 2) for _ in range(num_records)],
            "location_z": [round(random.uniform(0, 10), 2) for _ in range(num_records)],
        }
        
        # Create a DataFrame
        df = pd.DataFrame(data)
        
        # Add measurement units based on sensor type
        def get_measurement_unit(sensor_type):
            units = {
                "temperature": "°C",
                "pressure": "bar",
                "flow": "m³/h",
                "level": "%",
                "ph": "pH",
                "conductivity": "µS/cm",
                "vibration": "mm/s",
                "speed": "rpm",
                "torque": "Nm",
                "current": "A",
                "voltage": "V",
                "weight": "kg",
                "humidity": "%RH",
                "oxygen": "%",
                "co2": "ppm",
                "position": "mm",
                "proximity": "mm",
                "rpm": "rpm"
            }
            return units.get(sensor_type, "unit")
        
        df["measurement_unit"] = df["sensor_type"].apply(get_measurement_unit)
        
        # Generate measurement ranges based on sensor type
        def get_measurement_range(row):
            ranges = {
                "temperature": (0, 150),
                "pressure": (0, 25),
                "flow": (0, 100),
                "level": (0, 100),
                "ph": (0, 14),
                "conductivity": (0, 2000),
                "vibration": (0, 50),
                "speed": (0, 3000),
                "torque": (0, 500),
                "current": (0, 100),
                "voltage": (0, 440),
                "weight": (0, 2000),
                "humidity": (0, 100),
                "oxygen": (0, 25),
                "co2": (0, 5000),
                "position": (0, 1000),
                "proximity": (0, 50),
                "rpm": (0, 5000)
            }
            
            default_range = (0, 100)
            min_val, max_val = ranges.get(row["sensor_type"], default_range)
            
            # Add some variation to ranges
            min_val = max(0, min_val - random.uniform(0, min_val/5))
            max_val = max_val + random.uniform(0, max_val/5)
            
            return min_val, max_val
        
        # Apply the function to each row and create range min/max columns
        df["temp_ranges"] = df.apply(get_measurement_range, axis=1)
        df["measurement_range_min"] = df["temp_ranges"].apply(lambda x: round(x[0], 2))
        df["measurement_range_max"] = df["temp_ranges"].apply(lambda x: round(x[1], 2))
        df.drop("temp_ranges", axis=1, inplace=True)
        
        # Add accuracy based on sensor type and a bit of randomness
        df["accuracy"] = df["sensor_type"].apply(
            lambda x: round(random.uniform(0.1, 2.0), 2)
        )
        
        # Add status
        df["status"] = [random.choice(statuses) for _ in range(num_records)]
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "sensors.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.sensors_df = df
        
        print(f"Saved {num_records} sensor records to {output_file}")
        print(f"Sample data (first 3 records):")
        print(df.head(3))
        
        return df
    
    def generate_actuators(self, num_records=100):
        """
        Generate synthetic data for the Actuators table.
        
        Parameters:
        - num_records: Number of actuator records to generate
        
        Returns:
        - DataFrame containing the generated actuator data
        """
        # Define possible values for categorical fields
        actuator_types = ["valve", "motor", "pump", "heater", "fan", "agitator", "conveyor", 
                         "damper", "cylinder", "positioner", "relay", "switch", "mixer", 
                         "doser", "compressor", "blower"]
        
        manufacturers = ["Siemens", "ABB", "Emerson", "Honeywell", "Schneider Electric", 
                        "Festo", "SMC", "Bürkert", "Danfoss", "Asco", "Parker", "Rotork", 
                        "Auma", "Allen-Bradley", "SEW-Eurodrive", "WEG"]
        
        statuses = ["Active", "Maintenance", "Fault", "Standby", "Offline", "Reserved"]
        
        # Generate actuator data
        data = {
            "actuator_id": [f"ACT-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_records)],
            "equipment_id": [random.choice(self.equipment_ids) for _ in range(num_records)],
            "actuator_type": [random.choice(actuator_types) for _ in range(num_records)],
            "manufacturer": [random.choice(manufacturers) for _ in range(num_records)],
            "model_number": [f"A{random.randint(1000, 9999)}-{random.choice(['X', 'Y', 'Z', 'S', 'P'])}{random.randint(10, 99)}" for _ in range(num_records)],
            "installation_date": [(datetime.now() - timedelta(days=random.randint(30, 1825))).strftime("%Y-%m-%d") for _ in range(num_records)],
            "location_x": [round(random.uniform(0, 100), 2) for _ in range(num_records)],
            "location_y": [round(random.uniform(0, 100), 2) for _ in range(num_records)],
            "location_z": [round(random.uniform(0, 10), 2) for _ in range(num_records)],
        }
        
        # Create a DataFrame
        df = pd.DataFrame(data)
        
        # Generate control ranges based on actuator type
        def get_control_range(row):
            ranges = {
                "valve": (0, 100),             # Percent open
                "motor": (0, 3000),            # RPM
                "pump": (0, 500),              # Flow rate
                "heater": (0, 500),            # Temperature
                "fan": (0, 100),               # Percent speed
                "agitator": (0, 100),          # Percent speed
                "conveyor": (0, 10),           # m/s
                "damper": (0, 100),            # Percent open
                "cylinder": (0, 1000),         # mm extension
                "positioner": (0, 360),        # Degrees
                "relay": (0, 1),               # On/Off
                "switch": (0, 1),              # On/Off
                "mixer": (0, 100),             # Percent speed
                "doser": (0, 50),              # L/min
                "compressor": (0, 200),        # Bar
                "blower": (0, 100),            # Percent speed
            }
            
            default_range = (0, 100)
            min_val, max_val = ranges.get(row["actuator_type"], default_range)
            
            # Add some variation to ranges
            min_val = max(0, min_val)
            max_val = max_val + random.uniform(0, max_val/10)
            
            return min_val, max_val
        
        # Apply the function to each row and create range min/max columns
        df["temp_ranges"] = df.apply(get_control_range, axis=1)
        df["control_range_min"] = df["temp_ranges"].apply(lambda x: round(x[0], 2))
        df["control_range_max"] = df["temp_ranges"].apply(lambda x: round(x[1], 2))
        df.drop("temp_ranges", axis=1, inplace=True)
        
        # Add control units based on actuator type
        def get_control_unit(actuator_type):
            units = {
                "valve": "%",
                "motor": "rpm",
                "pump": "m³/h",
                "heater": "°C",
                "fan": "%",
                "agitator": "%",
                "conveyor": "m/s",
                "damper": "%",
                "cylinder": "mm",
                "positioner": "°",
                "relay": "binary",
                "switch": "binary",
                "mixer": "%",
                "doser": "L/min",
                "compressor": "bar",
                "blower": "%"
            }
            return units.get(actuator_type, "unit")
        
        df["control_unit"] = df["actuator_type"].apply(get_control_unit)
        
        # Add status
        df["status"] = [random.choice(statuses) for _ in range(num_records)]
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "actuators.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.actuators_df = df
        
        print(f"Saved {num_records} actuator records to {output_file}")
        print(f"Sample data (first 3 records):")
        print(df.head(3))
        
        return df
    
    def generate_sensor_readings(self, num_readings_per_sensor=1000, start_time=None, end_time=None):
        """
        Generate synthetic sensor readings data based on the sensors table.
        
        Parameters:
        - num_readings_per_sensor: Number of readings to generate per sensor
        - start_time: Start time for readings (defaults to 7 days ago)
        - end_time: End time for readings (defaults to now)
        
        Returns:
        - Sample DataFrame containing a subset of the generated readings data
        """
        if self.sensors_df is None or len(self.sensors_df) == 0:
            print("Error: No sensor data available. Generate sensors first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        # Prepare the output file with CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "sensor_readings.csv")
        
        # Use CSV writer for memory efficiency with large datasets
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'reading_id', 'sensor_id', 'timestamp', 'value', 
                'quality_indicator', 'status_code', 'batch_id', 'equipment_state_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Calculate total number of readings
            total_readings = len(self.sensors_df) * num_readings_per_sensor
            readings_count = 0
            
            # Process each sensor
            for _, sensor in self.sensors_df.iterrows():
                # Get the sensor's measurement range
                min_val = sensor['measurement_range_min']
                max_val = sensor['measurement_range_max']
                
                # Generate timestamps within the specified range
                time_points = [
                    start_time + (end_time - start_time) * (i / num_readings_per_sensor)
                    for i in range(num_readings_per_sensor)
                ]
                
                # Sort timestamps to ensure chronological order
                time_points.sort()
                
                # Generate base trend for this sensor (smooth curve + noise)
                # This creates more realistic data than pure random values
                base_trend = np.sin(np.linspace(0, random.randint(3, 8) * np.pi, num_readings_per_sensor))
                
                # Add noise and scaling to make it look realistic
                noise_level = (max_val - min_val) * random.uniform(0.05, 0.15)  # 5-15% noise
                
                # Generate realistic values for this sensor
                for i in range(num_readings_per_sensor):
                    # Create a unique reading ID
                    reading_id = f"READ-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Format timestamp
                    timestamp = time_points[i].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    
                    # Generate a somewhat realistic value with trend and noise
                    # Scale the base trend to fit within the sensor's range
                    scaled_value = min_val + (base_trend[i] + 1) / 2 * (max_val - min_val)
                    
                    # Add noise to the value
                    value = scaled_value + random.uniform(-noise_level, noise_level)
                    value = max(min_val, min(max_val, value))  # Clip to range
                    value = round(value, 2)
                    
                    # Quality indicator (higher is better, occasional low quality)
                    if random.random() < 0.05:  # 5% chance of lower quality
                        quality_indicator = round(random.uniform(50, 85), 1)
                    else:
                        quality_indicator = round(random.uniform(85, 100), 1)
                    
                    # Status code (mostly 0 = normal, occasional other values)
                    if random.random() < 0.03:  # 3% chance of abnormal status
                        status_code = random.choice([1, 2, 3, 4])  # Different error/warning codes
                    else:
                        status_code = 0  # Normal operation
                    
                    # Batch ID (some readings may not be associated with a batch)
                    if random.random() < 0.8:  # 80% chance of having a batch
                        batch_id = random.choice(self.batch_ids)
                    else:
                        batch_id = ""
                    
                    # Equipment state (some readings may not have an equipment state)
                    if random.random() < 0.9:  # 90% chance of having an equipment state
                        equipment_state_id = random.choice(self.equipment_state_ids)
                    else:
                        equipment_state_id = ""
                    
                    # Write the reading to the CSV
                    writer.writerow({
                        'reading_id': reading_id,
                        'sensor_id': sensor['sensor_id'],
                        'timestamp': timestamp,
                        'value': value,
                        'quality_indicator': quality_indicator,
                        'status_code': status_code,
                        'batch_id': batch_id,
                        'equipment_state_id': equipment_state_id
                    })
                    
                    readings_count += 1
                    if readings_count % 100000 == 0:
                        print(f"  Generated {readings_count:,} readings so far...")
        
        print(f"Successfully generated {readings_count:,} sensor readings.")
        print(f"Data saved to {output_file}")
        
        # Return a sample of the data (first 5 rows) for preview
        return pd.read_csv(output_file, nrows=5)
    
    def generate_actuator_commands(self, num_commands_per_actuator=100, start_time=None, end_time=None):
        """
        Generate synthetic actuator commands data based on the actuators table.
        
        Parameters:
        - num_commands_per_actuator: Number of commands to generate per actuator
        - start_time: Start time for commands (defaults to 7 days ago)
        - end_time: End time for commands (defaults to now)
        
        Returns:
        - Sample DataFrame containing a subset of the generated commands data
        """
        if self.actuators_df is None or len(self.actuators_df) == 0:
            print("Error: No actuator data available. Generate actuators first.")
            return None
        
        # Set default time range if not provided
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        # Command types and control modes
        command_types = ["position", "speed", "open", "close", "start", "stop", "setpoint", "reset"]
        control_modes = ["Auto", "Manual", "Cascade", "Supervised"]
        
        # Control mode weights (Auto is most common)
        control_mode_weights = [0.7, 0.2, 0.05, 0.05]
        
        # Calculate total number of commands
        total_commands = len(self.actuators_df) * num_commands_per_actuator
        
        # Prepare the output file with CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "actuator_commands.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'command_id', 'actuator_id', 'timestamp', 'command_value', 
                'command_type', 'control_mode', 'operator_id', 'batch_id', 'step_id'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            commands_count = 0
            
            # Process each actuator
            for _, actuator in self.actuators_df.iterrows():
                # Get the actuator's control range
                min_val = actuator['control_range_min']
                max_val = actuator['control_range_max']
                actuator_type = actuator['actuator_type']
                
                # Determine appropriate command types for this actuator type
                if actuator_type in ['valve', 'damper']:
                    specific_commands = ["open", "close", "position"]
                elif actuator_type in ['motor', 'pump', 'fan', 'agitator', 'conveyor', 'mixer', 'blower']:
                    specific_commands = ["start", "stop", "speed"]
                elif actuator_type in ['heater', 'compressor']:
                    specific_commands = ["start", "stop", "setpoint"]
                elif actuator_type in ['cylinder', 'positioner']:
                    specific_commands = ["position", "reset"]
                elif actuator_type in ['relay', 'switch']:
                    specific_commands = ["open", "close"]
                else:
                    specific_commands = command_types
                
                # Generate timestamps within the specified range
                time_points = sorted([
                    start_time + (end_time - start_time) * random.random()
                    for _ in range(num_commands_per_actuator)
                ])
                
                # Generate commands for this actuator
                for i in range(num_commands_per_actuator):
                    # Create a unique command ID
                    command_id = f"CMD-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Format timestamp
                    timestamp = time_points[i].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    
                    # Select command type
                    command_type = random.choice(specific_commands)
                    
                    # Determine command value based on command type and actuator type
                    if command_type == "open" or command_type == "start":
                        # For binary/open commands, use max value
                        command_value = max_val
                    elif command_type == "close" or command_type == "stop":
                        # For binary/close commands, use min value
                        command_value = min_val
                    elif command_type == "reset":
                        # Reset to a default position (usually 0 or middle)
                        if min_val == 0 and max_val == 1:  # Binary
                            command_value = 0
                        else:
                            command_value = min_val + (max_val - min_val) * 0.1  # 10% position
                    else:
                        # For setpoints and positions, use a value within range
                        # Create clustering around common setpoints (e.g., 0%, 25%, 50%, 75%, 100%)
                        if random.random() < 0.6:  # 60% chance of common setpoint
                            common_points = [
                                min_val,
                                min_val + (max_val - min_val) * 0.25,
                                min_val + (max_val - min_val) * 0.5,
                                min_val + (max_val - min_val) * 0.75,
                                max_val
                            ]
                            command_value = random.choice(common_points)
                        else:
                            # Random value within range
                            command_value = min_val + random.random() * (max_val - min_val)
                    
                    # Round command value appropriately
                    if min_val == 0 and max_val == 1:  # Binary actuator
                        command_value = round(command_value)
                    else:
                        command_value = round(command_value, 2)
                    
                    # Determine control mode (weighted selection)
                    control_mode = random.choices(control_modes, weights=control_mode_weights)[0]
                    
                    # Only include operator ID for manual control mode
                    if control_mode == "Manual":
                        operator_id = random.choice(self.operator_ids)
                    else:
                        operator_id = ""
                    
                    # Batch ID and Step ID (some commands may not be associated with a batch/step)
                    if random.random() < 0.8:  # 80% chance of having a batch
                        batch_id = random.choice(self.batch_ids)
                        # If there's a batch, high chance of having a step
                        if random.random() < 0.7:  # 70% chance of having a step if there's a batch
                            step_id = random.choice(self.step_ids)
                        else:
                            step_id = ""
                    else:
                        batch_id = ""
                        step_id = ""
                    
                    # Write the command to the CSV
                    writer.writerow({
                        'command_id': command_id,
                        'actuator_id': actuator['actuator_id'],
                        'timestamp': timestamp,
                        'command_value': command_value,
                        'command_type': command_type,
                        'control_mode': control_mode,
                        'operator_id': operator_id,
                        'batch_id': batch_id,
                        'step_id': step_id
                    })
                    
                    commands_count += 1
                    if commands_count % 10000 == 0:
                        print(f"  Generated {commands_count:,} commands so far...")
        
        print(f"Successfully generated {commands_count:,} actuator commands.")
        print(f"Data saved to {output_file}")
        
        # Return a sample of the data (first 5 rows) for preview
        return pd.read_csv(output_file, nrows=5)
    
    def generate_device_diagnostics(self, start_time=None, end_time=None, diagnostics_per_device=10):
        """
        Generate synthetic device diagnostics data for both sensors and actuators.
        
        Parameters:
        - start_time: Start time for diagnostics (defaults to 30 days ago)
        - end_time: End time for diagnostics (defaults to now)
        - diagnostics_per_device: Number of diagnostic records per device
        
        Returns:
        - Sample DataFrame containing a subset of the generated diagnostics data
        """
        if self.sensors_df is None or self.actuators_df is None:
            print("Error: Both sensor and actuator data are required. Generate these first.")
            return None
        
        # Set default time range if not provided (diagnostic data typically spans longer time than readings)
        if start_time is None:
            start_time = datetime.now() - timedelta(days=30)
        if end_time is None:
            end_time = datetime.now()
        
        # Combine sensor and actuator data into a unified devices list
        devices = []
        
        # Add sensors
        for _, sensor in self.sensors_df.iterrows():
            devices.append({
                'device_id': sensor['sensor_id'],
                'device_type': 'sensor',
                'device_subtype': sensor['sensor_type'],
                'equipment_id': sensor['equipment_id'],
                'status': sensor['status']
            })
        
        # Add actuators
        for _, actuator in self.actuators_df.iterrows():
            devices.append({
                'device_id': actuator['actuator_id'],
                'device_type': 'actuator',
                'device_subtype': actuator['actuator_type'],
                'equipment_id': actuator['equipment_id'],
                'status': actuator['status']
            })
        
        # Define diagnostic types
        diagnostic_types = {
            'sensor': [
                'Calibration Check', 'Signal Quality Test', 'Range Verification', 
                'Response Time Test', 'Interference Check', 'Power Supply Test',
                'Communication Test', 'Self-Diagnostic', 'Drift Analysis'
            ],
            'actuator': [
                'Movement Test', 'Response Time Test', 'Leak Test', 'Position Verification', 
                'Torque Test', 'Speed Test', 'Current Draw Test', 'Self-Diagnostic',
                'Feedback Verification', 'Lubrication Check', 'Wear Analysis'
            ]
        }
        
        # Define status code messages
        status_code_messages = {
            0: ["Normal operation", "No issues detected", "All parameters within normal range", 
                "Device functioning correctly", "Diagnostic passed"],
            1: ["Minor deviation detected", "Parameter near warning threshold", "Slight performance degradation", 
                "Recommend monitoring", "Non-critical warning"],
            2: ["Parameter outside optimal range", "Performance degradation detected", "Maintenance recommended", 
                "Minor issue detected", "Device requires attention"],
            3: ["Significant deviation detected", "Multiple parameters out of range", "Performance significantly degraded", 
                "Maintenance required soon", "Device operating outside specifications"],
            4: ["Major issue detected", "Device may fail soon", "Immediate maintenance required", 
                "Performance severely degraded", "Reliability compromised"],
            5: ["Critical failure detected", "Device non-operational", "Emergency maintenance required", 
                "Safety risk possible", "Replace device immediately"]
        }
        
        # Calculate total number of diagnostics
        total_diagnostics = len(devices) * diagnostics_per_device
        
        # Prepare the output file with CSV writer for memory efficiency
        output_file = os.path.join(self.output_dir, "device_diagnostics.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'diagnostic_id', 'device_id', 'timestamp', 'diagnostic_type', 
                'status_code', 'diagnostic_message', 'severity_level', 
                'battery_level', 'communication_quality', 'internal_temperature',
                'maintenance_required'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            print(f"Generating {total_diagnostics:,} device diagnostic records...")
            diagnostics_count = 0
            
            # Process each device
            for device in devices:
                device_id = device['device_id']
                device_type = device['device_type']
                device_subtype = device['device_subtype']
                device_status = device['status']
                
                # Get applicable diagnostic types for this device type
                applicable_diagnostics = diagnostic_types.get(device_type, diagnostic_types['sensor'])
                
                # Generate timestamps within the specified range (more spread out than readings)
                time_points = sorted([
                    start_time + (end_time - start_time) * random.random()
                    for _ in range(diagnostics_per_device)
                ])
                
                # Current device state (will evolve over time to simulate deterioration)
                device_health = 100.0  # Start at 100% health
                
                # Rate of deterioration (different for each device)
                deterioration_rate = random.uniform(0.1, 2.0)
                
                # Generate diagnostic records for this device
                for i in range(diagnostics_per_device):
                    # Create a unique diagnostic ID
                    diagnostic_id = f"DIAG-{uuid.uuid4().hex[:12].upper()}"
                    
                    # Format timestamp
                    timestamp = time_points[i].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    
                    # Determine diagnostic type
                    diagnostic_type = random.choice(applicable_diagnostics)
                    
                    # Simulate device health deterioration over time
                    # Devices in "Maintenance" or "Fault" status deteriorate faster
                    if device_status in ["Maintenance", "Fault"]:
                        deterioration_factor = 2.0
                    else:
                        deterioration_factor = 1.0
                    
                    # Calculate position in time series (0 to 1)
                    time_position = i / (diagnostics_per_device - 1) if diagnostics_per_device > 1 else 0
                    
                    # Decrease health over time with some randomness
                    health_decrease = deterioration_rate * deterioration_factor * time_position
                    health_decrease += random.uniform(-0.5, 0.5) * health_decrease  # Add noise
                    device_health -= health_decrease
                    device_health = max(50, device_health)  # Don't let it go below 50%
                    
                    # Determine severity based on device health and some randomness
                    if device_health > 95:
                        severity_candidates = [0]
                    elif device_health > 90:
                        severity_candidates = [0, 0, 0, 1]
                    elif device_health > 80:
                        severity_candidates = [0, 0, 1, 1, 2]
                    elif device_health > 70:
                        severity_candidates = [0, 1, 1, 2, 2]
                    elif device_health > 60:
                        severity_candidates = [1, 2, 2, 3, 3]
                    else:
                        severity_candidates = [2, 3, 3, 4, 4, 5]
                    
                    severity_level = random.choice(severity_candidates)
                    
                    # Set status code based on severity
                    status_code = severity_level
                    
                    # Get appropriate diagnostic message
                    diagnostic_message = random.choice(status_code_messages[status_code])
                    
                    # Generate battery level (only for wireless devices)
                    has_battery = random.random() < 0.3  # 30% of devices are wireless
                    if has_battery:
                        # Battery level decreases over time, with some recharges
                        base_battery = 100 - 50 * time_position  # Decreases from 100% to 50%
                        # Occasionally "recharge" the battery
                        if random.random() < 0.2:  # 20% chance of recent recharge
                            base_battery = min(100, base_battery + random.uniform(30, 80))
                        
                        battery_level = round(base_battery + random.uniform(-10, 10), 1)  # Add some noise
                        battery_level = max(5, min(100, battery_level))  # Keep between 5% and 100%
                    else:
                        battery_level = None
                    
                    # Generate communication quality
                    base_comm_quality = 100 - 20 * time_position  # Slight degradation over time
                    # Add random fluctuations
                    comm_quality = round(base_comm_quality + random.uniform(-15, 5), 1)
                    comm_quality = max(60, min(100, comm_quality))  # Keep between 60% and 100%
                    
                    # Generate internal temperature
                    if device_type == 'sensor':
                        # Sensors typically run cooler
                        base_temp = 25 + 5 * time_position  # Gradual increase over time
                        temp_variation = random.uniform(-3, 5)
                    else:
                        # Actuators often run hotter
                        base_temp = 30 + 8 * time_position  # More significant increase over time
                        temp_variation = random.uniform(-3, 8)
                    
                    internal_temperature = round(base_temp + temp_variation, 1)
                    
                    # Determine if maintenance is required
                    maintenance_required = severity_level >= 3  # Levels 3, 4, 5 require maintenance
                    
                    # Write the diagnostic record to the CSV
                    writer.writerow({
                        'diagnostic_id': diagnostic_id,
                        'device_id': device_id,
                        'timestamp': timestamp,
                        'diagnostic_type': diagnostic_type,
                        'status_code': status_code,
                        'diagnostic_message': diagnostic_message,
                        'severity_level': severity_level,
                        'battery_level': battery_level if has_battery else '',
                        'communication_quality': comm_quality,
                        'internal_temperature': internal_temperature,
                        'maintenance_required': 1 if maintenance_required else 0
                    })
                    
                    diagnostics_count += 1
                    if diagnostics_count % 10000 == 0:
                        print(f"  Generated {diagnostics_count:,} diagnostic records so far...")
        
        print(f"Successfully generated {diagnostics_count:,} device diagnostic records.")
        print(f"Data saved to {output_file}")
        
        # Return a sample of the data (first 5 rows) for preview
        return pd.read_csv(output_file, nrows=5)
    
    def generate_control_loops(self, num_loops=50):
        """
        Generate synthetic control loops data based on the sensors and actuators tables.
        
        Parameters:
        - num_loops: Number of control loops to generate
        
        Returns:
        - DataFrame containing the generated control loops data
        """
        if self.sensors_df is None or self.actuators_df is None:
            print("Error: Both sensor and actuator data are required. Generate these first.")
            return None
        
        # Filter sensors by type (only certain sensor types are used for process variables)
        pv_sensor_types = ['temperature', 'pressure', 'flow', 'level', 'ph', 'conductivity', 
                         'speed', 'position', 'weight', 'humidity', 'oxygen', 'co2']
        
        pv_sensors_df = self.sensors_df[self.sensors_df['sensor_type'].isin(pv_sensor_types)]
        
        if len(pv_sensors_df) == 0:
            print("Warning: No suitable process variable sensors found. Using all sensors instead.")
            pv_sensors_df = self.sensors_df
        
        # Filter actuators by type (only certain actuator types are used for control outputs)
        cv_actuator_types = ['valve', 'motor', 'pump', 'heater', 'fan', 'agitator', 
                           'damper', 'positioner', 'doser', 'compressor']
        
        cv_actuators_df = self.actuators_df[self.actuators_df['actuator_type'].isin(cv_actuator_types)]
        
        if len(cv_actuators_df) == 0:
            print("Warning: No suitable control variable actuators found. Using all actuators instead.")
            cv_actuators_df = self.actuators_df
        
        # Make sure we have enough sensors and actuators
        num_loops = min(num_loops, len(pv_sensors_df), len(cv_actuators_df))
        
        if num_loops == 0:
            print("Error: Not enough sensors and actuators to create control loops.")
            return None
        
        print(f"Generating {num_loops} control loops...")
        
        # Define controller types and their probabilities
        controller_types = {
            "PID": 0.6,        # Most common
            "Cascade": 0.15,
            "Feedforward": 0.1,
            "On-Off": 0.05,
            "Ratio": 0.05,
            "Model Predictive": 0.03,
            "Fuzzy Logic": 0.02
        }
        
        # Define control modes and their probabilities
        control_modes = {
            "Auto": 0.7,       # Most common
            "Manual": 0.15,
            "Cascade": 0.1,
            "Supervised": 0.05
        }
        
        # Create data structure
        data = {
            'loop_id': [f"LOOP-{uuid.uuid4().hex[:8].upper()}" for _ in range(num_loops)],
            'loop_name': [],
            'process_variable_sensor_id': [],
            'control_output_actuator_id': [],
            'controller_type': [],
            'control_mode': [],
            'setpoint_value': [],
            'setpoint_unit': [],
            'p_value': [],
            'i_value': [],
            'd_value': [],
            'equipment_id': [],
            'status': []
        }
        
        # Sample sensors and actuators without replacement to ensure uniqueness
        sampled_sensors = pv_sensors_df.sample(n=num_loops).reset_index(drop=True)
        sampled_actuators = cv_actuators_df.sample(n=num_loops).reset_index(drop=True)
        
        # Create logical pairings based on sensor and actuator types
        pairings = []
        
        # First, try to pair sensors and actuators on the same equipment
        for i in range(num_loops):
            sensor = sampled_sensors.iloc[i]
            
            # Find actuators on the same equipment as this sensor
            same_equipment_actuators = sampled_actuators[sampled_actuators['equipment_id'] == sensor['equipment_id']]
            
            if len(same_equipment_actuators) > 0:
                # Pick a random actuator from the same equipment
                actuator = same_equipment_actuators.sample(1).iloc[0]
                equipment_id = sensor['equipment_id']
            else:
                # If no matching actuator on same equipment, just pick a random one
                actuator = sampled_actuators.iloc[i]
                
                # Use one of their equipment IDs (prefer the sensor's)
                if random.random() < 0.7:
                    equipment_id = sensor['equipment_id']
                else:
                    equipment_id = actuator['equipment_id']
            
            pairings.append((sensor, actuator, equipment_id))
        
        # Create appropriate loop names and parameter values
        for i, (sensor, actuator, equipment_id) in enumerate(pairings):
            # Create a descriptive loop name based on what it controls
            sensor_type = sensor['sensor_type']
            actuator_type = actuator['actuator_type']
            
            # Create logical loop name
            if sensor_type == 'temperature' and actuator_type in ['heater', 'valve']:
                loop_name = f"Temperature Control {i+1}"
            elif sensor_type == 'flow' and actuator_type in ['valve', 'pump']:
                loop_name = f"Flow Control {i+1}"
            elif sensor_type == 'pressure' and actuator_type in ['valve', 'compressor']:
                loop_name = f"Pressure Control {i+1}"
            elif sensor_type == 'level' and actuator_type in ['valve', 'pump']:
                loop_name = f"Level Control {i+1}"
            elif sensor_type == 'ph' and actuator_type in ['pump', 'valve', 'doser']:
                loop_name = f"pH Control {i+1}"
            elif sensor_type == 'speed' and actuator_type in ['motor', 'fan']:
                loop_name = f"Speed Control {i+1}"
            elif sensor_type == 'position' and actuator_type in ['positioner', 'motor']:
                loop_name = f"Position Control {i+1}"
            else:
                loop_name = f"{sensor_type.capitalize()}-{actuator_type.capitalize()} Control {i+1}"
            
            data['loop_name'].append(loop_name)
            data['process_variable_sensor_id'].append(sensor['sensor_id'])
            data['control_output_actuator_id'].append(actuator['actuator_id'])
            
            # Select controller type (weighted random)
            data['controller_type'].append(
                random.choices(list(controller_types.keys()), 
                             weights=list(controller_types.values()))[0]
            )
            
            # Select control mode (weighted random)
            data['control_mode'].append(
                random.choices(list(control_modes.keys()), 
                             weights=list(control_modes.values()))[0]
            )
            
            # Set appropriate setpoint based on sensor type
            sensor_unit = sensor['measurement_unit']
            sensor_range_min = sensor['measurement_range_min']
            sensor_range_max = sensor['measurement_range_max']
            
            # Set setpoint somewhere in the middle of the range
            setpoint = sensor_range_min + (sensor_range_max - sensor_range_min) * random.uniform(0.3, 0.7)
            data['setpoint_value'].append(round(setpoint, 2))
            data['setpoint_unit'].append(sensor_unit)
            
            # Set PID parameters based on controller type
            controller_type = data['controller_type'][-1]
            
            if controller_type == "PID":
                # Standard PID values
                p_value = round(random.uniform(0.5, 10.0), 2)
                i_value = round(random.uniform(0.05, 2.0), 3)
                d_value = round(random.uniform(0, 0.5), 3)
            elif controller_type == "On-Off":
                # On-Off controllers don't use P, I, D in the same way
                p_value = 1.0  # Just binary
                i_value = 0.0
                d_value = 0.0
            else:
                # Other controller types with some variation
                p_value = round(random.uniform(0.1, 20.0), 2)
                i_value = round(random.uniform(0, 5.0), 3)
                d_value = round(random.uniform(0, 2.0), 3)
            
            data['p_value'].append(p_value)
            data['i_value'].append(i_value)
            data['d_value'].append(d_value)
            
            # Set equipment and status
            data['equipment_id'].append(equipment_id)
            
            # Status (mostly active)
            statuses = ["Active", "Tuning", "Inactive", "Fault"]
            weights = [0.85, 0.05, 0.07, 0.03]  # Mostly active
            data['status'].append(random.choices(statuses, weights=weights)[0])
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = os.path.join(self.output_dir, "control_loops.csv")
        df.to_csv(output_file, index=False)
        
        # Store for later use
        self.control_loops_df = df
        
        print(f"Successfully generated {num_loops} control loops.")
        print(f"Data saved to {output_file}")
        print(f"Sample data (first 3 records):")
        print(df.head(3))
        
        return df

def main():
    """Main function to run the data generator"""
    parser = argparse.ArgumentParser(description='Generate ISA-95 Level 1 data')
    parser.add_argument('--output', type=str, default='data', 
                      help='Output directory for generated data (default: data)')
    parser.add_argument('--sensors', type=int, default=100, 
                      help='Number of sensors to generate (default: 100)')
    parser.add_argument('--actuators', type=int, default=100, 
                      help='Number of actuators to generate (default: 100)')
    parser.add_argument('--readings', type=int, default=1000, 
                      help='Number of readings per sensor (default: 1000)')
    parser.add_argument('--commands', type=int, default=100, 
                      help='Number of commands per actuator (default: 100)')
    parser.add_argument('--loops', type=int, default=50, 
                      help='Number of control loops (default: 50)')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = ISA95Level1DataGenerator(output_dir=args.output)
    
    # Start timer
    start_time = time.time()
    
    # Generate all data
    generator.generate_all_data(
        num_sensors=args.sensors,
        num_actuators=args.actuators,
        readings_per_sensor=args.readings,
        commands_per_actuator=args.commands,
        num_control_loops=args.loops
    )
    
    
    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\nTotal generation time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()