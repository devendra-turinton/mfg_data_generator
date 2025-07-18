import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')

# Function to load all datasets
def load_all_data():
    """Load all available ISA-95 Level 1 datasets and return a dictionary of dataframes"""
    data_path = "data/"
    datasets = {}
    
    # List of all potential Level 1 datasets with their actual filenames
    # Try both naming conventions (with or without _data suffix)
    dataset_files = {
        "sensors": ["sensors.csv"],
        "sensor_readings": ["sensor_readings.csv"],
        "actuators": ["actuators.csv"],
        "actuator_commands": ["actuator_commands.csv"], 
        "device_diagnostics": ["device_diagnostics.csv"],
        "control_loops": ["control_loops.csv"]
    }
    
    # Load each dataset if it exists
    for dataset_name, filenames in dataset_files.items():
        # Try each possible filename
        for filename in filenames:
            file_path = os.path.join(data_path, filename)
            if os.path.exists(file_path):
                try:
                    # Load the dataset
                    df = pd.read_csv(file_path)
                    
                    # Convert date columns to datetime
                    for col in df.columns:
                        if 'date' in col.lower() or 'time' in col.lower() or col == 'timestamp':
                            try:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                            except:
                                pass  # Skip if conversion fails
                    
                    # Store in dictionary
                    datasets[dataset_name] = df
                    print(f"Loaded {dataset_name} with {len(df)} records and {len(df.columns)} columns")
                    print(f"Columns: {', '.join(df.columns)}")
                    # Break once we've found a valid file for this dataset
                    break
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
        else:  # This else belongs to the for loop, executed if no break occurred
            print(f"No valid file found for {dataset_name}")
    
    return datasets

# Calculate key metrics for dashboard
def calculate_metrics(datasets):
    """Calculate key metrics from the datasets for the dashboard"""
    metrics = {}
    
    # Track metrics calculation for debugging
    calculation_count = 0
    
    try:
        # Rest of function remains the same...
        # 1. Sensor Metrics
        if 'sensors' in datasets:
            sensors = datasets['sensors']
            
            # Total number of sensors
            total_sensors = len(sensors)
            metrics['total_sensors'] = total_sensors
            print(f"Total sensors: {total_sensors}")
            calculation_count += 1
            
            # Count sensors by type
            if 'sensor_type' in sensors.columns:
                sensor_types = sensors['sensor_type'].value_counts().reset_index()
                sensor_types.columns = ['type', 'count']
                metrics['sensor_types'] = sensor_types
                print(f"Sensor types: {len(sensor_types)} types found")
                calculation_count += 1
            
            # Count sensors by status
            if 'status' in sensors.columns:
                sensor_status = sensors['status'].value_counts().reset_index()
                sensor_status.columns = ['status', 'count']
                metrics['sensor_status'] = sensor_status
                print(f"Sensor statuses: {', '.join(sensor_status['status'].unique())}")
                calculation_count += 1
                
                # Calculate sensor operational rate
                operational_statuses = ['Active', 'Running', 'Online', 'Operational']
                operational_sensors = sensors[sensors['status'].isin(operational_statuses)].shape[0]
                operational_rate = operational_sensors / total_sensors * 100 if total_sensors > 0 else 0
                metrics['sensor_operational_rate'] = operational_rate
                print(f"Sensor operational rate: {operational_rate:.2f}%")
                calculation_count += 1
            
            # Count sensors by measurement unit
            if 'measurement_unit' in sensors.columns:
                unit_counts = sensors['measurement_unit'].value_counts().reset_index()
                unit_counts.columns = ['unit', 'count']
                metrics['sensor_units'] = unit_counts
                calculation_count += 1
            
            # Sensor calibration status
            if 'calibration_due_date' in sensors.columns:
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_dtype(sensors['calibration_due_date']):
                    sensors['calibration_due_date'] = pd.to_datetime(sensors['calibration_due_date'], errors='coerce')
                
                # Count sensors past calibration due date
                past_due = sensors[sensors['calibration_due_date'] < datetime.now()].shape[0]
                metrics['sensors_past_calibration'] = past_due
                print(f"Sensors past calibration: {past_due}")
                calculation_count += 1
                
                # Calibration due in next 30 days
                due_soon = sensors[(sensors['calibration_due_date'] >= datetime.now()) & 
                                 (sensors['calibration_due_date'] <= datetime.now() + timedelta(days=30))].shape[0]
                metrics['sensors_due_calibration_soon'] = due_soon
                print(f"Sensors due calibration soon: {due_soon}")
                calculation_count += 1
        
        # Continue with remaining calculations...
        # (The rest of the function remains unchanged)
        
        # 2. Sensor Reading Metrics
        if 'sensor_readings' in datasets:
            readings = datasets['sensor_readings']
            
            # Total number of readings
            total_readings = len(readings)
            metrics['total_readings'] = total_readings
            print(f"Total readings: {total_readings}")
            calculation_count += 1
            
            # Reading statistics
            if 'value' in readings.columns:
                readings['value_num'] = pd.to_numeric(readings['value'], errors='coerce')
                
                reading_stats = {
                    'min_reading': readings['value_num'].min(),
                    'max_reading': readings['value_num'].max(),
                    'avg_reading': readings['value_num'].mean(),
                    'median_reading': readings['value_num'].median()
                }
                metrics.update(reading_stats)
                print(f"Reading stats - Min: {reading_stats['min_reading']}, Max: {reading_stats['max_reading']}, Avg: {reading_stats['avg_reading']:.2f}")
                calculation_count += 1
            
            # Reading quality metrics
            if 'quality_indicator' in readings.columns:
                readings['quality_num'] = pd.to_numeric(readings['quality_indicator'], errors='coerce')
                
                quality_stats = {
                    'min_quality': readings['quality_num'].min(),
                    'max_quality': readings['quality_num'].max(),
                    'avg_quality': readings['quality_num'].mean()
                }
                metrics.update(quality_stats)
                print(f"Quality stats - Min: {quality_stats['min_quality']}, Max: {quality_stats['max_quality']}, Avg: {quality_stats['avg_quality']:.2f}")
                calculation_count += 1
                
                # Quality distribution
                def quality_category(quality):
                    if quality >= 90:
                        return 'High (90-100%)'
                    elif quality >= 70:
                        return 'Medium (70-90%)'
                    else:
                        return 'Low (<70%)'
                        
                readings['quality_category'] = readings['quality_num'].apply(quality_category)
                quality_dist = readings['quality_category'].value_counts().reset_index()
                quality_dist.columns = ['category', 'count']
                metrics['quality_distribution'] = quality_dist
                calculation_count += 1
            
            # Reading status code analysis
            if 'status_code' in readings.columns:
                readings['status_code_num'] = pd.to_numeric(readings['status_code'], errors='coerce')
                status_counts = readings['status_code_num'].value_counts().reset_index()
                status_counts.columns = ['code', 'count']
                metrics['reading_status_codes'] = status_counts
                calculation_count += 1
                
                # Count of error readings
                error_codes = [1, 2, 3, 4, 5]  # Assuming these are error codes
                error_readings = readings[readings['status_code_num'].isin(error_codes)].shape[0]
                error_rate = error_readings / total_readings * 100 if total_readings > 0 else 0
                metrics['reading_error_rate'] = error_rate
                print(f"Reading error rate: {error_rate:.2f}%")
                calculation_count += 1
            
            # Readings over time
            if 'timestamp' in readings.columns:
                # Ensure timestamp is datetime
                if not pd.api.types.is_datetime64_dtype(readings['timestamp']):
                    readings['timestamp'] = pd.to_datetime(readings['timestamp'], errors='coerce')
                    
                # Group by hour (using a sample of the data if it's very large)
                if len(readings) > 100000:
                    # Use a sample for large datasets to improve performance
                    sample_size = min(100000, int(len(readings) * 0.1))
                    sample_readings = readings.sample(sample_size)
                    sample_readings['hour'] = sample_readings['timestamp'].dt.floor('H')
                    hourly_readings = sample_readings.groupby('hour').agg(
                        avg_value=('value_num', 'mean'),
                        avg_quality=('quality_num', 'mean'),
                        count=('value_num', 'count')
                    ).reset_index()
                else:
                    readings['hour'] = readings['timestamp'].dt.floor('H')
                    hourly_readings = readings.groupby('hour').agg(
                        avg_value=('value_num', 'mean'),
                        avg_quality=('quality_num', 'mean'),
                        count=('value_num', 'count')
                    ).reset_index()
                
                metrics['hourly_readings'] = hourly_readings
                calculation_count += 1
            
            # Reading by sensor type
            if 'sensor_id' in readings.columns and 'sensors' in datasets:
                sensors = datasets['sensors']
                if 'sensor_id' in sensors.columns and 'sensor_type' in sensors.columns:
                    # Create mapping of sensor_id to sensor_type
                    sensor_type_map = dict(zip(sensors['sensor_id'], sensors['sensor_type']))
                    
                    # Use a sample for large datasets
                    if len(readings) > 100000:
                        sample_size = min(100000, int(len(readings) * 0.1))
                        sample_readings = readings.sample(sample_size)
                        # Add sensor type to readings
                        sample_readings['sensor_type'] = sample_readings['sensor_id'].map(sensor_type_map)
                        
                        # Group by sensor type
                        readings_by_type = sample_readings.groupby('sensor_type').agg(
                            avg_value=('value_num', 'mean'),
                            avg_quality=('quality_num', 'mean'),
                            count=('value_num', 'count')
                        ).reset_index()
                    else:
                        # Add sensor type to readings
                        readings['sensor_type'] = readings['sensor_id'].map(sensor_type_map)
                        
                        # Group by sensor type
                        readings_by_type = readings.groupby('sensor_type').agg(
                            avg_value=('value_num', 'mean'),
                            avg_quality=('quality_num', 'mean'),
                            count=('value_num', 'count')
                        ).reset_index()
                    
                    metrics['readings_by_sensor_type'] = readings_by_type
                    calculation_count += 1
        
        # Remaining metrics calculation continues as before...
        # ...
        
        # 3. Actuator Metrics
        if 'actuators' in datasets:
            actuators = datasets['actuators']
            
            # Total number of actuators
            total_actuators = len(actuators)
            metrics['total_actuators'] = total_actuators
            print(f"Total actuators: {total_actuators}")
            calculation_count += 1
            
            # Count actuators by type
            if 'actuator_type' in actuators.columns:
                actuator_types = actuators['actuator_type'].value_counts().reset_index()
                actuator_types.columns = ['type', 'count']
                metrics['actuator_types'] = actuator_types
                print(f"Actuator types: {len(actuator_types)} types found")
                calculation_count += 1
            
            # Count actuators by status
            if 'status' in actuators.columns:
                actuator_status = actuators['status'].value_counts().reset_index()
                actuator_status.columns = ['status', 'count']
                metrics['actuator_status'] = actuator_status
                print(f"Actuator statuses: {', '.join(actuator_status['status'].unique())}")
                calculation_count += 1
                
                # Calculate actuator operational rate
                operational_statuses = ['Active', 'Running', 'Online', 'Operational']
                operational_actuators = actuators[actuators['status'].isin(operational_statuses)].shape[0]
                operational_rate = operational_actuators / total_actuators * 100 if total_actuators > 0 else 0
                metrics['actuator_operational_rate'] = operational_rate
                print(f"Actuator operational rate: {operational_rate:.2f}%")
                calculation_count += 1
            
            # Count actuators by control unit
            if 'control_unit' in actuators.columns:
                unit_counts = actuators['control_unit'].value_counts().reset_index()
                unit_counts.columns = ['unit', 'count']
                metrics['actuator_control_units'] = unit_counts
                calculation_count += 1
        
        # 4. Actuator Command Metrics
        if 'actuator_commands' in datasets:
            commands = datasets['actuator_commands']
            
            # Total number of commands
            total_commands = len(commands)
            metrics['total_commands'] = total_commands
            print(f"Total commands: {total_commands}")
            calculation_count += 1
            
            # Command value statistics
            if 'command_value' in commands.columns:
                commands['value_num'] = pd.to_numeric(commands['command_value'], errors='coerce')
                
                command_stats = {
                    'min_command': commands['value_num'].min(),
                    'max_command': commands['value_num'].max(),
                    'avg_command': commands['value_num'].mean()
                }
                metrics.update(command_stats)
                print(f"Command stats - Min: {command_stats['min_command']}, Max: {command_stats['max_command']}, Avg: {command_stats['avg_command']:.2f}")
                calculation_count += 1
            
            # Command type distribution
            if 'command_type' in commands.columns:
                command_types = commands['command_type'].value_counts().reset_index()
                command_types.columns = ['type', 'count']
                metrics['command_types'] = command_types
                calculation_count += 1
            
            # Control mode distribution
            if 'control_mode' in commands.columns:
                control_modes = commands['control_mode'].value_counts().reset_index()
                control_modes.columns = ['mode', 'count']
                metrics['control_modes'] = control_modes
                calculation_count += 1
                
                # Manual command percentage
                manual_commands = commands[commands['control_mode'] == 'Manual'].shape[0]
                manual_rate = manual_commands / total_commands * 100 if total_commands > 0 else 0
                metrics['manual_command_rate'] = manual_rate
                print(f"Manual command rate: {manual_rate:.2f}%")
                calculation_count += 1
            
            # Commands over time
            if 'timestamp' in commands.columns:
                # Ensure timestamp is datetime
                if not pd.api.types.is_datetime64_dtype(commands['timestamp']):
                    commands['timestamp'] = pd.to_datetime(commands['timestamp'], errors='coerce')
                    
                # Group by hour (use a sample if it's very large)
                if len(commands) > 50000:
                    sample_size = min(50000, int(len(commands) * 0.1))
                    sample_commands = commands.sample(sample_size)
                    sample_commands['hour'] = sample_commands['timestamp'].dt.floor('H')
                    hourly_commands = sample_commands.groupby('hour').agg(
                        avg_value=('value_num', 'mean'),
                        count=('value_num', 'count')
                    ).reset_index()
                else:
                    commands['hour'] = commands['timestamp'].dt.floor('H')
                    hourly_commands = commands.groupby('hour').agg(
                        avg_value=('value_num', 'mean'),
                        count=('value_num', 'count')
                    ).reset_index()
                
                metrics['hourly_commands'] = hourly_commands
                calculation_count += 1
            
            # Commands by operator (for manual commands)
            if 'operator_id' in commands.columns:
                operator_commands = commands.dropna(subset=['operator_id']).groupby('operator_id').size().reset_index(name='count')
                metrics['commands_by_operator'] = operator_commands
                calculation_count += 1
        
        # 5. Device Diagnostic Metrics
        if 'device_diagnostics' in datasets:
            diagnostics = datasets['device_diagnostics']
            
            # Total number of diagnostics
            total_diagnostics = len(diagnostics)
            metrics['total_diagnostics'] = total_diagnostics
            print(f"Total diagnostics: {total_diagnostics}")
            calculation_count += 1
            
            # Diagnostic type distribution
            if 'diagnostic_type' in diagnostics.columns:
                diag_types = diagnostics['diagnostic_type'].value_counts().reset_index()
                diag_types.columns = ['type', 'count']
                metrics['diagnostic_types'] = diag_types
                calculation_count += 1
            
            # Severity level distribution
            if 'severity_level' in diagnostics.columns:
                diagnostics['severity_num'] = pd.to_numeric(diagnostics['severity_level'], errors='coerce')
                
                severity_counts = diagnostics.groupby('severity_num').size().reset_index(name='count')
                metrics['severity_distribution'] = severity_counts
                calculation_count += 1
                
                # High severity percentage
                high_severity = diagnostics[diagnostics['severity_num'] >= 3].shape[0]
                high_severity_rate = high_severity / total_diagnostics * 100 if total_diagnostics > 0 else 0
                metrics['high_severity_rate'] = high_severity_rate
                print(f"High severity rate: {high_severity_rate:.2f}%")
                calculation_count += 1
            
            # Maintenance required count
            if 'maintenance_required' in diagnostics.columns:
                # Convert to numeric if it's not already
                diagnostics['maintenance_required_num'] = pd.to_numeric(diagnostics['maintenance_required'], errors='coerce')
                maintenance_required = diagnostics[diagnostics['maintenance_required_num'] > 0].shape[0]
                maintenance_rate = maintenance_required / total_diagnostics * 100 if total_diagnostics > 0 else 0
                metrics['maintenance_required_rate'] = maintenance_rate
                print(f"Maintenance required rate: {maintenance_rate:.2f}%")
                calculation_count += 1
            
            # Communication quality statistics
            if 'communication_quality' in diagnostics.columns:
                diagnostics['comm_quality_num'] = pd.to_numeric(diagnostics['communication_quality'], errors='coerce')
                
                comm_stats = {
                    'min_comm_quality': diagnostics['comm_quality_num'].min(),
                    'max_comm_quality': diagnostics['comm_quality_num'].max(),
                    'avg_comm_quality': diagnostics['comm_quality_num'].mean()
                }
                metrics.update(comm_stats)
                calculation_count += 1
            
            # Internal temperature statistics
            if 'internal_temperature' in diagnostics.columns:
                diagnostics['temp_num'] = pd.to_numeric(diagnostics['internal_temperature'], errors='coerce')
                
                temp_stats = {
                    'min_internal_temp': diagnostics['temp_num'].min(),
                    'max_internal_temp': diagnostics['temp_num'].max(),
                    'avg_internal_temp': diagnostics['temp_num'].mean()
                }
                metrics.update(temp_stats)
                calculation_count += 1
        
        # 6. Control Loop Metrics
        if 'control_loops' in datasets:
            loops = datasets['control_loops']
            
            # Total number of control loops
            total_loops = len(loops)
            metrics['total_control_loops'] = total_loops
            print(f"Total control loops: {total_loops}")
            calculation_count += 1
            
            # Control loop type distribution
            if 'controller_type' in loops.columns:
                controller_types = loops['controller_type'].value_counts().reset_index()
                controller_types.columns = ['type', 'count']
                metrics['controller_types'] = controller_types
                calculation_count += 1
            
            # Control mode distribution
            if 'control_mode' in loops.columns:
                loop_modes = loops['control_mode'].value_counts().reset_index()
                loop_modes.columns = ['mode', 'count']
                metrics['loop_control_modes'] = loop_modes
                calculation_count += 1
                
                # Auto mode percentage
                auto_loops = loops[loops['control_mode'] == 'Auto'].shape[0]
                auto_rate = auto_loops / total_loops * 100 if total_loops > 0 else 0
                metrics['auto_control_rate'] = auto_rate
                print(f"Auto control rate: {auto_rate:.2f}%")
                calculation_count += 1
            
            # PID parameter statistics
            pid_params = ['p_value', 'i_value', 'd_value']
            for param in pid_params:
                if param in loops.columns:
                    loops[f'{param}_num'] = pd.to_numeric(loops[param], errors='coerce')
                    
                    param_stats = {
                        f'min_{param}': loops[f'{param}_num'].min(),
                        f'max_{param}': loops[f'{param}_num'].max(),
                        f'avg_{param}': loops[f'{param}_num'].mean()
                    }
                    metrics.update(param_stats)
                    calculation_count += 1
                    
            # Control loop status distribution
            if 'status' in loops.columns:
                loop_status = loops['status'].value_counts().reset_index()
                loop_status.columns = ['status', 'count']
                metrics['loop_status'] = loop_status
                calculation_count += 1
        
        print(f"Metrics calculated successfully. {calculation_count} metrics")
        
    except Exception as e:
        print(f"Error calculating metrics: {e}")
    
    return metrics

# Chart creation functions
def create_sensor_types_chart(metrics):
    """Create sensor types distribution chart"""
    if 'sensor_types' not in metrics or metrics['sensor_types'].empty:
        fig = go.Figure()
        fig.update_layout(
            title="Sensor Type Distribution (No Data)",
            height=400
        )
        return fig
    
    sensor_types = metrics['sensor_types']
    
    # For better visibility, limit to top 10 types if there are many
    if len(sensor_types) > 10:
        sensor_types = sensor_types.sort_values('count', ascending=False).head(10)
        title = "Top 10 Sensor Types"
    else:
        title = "Sensor Type Distribution"
    
    try:
        fig = px.pie(sensor_types, values='count', names='type',
                    color_discrete_sequence=px.colors.qualitative.Plotly)
        
        fig.update_layout(
            title=title,
            legend=dict(orientation="h", yanchor="bottom", y=-0.1),
            margin=dict(t=40, b=40, l=10, r=10),
            height=400
        )
    except Exception as e:
        print(f"Error creating sensor types chart: {e}")
        # Create a fallback figure
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=sensor_types['type'],
            y=sensor_types['count'],
            marker_color='royalblue'
        ))
        fig.update_layout(
            title=title,
            xaxis_title="Sensor Type",
            yaxis_title="Count",
            height=400
        )
    
    return fig

def create_sensor_status_chart(metrics):
    """Create sensor status distribution chart"""
    if 'sensor_status' not in metrics or metrics['sensor_status'].empty:
        fig = go.Figure()
        fig.update_layout(
            title="Sensor Status Distribution (No Data)",
            height=400
        )
        return fig
    
    sensor_status = metrics['sensor_status']
    
    # Define colors for different statuses
    status_colors = {
        'Active': '#2ca02c',       # Green
        'Running': '#2ca02c',      # Green
        'Online': '#2ca02c',       # Green
        'Operational': '#2ca02c',  # Green
        'Idle': '#1f77b4',         # Blue
        'Standby': '#1f77b4',      # Blue
        'Offline': '#d62728',      # Red
        'Fault': '#d62728',        # Red
        'Error': '#d62728',        # Red
        'Maintenance': '#ff7f0e',  # Orange
        'Calibration Due': '#ff7f0e', # Orange
        'Calibrating': '#ff7f0e',  # Orange
        'Unknown': '#7f7f7f'       # Gray
    }
    
    # Create color sequence based on statuses
    color_sequence = [status_colors.get(status, '#1f77b4') for status in sensor_status['status']]
    
    try:
        fig = px.bar(sensor_status, x='status', y='count',
                    color='status', color_discrete_sequence=color_sequence,
                    labels={'status': 'Status', 'count': 'Count'})
        
        fig.update_layout(
            title="Sensor Status Distribution",
            xaxis_title="Status",
            yaxis_title="Count",
            margin=dict(t=40, b=40, l=10, r=10),
            height=400,
            showlegend=False
        )
    except Exception as e:
        print(f"Error creating sensor status chart: {e}")
        # Create a fallback figure
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=sensor_status['status'],
            y=sensor_status['count']
        ))
        fig.update_layout(
            title="Sensor Status Distribution",
            xaxis_title="Status",
            yaxis_title="Count",
            height=400
        )
    
    return fig

def create_actuator_types_chart(metrics):
    """Create actuator types distribution chart"""
    if 'actuator_types' not in metrics or metrics['actuator_types'].empty:
        fig = go.Figure()
        fig.update_layout(
            title="Actuator Type Distribution (No Data)",
            height=400
        )
        return fig
    
    actuator_types = metrics['actuator_types']
    
    # For better visibility, limit to top 10 types if there are many
    if len(actuator_types) > 10:
        actuator_types = actuator_types.sort_values('count', ascending=False).head(10)
        title = "Top 10 Actuator Types"
    else:
        title = "Actuator Type Distribution"
    
    try:
        fig = px.pie(actuator_types, values='count', names='type',
                    color_discrete_sequence=px.colors.qualitative.Plotly)
        
        fig.update_layout(
            title=title,
            legend=dict(orientation="h", yanchor="bottom", y=-0.1),
            margin=dict(t=40, b=40, l=10, r=10),
            height=400
        )
    except Exception as e:
        print(f"Error creating actuator types chart: {e}")
        # Create a fallback figure
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=actuator_types['type'],
            y=actuator_types['count'],
            marker_color='royalblue'
        ))
        fig.update_layout(
            title=title,
            xaxis_title="Actuator Type",
            yaxis_title="Count",
            height=400
        )
    
    return fig

def create_actuator_status_chart(metrics):
    """Create actuator status distribution chart"""
    if 'actuator_status' not in metrics:
        return go.Figure()
    
    actuator_status = metrics['actuator_status']
    
    # Define colors for different statuses
    status_colors = {
        'Active': '#2ca02c',       # Green
        'Running': '#2ca02c',      # Green
        'Online': '#2ca02c',       # Green
        'Operational': '#2ca02c',  # Green
        'Idle': '#1f77b4',         # Blue
        'Standby': '#1f77b4',      # Blue
        'Offline': '#d62728',      # Red
        'Fault': '#d62728',        # Red
        'Error': '#d62728',        # Red
        'Maintenance': '#ff7f0e',  # Orange
        'Reserved': '#9467bd',     # Purple
        'Unknown': '#7f7f7f'       # Gray
    }
    
    # Create color sequence based on statuses
    color_sequence = [status_colors.get(status, '#1f77b4') for status in actuator_status['status']]
    
    fig = px.bar(actuator_status, x='status', y='count',
                 color='status', color_discrete_sequence=color_sequence,
                 labels={'status': 'Status', 'count': 'Count'})
    
    fig.update_layout(
        title="Actuator Status Distribution",
        xaxis_title="Status",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_reading_quality_chart(metrics):
    """Create reading quality distribution chart"""
    if 'quality_distribution' not in metrics:
        return go.Figure()
    
    quality_dist = metrics['quality_distribution']
    
    # Define colors for different quality categories
    colors = {
        'High (90-100%)': '#2ca02c',  # Green
        'Medium (70-90%)': '#ff7f0e',  # Orange
        'Low (<70%)': '#d62728'        # Red
    }
    
    # Ensure the categories are ordered correctly
    category_order = ['High (90-100%)', 'Medium (70-90%)', 'Low (<70%)']
    quality_dist = quality_dist.set_index('category').reindex(category_order).reset_index()
    
    # Create color sequence based on categories
    color_sequence = [colors.get(cat, '#1f77b4') for cat in quality_dist['category']]
    
    fig = px.bar(quality_dist, x='category', y='count',
                 color='category', color_discrete_sequence=color_sequence,
                 labels={'category': 'Quality Category', 'count': 'Count'})
    
    fig.update_layout(
        title="Sensor Reading Quality Distribution",
        xaxis_title="Quality Category",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_readings_by_type_chart(metrics):
    """Create readings by sensor type chart"""
    if 'readings_by_sensor_type' not in metrics or metrics['readings_by_sensor_type'].empty:
        fig = go.Figure()
        fig.update_layout(
            title="Readings by Sensor Type (No Data)",
            height=400
        )
        return fig
    
    readings_by_type = metrics['readings_by_sensor_type']
    
    # For better visibility, limit to top 10 types if there are many
    if len(readings_by_type) > 10:
        readings_by_type = readings_by_type.sort_values('count', ascending=False).head(10)
        title = "Top 10 Sensor Types by Reading Count"
    else:
        title = "Readings by Sensor Type"
    
    try:
        fig = px.bar(readings_by_type, x='sensor_type', y='count',
                    color='sensor_type', color_discrete_sequence=px.colors.qualitative.Plotly,
                    labels={'sensor_type': 'Sensor Type', 'count': 'Reading Count'})
        
        fig.update_layout(
            title=title,
            xaxis_title="Sensor Type",
            yaxis_title="Reading Count",
            margin=dict(t=40, b=40, l=10, r=10),
            height=400,
            showlegend=False
        )
    except Exception as e:
        print(f"Error creating readings by type chart: {e}")
        # Create a fallback figure
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=readings_by_type['sensor_type'],
            y=readings_by_type['count'],
            marker_color='royalblue'
        ))
        fig.update_layout(
            title=title,
            xaxis_title="Sensor Type",
            yaxis_title="Reading Count",
            height=400
        )
    
    return fig

def create_control_modes_chart(metrics):
    """Create control mode distribution chart"""
    if 'control_modes' not in metrics:
        return go.Figure()
    
    control_modes = metrics['control_modes']
    
    # Define colors for different modes
    mode_colors = {
        'Auto': '#2ca02c',       # Green
        'Manual': '#d62728',     # Red
        'Cascade': '#ff7f0e',    # Orange
        'Remote': '#1f77b4',     # Blue
        'Supervised': '#9467bd'  # Purple
    }
    
    # Create color sequence based on modes
    color_sequence = [mode_colors.get(mode, '#1f77b4') for mode in control_modes['mode']]
    
    fig = px.pie(control_modes, values='count', names='mode',
                 color='mode', color_discrete_sequence=color_sequence)
    
    fig.update_layout(
        title="Control Mode Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_command_types_chart(metrics):
    """Create command types distribution chart"""
    if 'command_types' not in metrics:
        return go.Figure()
    
    command_types = metrics['command_types']
    
    fig = px.bar(command_types, x='type', y='count',
                 color='type', color_discrete_sequence=px.colors.qualitative.Plotly,
                 labels={'type': 'Command Type', 'count': 'Count'})
    
    fig.update_layout(
        title="Command Type Distribution",
        xaxis_title="Command Type",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_hourly_readings_chart(metrics):
    """Create hourly readings chart"""
    if 'hourly_readings' not in metrics:
        return go.Figure()
    
    hourly_readings = metrics['hourly_readings']
    
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add traces
    fig.add_trace(
        go.Scatter(x=hourly_readings['hour'], y=hourly_readings['avg_value'],
                  name="Average Value", line=dict(color='#1f77b4')),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Scatter(x=hourly_readings['hour'], y=hourly_readings['avg_quality'],
                  name="Quality (%)", line=dict(color='#2ca02c')),
        secondary_y=True,
    )
    
    # Add figure title
    fig.update_layout(
        title="Sensor Readings Over Time",
        xaxis_title="Time",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2)
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text="Average Reading Value", secondary_y=False)
    fig.update_yaxes(title_text="Quality (%)", secondary_y=True)
    
    return fig

def create_hourly_commands_chart(metrics):
    """Create hourly commands chart"""
    if 'hourly_commands' not in metrics:
        return go.Figure()
    
    hourly_commands = metrics['hourly_commands']
    
    fig = px.bar(hourly_commands, x='hour', y='count',
                 labels={'hour': 'Time', 'count': 'Command Count'},
                 color_discrete_sequence=['#ff7f0e'])
    
    fig.update_layout(
        title="Actuator Commands Over Time",
        xaxis_title="Time",
        yaxis_title="Command Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_diagnostic_types_chart(metrics):
    """Create diagnostic types distribution chart"""
    if 'diagnostic_types' not in metrics:
        return go.Figure()
    
    diagnostic_types = metrics['diagnostic_types']
    
    fig = px.pie(diagnostic_types, values='count', names='type',
                 color_discrete_sequence=px.colors.qualitative.Plotly)
    
    fig.update_layout(
        title="Diagnostic Type Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_severity_distribution_chart(metrics):
    """Create severity distribution chart"""
    if 'severity_distribution' not in metrics:
        return go.Figure()
    
    severity_dist = metrics['severity_distribution']
    
    # Define colors based on severity level
    colors = []
    for level in severity_dist['severity_num']:
        if level >= 4:
            colors.append('#d62728')  # Red for high severity
        elif level == 3:
            colors.append('#ff7f0e')  # Orange for medium-high severity
        elif level == 2:
            colors.append('#ffbb78')  # Light orange for medium severity
        elif level == 1:
            colors.append('#aec7e8')  # Light blue for low severity
        else:
            colors.append('#1f77b4')  # Blue for no severity
    
    fig = px.bar(severity_dist, x='severity_num', y='count',
                 color='severity_num', color_discrete_sequence=colors,
                 labels={'severity_num': 'Severity Level', 'count': 'Count'})
    
    fig.update_layout(
        title="Diagnostic Severity Distribution",
        xaxis_title="Severity Level",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_controller_types_chart(metrics):
    """Create controller types distribution chart"""
    if 'controller_types' not in metrics:
        return go.Figure()
    
    controller_types = metrics['controller_types']
    
    fig = px.pie(controller_types, values='count', names='type',
                 color_discrete_sequence=px.colors.qualitative.Plotly)
    
    fig.update_layout(
        title="Controller Type Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_loop_modes_chart(metrics):
    """Create loop control modes distribution chart"""
    if 'loop_control_modes' not in metrics:
        return go.Figure()
    
    loop_modes = metrics['loop_control_modes']
    
    # Define colors for different modes
    mode_colors = {
        'Auto': '#2ca02c',       # Green
        'Manual': '#d62728',     # Red
        'Cascade': '#ff7f0e',    # Orange
        'Remote': '#1f77b4',     # Blue
        'Supervisory': '#9467bd', # Purple
        'Supervised': '#9467bd'  # Purple
    }
    
    # Create color sequence based on modes
    color_sequence = [mode_colors.get(mode, '#1f77b4') for mode in loop_modes['mode']]
    
    fig = px.bar(loop_modes, x='mode', y='count',
                 color='mode', color_discrete_sequence=color_sequence,
                 labels={'mode': 'Control Mode', 'count': 'Count'})
    
    fig.update_layout(
        title="Loop Control Modes",
        xaxis_title="Control Mode",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_calibration_status_chart(metrics):
    """Create calibration status chart"""
    if ('sensors_past_calibration' not in metrics or 
        'sensors_due_calibration_soon' not in metrics or 
        'total_sensors' not in metrics):
        fig = go.Figure()
        fig.update_layout(
            title="Sensor Calibration Status (No Data)",
            height=400
        )
        return fig
    
    past_due = metrics['sensors_past_calibration']
    due_soon = metrics['sensors_due_calibration_soon']
    total = metrics['total_sensors']
    current = max(0, total - past_due - due_soon)  # Ensure we don't get negative values
    
    # Create data for pie chart
    labels = ['Current', 'Due Soon (30 days)', 'Past Due']
    values = [current, due_soon, past_due]
    colors = ['#2ca02c', '#ff7f0e', '#d62728']  # Green, Orange, Red
    
    # Check for valid data
    if sum(values) == 0:
        fig = go.Figure()
        fig.update_layout(
            title="Sensor Calibration Status (No Data)",
            height=400
        )
        return fig
    
    try:
        fig = px.pie(values=values, names=labels, color=labels,
                    color_discrete_map=dict(zip(labels, colors)))
        
        fig.update_layout(
            title="Sensor Calibration Status",
            legend=dict(orientation="h", yanchor="bottom", y=-0.1),
            margin=dict(t=40, b=40, l=10, r=10),
            height=400
        )
    except Exception as e:
        print(f"Error creating calibration status chart: {e}")
        # Create a fallback figure
        fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
        fig.update_layout(
            title="Sensor Calibration Status",
            height=400
        )
    
    return fig

def create_maintenance_status_chart(metrics):
    """Create maintenance status chart"""
    if ('maintenance_required_rate' not in metrics):
        return go.Figure()
    
    maintenance_rate = metrics['maintenance_required_rate']
    
    # Create gauge chart
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = maintenance_rate,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Devices Requiring Maintenance (%)"},
        gauge = {
            'axis': {'range': [None, 100]},
            'bar': {'color': "#ff7f0e"},
            'steps': [
                {'range': [0, 5], 'color': '#2ca02c'},
                {'range': [5, 15], 'color': '#ffbb78'},
                {'range': [15, 30], 'color': '#ff7f0e'},
                {'range': [30, 100], 'color': '#d62728'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 30
            }
        }
    ))
    
    fig.update_layout(
        height=400,
        margin=dict(t=40, b=40, l=10, r=10)
    )
    
    return fig

def create_sensor_measurement_units_chart(metrics):
    """Create sensor measurement units chart"""
    if 'sensor_units' not in metrics or metrics['sensor_units'].empty:
        fig = go.Figure()
        fig.update_layout(
            title="Sensor Measurement Units (No Data)",
            height=400
        )
        return fig
    
    units = metrics['sensor_units']
    
    try:
        fig = px.bar(units, x='unit', y='count',
                    color='unit', color_discrete_sequence=px.colors.qualitative.Plotly,
                    labels={'unit': 'Measurement Unit', 'count': 'Count'})
        
        fig.update_layout(
            title="Sensor Measurement Units",
            xaxis_title="Unit",
            yaxis_title="Count",
            margin=dict(t=40, b=40, l=10, r=10),
            height=400,
            showlegend=False
        )
    except Exception as e:
        print(f"Error creating sensor measurement units chart: {e}")
        # Create a fallback figure using plotly.graph_objects
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=units['unit'],
            y=units['count'],
            marker_color='royalblue'
        ))
        fig.update_layout(
            title="Sensor Measurement Units",
            xaxis_title="Unit",
            yaxis_title="Count",
            height=400
        )
    
    return fig

# Set up the Dash application
def create_dashboard(datasets, metrics):
    """Create a Dash dashboard to visualize the metrics"""
    # Initialize the Dash app
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Define the layout
    app.layout = dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.H1("Process Control Dashboard", className="text-center mb-4"),
                html.H5("ISA-95 Level 1 Sensing & Manipulation", className="text-center text-muted mb-5")
            ], width=12)
        ]),
        
        # Top metrics cards - Row 1
        dbc.Row([
            # Total Sensors
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Total Sensors", className="card-title"),
                        html.H3(f"{metrics.get('total_sensors', 0)}", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Sensor Operational Rate
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Sensor Operational Rate", className="card-title"),
                        html.H3(f"{metrics.get('sensor_operational_rate', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Total Actuators
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Total Actuators", className="card-title"),
                        html.H3(f"{metrics.get('total_actuators', 0)}", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Actuator Operational Rate
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Actuator Operational Rate", className="card-title"),
                        html.H3(f"{metrics.get('actuator_operational_rate', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3)
        ]),
        
        # Top metrics cards - Row 2
        dbc.Row([
            # Reading Quality
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Avg Reading Quality", className="card-title"),
                        html.H3(f"{metrics.get('avg_quality', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Reading Error Rate
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Reading Error Rate", className="card-title"),
                        html.H3(f"{metrics.get('reading_error_rate', 0):.1f}%", className="card-text text-danger")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Manual Command Rate
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Manual Command Rate", className="card-title"),
                        html.H3(f"{metrics.get('manual_command_rate', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Auto Control Rate
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Auto Control Rate", className="card-title"),
                        html.H3(f"{metrics.get('auto_control_rate', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3)
        ]),
        
        # Calibration and Maintenance Status
        dbc.Row([
            # Calibration Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sensor Calibration Status"),
                    dbc.CardBody(
                        dcc.Graph(id='calibration-status', figure=create_calibration_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Maintenance Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Maintenance Status"),
                    dbc.CardBody(
                        dcc.Graph(id='maintenance-status', figure=create_maintenance_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Sensor Analysis
        dbc.Row([
            # Sensor Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sensor Types"),
                    dbc.CardBody(
                        dcc.Graph(id='sensor-types', figure=create_sensor_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Sensor Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sensor Status"),
                    dbc.CardBody(
                        dcc.Graph(id='sensor-status', figure=create_sensor_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Sensor Measurement Units
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sensor Measurement Units"),
                    dbc.CardBody(
                        dcc.Graph(id='sensor-units', figure=create_sensor_measurement_units_chart(metrics))
                    )
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Actuator Analysis
        dbc.Row([
            # Actuator Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Actuator Types"),
                    dbc.CardBody(
                        dcc.Graph(id='actuator-types', figure=create_actuator_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Actuator Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Actuator Status"),
                    dbc.CardBody(
                        dcc.Graph(id='actuator-status', figure=create_actuator_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Reading Analysis
        dbc.Row([
            # Reading Quality Distribution
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Reading Quality Distribution"),
                    dbc.CardBody(
                        dcc.Graph(id='reading-quality', figure=create_reading_quality_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Readings by Sensor Type
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Readings by Sensor Type"),
                    dbc.CardBody(
                        dcc.Graph(id='readings-by-type', figure=create_readings_by_type_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Command Analysis
        dbc.Row([
            # Control Mode Distribution
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Control Mode Distribution"),
                    dbc.CardBody(
                        dcc.Graph(id='control-modes', figure=create_control_modes_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Command Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Command Types"),
                    dbc.CardBody(
                        dcc.Graph(id='command-types', figure=create_command_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Time Series Analysis
        dbc.Row([
            # Hourly Readings
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sensor Readings Over Time"),
                    dbc.CardBody(
                        dcc.Graph(id='hourly-readings', figure=create_hourly_readings_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Hourly Commands
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Actuator Commands Over Time"),
                    dbc.CardBody(
                        dcc.Graph(id='hourly-commands', figure=create_hourly_commands_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Diagnostics Analysis
        dbc.Row([
            # Diagnostic Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Diagnostic Types"),
                    dbc.CardBody(
                        dcc.Graph(id='diagnostic-types', figure=create_diagnostic_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Severity Distribution
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Severity Distribution"),
                    dbc.CardBody(
                        dcc.Graph(id='severity-distribution', figure=create_severity_distribution_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Control Loop Analysis
        dbc.Row([
            # Controller Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Controller Types"),
                    dbc.CardBody(
                        dcc.Graph(id='controller-types', figure=create_controller_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Loop Control Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Loop Control Modes"),
                    dbc.CardBody(
                        dcc.Graph(id='loop-modes', figure=create_loop_modes_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P("ISA-95 Level 1 Process Control Dashboard", className="text-center text-muted")
            ], width=12)
        ])
    ], fluid=True)
    
    return app

# Main function to run the dashboard
def main():
    # Load all data
    print("Loading data...")
    datasets = load_all_data()
    
    # Calculate metrics
    print("Calculating metrics...")
    metrics = calculate_metrics(datasets)
    
    # Create and run the dashboard
    print("Creating dashboard...")
    app = create_dashboard(datasets, metrics)
    
    print("Dashboard ready! Running on http://127.0.0.1:8060/")
    app.run_server(debug=True, port=8060)  # Using port 8053 to avoid conflict with other dashboards

if __name__ == "__main__":
    main()