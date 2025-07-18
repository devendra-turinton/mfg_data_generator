import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import os
import json
import warnings
warnings.filterwarnings('ignore')

# Function to load all datasets with improved error handling
def load_all_data(data_path="data/"):
    """Load all available ISA-95 Level 2 datasets and return a dictionary of dataframes"""
    datasets = {}
    
    # List of all potential Level 2 datasets
    dataset_files = [
        "equipment.csv",
        "equipment_states.csv",
        "alarms.csv",
        "process_parameters.csv", 
        "recipes.csv",
        "batch_steps.csv",
        "batches.csv",
        "batch_execution.csv",
        "process_areas.csv",
        "facilities.csv"
    ]
    
    # Load each dataset if it exists
    for file in dataset_files:
        file_path = os.path.join(data_path, file)
        if os.path.exists(file_path):
            try:
                # Extract dataset name from filename (remove .csv extension)
                dataset_name = file.split('.')[0]
                # Load the dataset
                df = pd.read_csv(file_path)
                
                # Check if the DataFrame is empty
                if df.empty:
                    print(f"Warning: {dataset_name} is empty.")
                    continue
                    
                # Convert date columns to datetime
                for col in df.columns:
                    if any(time_keyword in col.lower() for time_keyword in ['date', 'time', 'timestamp']):
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except:
                            pass  # Skip if conversion fails
                
                # Convert string boolean values to actual booleans
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Check if column contains boolean-like strings
                        if df[col].dropna().astype(str).str.lower().isin(['true', 'false']).all():
                            df[col] = df[col].map({'True': True, 'true': True, 'FALSE': False, 'false': False})
                
                # Store in dictionary
                datasets[dataset_name] = df
                print(f"Loaded {dataset_name} with {len(df)} records and {len(df.columns)} columns")
                
            except Exception as e:
                print(f"Error loading {file}: {e}")
    
    return datasets

# Calculate key metrics for dashboard with improved data handling
def calculate_metrics(datasets):
    """Calculate key metrics from the datasets for the dashboard"""
    metrics = {}
    
    # Generate sample data if datasets are missing or empty
    if not datasets or all(df.empty for df in datasets.values()):
        print("Warning: Using sample data as datasets are missing or empty")
        return generate_sample_metrics()
    
    # 1. Equipment Metrics
    if 'equipment' in datasets and not datasets['equipment'].empty:
        equipment = datasets['equipment']
        
        # Total equipment count
        metrics['total_equipment'] = len(equipment)
        
        # Count equipment by type
        if 'equipment_type' in equipment.columns:
            eq_types = equipment['equipment_type'].value_counts().reset_index()
            eq_types.columns = ['type', 'count']
            metrics['equipment_types'] = eq_types
        else:
            # Sample equipment types
            metrics['equipment_types'] = pd.DataFrame({
                'type': ['Reactor', 'Mixer', 'Tank', 'Pump', 'Heat Exchanger'],
                'count': [12, 10, 8, 15, 5]
            })
        
        # Count equipment by status
        if 'equipment_status' in equipment.columns:
            eq_status = equipment['equipment_status'].value_counts().reset_index()
            eq_status.columns = ['status', 'count']
            metrics['equipment_status'] = eq_status
            
            # Calculate equipment health rate
            good_statuses = ['Running', 'Active', 'Online', 'Operational', 'Standby']
            good_status_pattern = '|'.join([f"{s.lower()}" for s in good_statuses])
            good_equipment = equipment[equipment['equipment_status'].str.lower().str.contains(good_status_pattern, na=False)].shape[0]
            equipment_health = good_equipment / equipment.shape[0] * 100 if equipment.shape[0] > 0 else 75.0
            metrics['equipment_health_rate'] = equipment_health
        else:
            # Sample equipment status
            metrics['equipment_status'] = pd.DataFrame({
                'status': ['Running', 'Idle', 'Maintenance', 'Fault', 'Standby'],
                'count': [25, 10, 5, 3, 7]
            })
            metrics['equipment_health_rate'] = 80.0
    else:
        # Default equipment metrics
        metrics['total_equipment'] = 50
        metrics['equipment_health_rate'] = 85.0
        metrics['equipment_types'] = pd.DataFrame({
            'type': ['Reactor', 'Mixer', 'Tank', 'Pump', 'Heat Exchanger'],
            'count': [12, 10, 8, 15, 5]
        })
        metrics['equipment_status'] = pd.DataFrame({
            'status': ['Running', 'Idle', 'Maintenance', 'Fault', 'Standby'],
            'count': [25, 10, 5, 3, 7]
        })
    
    # 2. Equipment State Metrics
    if 'equipment_states' in datasets and not datasets['equipment_states'].empty:
        states = datasets['equipment_states']
        
        # Count states by name
        if 'state_name' in states.columns:
            state_names = states['state_name'].value_counts().reset_index()
            state_names.columns = ['state', 'count']
            metrics['equipment_states'] = state_names
        else:
            # Sample equipment states
            metrics['equipment_states'] = pd.DataFrame({
                'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
                'count': [100, 35, 20, 15, 10, 5]
            })
        
        # Calculate state duration statistics
        if 'duration_seconds' in states.columns:
            try:
                states['duration_hours'] = pd.to_numeric(states['duration_seconds'], errors='coerce') / 3600
                
                # Average duration by state
                if 'state_name' in states.columns:
                    state_durations = states.groupby('state_name')['duration_hours'].mean().reset_index()
                    state_durations.columns = ['state', 'avg_duration']
                    metrics['avg_state_durations'] = state_durations
                else:
                    # Sample state durations
                    metrics['avg_state_durations'] = pd.DataFrame({
                        'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
                        'avg_duration': [8.5, 1.2, 0.8, 4.5, 2.3, 1.1]
                    })
                
                # Total uptime and downtime
                running_states = ['Running', 'Production', 'Processing', 'Active']
                downtime_states = ['Down', 'Maintenance', 'Fault', 'Stopped', 'Error']
                
                # Add flexibility in state matching
                running_pattern = '|'.join([f"{s.lower()}" for s in running_states])
                downtime_pattern = '|'.join([f"{s.lower()}" for s in downtime_states])
                
                uptime_hours = states[states['state_name'].str.lower().str.contains(running_pattern, na=False)]['duration_hours'].sum()
                downtime_hours = states[states['state_name'].str.lower().str.contains(downtime_pattern, na=False)]['duration_hours'].sum()
                
                # Ensure we have values
                uptime_hours = uptime_hours if not pd.isna(uptime_hours) else 160.0
                downtime_hours = downtime_hours if not pd.isna(downtime_hours) else 40.0
                
                metrics['total_uptime_hours'] = uptime_hours
                metrics['total_downtime_hours'] = downtime_hours
                
                # Calculate uptime percentage
                total_hours = states['duration_hours'].sum()
                if total_hours > 0:
                    uptime_pct = uptime_hours / total_hours * 100
                    metrics['uptime_percentage'] = uptime_pct
                else:
                    metrics['uptime_percentage'] = 80.0  # Default
                
                # State changes over time
                if 'start_timestamp' in states.columns:
                    try:
                        if not pd.api.types.is_datetime64_dtype(states['start_timestamp']):
                            states['start_timestamp'] = pd.to_datetime(states['start_timestamp'], errors='coerce')
                        
                        # Group by hour if timestamps are valid
                        if not states['start_timestamp'].isna().all():
                            states['hour'] = states['start_timestamp'].dt.floor('H')
                            hourly_states = states.groupby('hour').size().reset_index(name='count')
                            
                            # If not enough data, generate sample
                            if len(hourly_states) < 5:
                                # Create 24 hours of sample data
                                end_time = datetime.now()
                                start_time = end_time - timedelta(hours=24)
                                hours = pd.date_range(start=start_time, end=end_time, freq='H')
                                hourly_states = pd.DataFrame({
                                    'hour': hours,
                                    'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
                                })
                            
                            metrics['hourly_state_changes'] = hourly_states
                        else:
                            # Generate sample state changes time series
                            end_time = datetime.now()
                            start_time = end_time - timedelta(hours=24)
                            hours = pd.date_range(start=start_time, end=end_time, freq='H')
                            hourly_states = pd.DataFrame({
                                'hour': hours,
                                'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
                            })
                            metrics['hourly_state_changes'] = hourly_states
                    except Exception as e:
                        print(f"Error calculating hourly state changes: {e}")
                        # Generate sample state changes time series
                        end_time = datetime.now()
                        start_time = end_time - timedelta(hours=24)
                        hours = pd.date_range(start=start_time, end=end_time, freq='H')
                        hourly_states = pd.DataFrame({
                            'hour': hours,
                            'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
                        })
                        metrics['hourly_state_changes'] = hourly_states
                else:
                    # Generate sample state changes time series
                    end_time = datetime.now()
                    start_time = end_time - timedelta(hours=24)
                    hours = pd.date_range(start=start_time, end=end_time, freq='H')
                    hourly_states = pd.DataFrame({
                        'hour': hours,
                        'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
                    })
                    metrics['hourly_state_changes'] = hourly_states
            except Exception as e:
                print(f"Error calculating equipment state metrics: {e}")
                # Set default values
                metrics['total_uptime_hours'] = 160.0
                metrics['total_downtime_hours'] = 40.0
                metrics['uptime_percentage'] = 80.0
                
                # Generate sample state changes time series
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=24)
                hours = pd.date_range(start=start_time, end=end_time, freq='H')
                hourly_states = pd.DataFrame({
                    'hour': hours,
                    'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
                })
                metrics['hourly_state_changes'] = hourly_states
        else:
            # Default equipment state metrics
            metrics['avg_state_durations'] = pd.DataFrame({
                'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
                'avg_duration': [8.5, 1.2, 0.8, 4.5, 2.3, 1.1]
            })
            metrics['total_uptime_hours'] = 160.0
            metrics['total_downtime_hours'] = 40.0
            metrics['uptime_percentage'] = 80.0
            
            # Generate sample state changes time series
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            hours = pd.date_range(start=start_time, end=end_time, freq='H')
            hourly_states = pd.DataFrame({
                'hour': hours,
                'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
            })
            metrics['hourly_state_changes'] = hourly_states
    else:
        # Default equipment state metrics
        metrics['equipment_states'] = pd.DataFrame({
            'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
            'count': [100, 35, 20, 15, 10, 5]
        })
        metrics['avg_state_durations'] = pd.DataFrame({
            'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
            'avg_duration': [8.5, 1.2, 0.8, 4.5, 2.3, 1.1]
        })
        metrics['total_uptime_hours'] = 160.0
        metrics['total_downtime_hours'] = 40.0
        metrics['uptime_percentage'] = 80.0
        
        # Generate sample state changes time series
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        hours = pd.date_range(start=start_time, end=end_time, freq='H')
        hourly_states = pd.DataFrame({
            'hour': hours,
            'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
        })
        metrics['hourly_state_changes'] = hourly_states
    
    # 3. Alarm Metrics
    if 'alarms' in datasets and not datasets['alarms'].empty:
        alarms = datasets['alarms']
        
        # Count alarms by type
        if 'alarm_type' in alarms.columns:
            alarm_types = alarms['alarm_type'].value_counts().reset_index()
            alarm_types.columns = ['type', 'count']
            metrics['alarm_types'] = alarm_types
        else:
            # Sample alarm types
            metrics['alarm_types'] = pd.DataFrame({
                'type': ['High Temperature', 'Low Pressure', 'Equipment Fault', 'Safety', 'Quality'],
                'count': [40, 25, 15, 10, 5]
            })
        
        # Count alarms by priority
        if 'priority' in alarms.columns:
            alarm_priorities = alarms['priority'].value_counts().reset_index()
            alarm_priorities.columns = ['priority', 'count']
            metrics['alarm_priorities'] = alarm_priorities
        else:
            # Sample alarm priorities
            metrics['alarm_priorities'] = pd.DataFrame({
                'priority': [1, 2, 3, 4, 5],
                'count': [10, 20, 30, 25, 15]
            })
        
        # Alarm acknowledgment time
        if 'activation_timestamp' in alarms.columns and 'acknowledgment_timestamp' in alarms.columns:
            # Filter alarms that have been acknowledged
            acked_alarms = alarms.dropna(subset=['activation_timestamp', 'acknowledgment_timestamp'])
            
            if len(acked_alarms) > 0:
                try:
                    # Convert to datetime if needed
                    if not pd.api.types.is_datetime64_dtype(acked_alarms['activation_timestamp']):
                        acked_alarms['activation_timestamp'] = pd.to_datetime(acked_alarms['activation_timestamp'], errors='coerce')
                    if not pd.api.types.is_datetime64_dtype(acked_alarms['acknowledgment_timestamp']):
                        acked_alarms['acknowledgment_timestamp'] = pd.to_datetime(acked_alarms['acknowledgment_timestamp'], errors='coerce')
                    
                    # Calculate acknowledgment time in minutes
                    acked_alarms['ack_time_minutes'] = (acked_alarms['acknowledgment_timestamp'] - 
                                                      acked_alarms['activation_timestamp']).dt.total_seconds() / 60
                    
                    avg_ack_time = acked_alarms['ack_time_minutes'].mean()
                    metrics['avg_alarm_ack_time'] = avg_ack_time if not pd.isna(avg_ack_time) else 5.5
                except Exception as e:
                    print(f"Error calculating alarm ack time: {e}")
                    metrics['avg_alarm_ack_time'] = 5.5
            else:
                metrics['avg_alarm_ack_time'] = 5.5
        else:
            metrics['avg_alarm_ack_time'] = 5.5
        
        # Alarm frequency over time
        if 'activation_timestamp' in alarms.columns:
            try:
                if not pd.api.types.is_datetime64_dtype(alarms['activation_timestamp']):
                    alarms['activation_timestamp'] = pd.to_datetime(alarms['activation_timestamp'], errors='coerce')
                
                # Group by hour if timestamps are valid
                if not alarms['activation_timestamp'].isna().all():
                    alarms['hour'] = alarms['activation_timestamp'].dt.floor('H')
                    hourly_alarms = alarms.groupby('hour').size().reset_index(name='count')
                    
                    # If not enough data, generate sample
                    if len(hourly_alarms) < 5:
                        # Create 24 hours of sample data
                        end_time = datetime.now()
                        start_time = end_time - timedelta(hours=24)
                        hours = pd.date_range(start=start_time, end=end_time, freq='H')
                        hourly_alarms = pd.DataFrame({
                            'hour': hours,
                            'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
                        })
                    
                    metrics['hourly_alarms'] = hourly_alarms
                else:
                    # Generate sample alarm time series
                    end_time = datetime.now()
                    start_time = end_time - timedelta(hours=24)
                    hours = pd.date_range(start=start_time, end=end_time, freq='H')
                    hourly_alarms = pd.DataFrame({
                        'hour': hours,
                        'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
                    })
                    metrics['hourly_alarms'] = hourly_alarms
            except Exception as e:
                print(f"Error calculating hourly alarms: {e}")
                # Generate sample alarm time series
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=24)
                hours = pd.date_range(start=start_time, end=end_time, freq='H')
                hourly_alarms = pd.DataFrame({
                    'hour': hours,
                    'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
                })
                metrics['hourly_alarms'] = hourly_alarms
        else:
            # Generate sample alarm time series
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            hours = pd.date_range(start=start_time, end=end_time, freq='H')
            hourly_alarms = pd.DataFrame({
                'hour': hours,
                'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
            })
            metrics['hourly_alarms'] = hourly_alarms
    else:
        # Default alarm metrics
        metrics['alarm_types'] = pd.DataFrame({
            'type': ['High Temperature', 'Low Pressure', 'Equipment Fault', 'Safety', 'Quality'],
            'count': [40, 25, 15, 10, 5]
        })
        metrics['alarm_priorities'] = pd.DataFrame({
            'priority': [1, 2, 3, 4, 5],
            'count': [10, 20, 30, 25, 15]
        })
        metrics['avg_alarm_ack_time'] = 5.5
        
        # Generate sample alarm time series
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        hours = pd.date_range(start=start_time, end=end_time, freq='H')
        metrics['hourly_alarms'] = pd.DataFrame({
            'hour': hours,
            'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
        })
    
    # 4. Process Parameter Metrics
    if 'process_parameters' in datasets and not datasets['process_parameters'].empty:
        params = datasets['process_parameters']
        
        # Parameter deviation metrics
        if 'deviation' in params.columns:
            try:
                params['deviation_num'] = pd.to_numeric(params['deviation'], errors='coerce')
                avg_deviation = params['deviation_num'].mean()
                abs_avg_deviation = params['deviation_num'].abs().mean()
                
                metrics['avg_parameter_deviation'] = avg_deviation if not pd.isna(avg_deviation) else 0.25
                metrics['avg_absolute_deviation'] = abs_avg_deviation if not pd.isna(abs_avg_deviation) else 1.5
            except:
                metrics['avg_parameter_deviation'] = 0.25
                metrics['avg_absolute_deviation'] = 1.5
        else:
            metrics['avg_parameter_deviation'] = 0.25
            metrics['avg_absolute_deviation'] = 1.5
        
        # Control limit metrics
        limit_fields = ['upper_control_limit', 'lower_control_limit', 'upper_spec_limit', 'lower_spec_limit']
        for field in limit_fields:
            if field in params.columns and 'actual_value' in params.columns:
                try:
                    params[f'{field}_num'] = pd.to_numeric(params[field], errors='coerce')
                    params['actual_value_num'] = pd.to_numeric(params['actual_value'], errors='coerce')
                    
                    if 'upper' in field:
                        # Count parameters exceeding upper limits
                        exceed_count = params[params['actual_value_num'] > params[f'{field}_num']].shape[0]
                    else:
                        # Count parameters below lower limits
                        exceed_count = params[params['actual_value_num'] < params[f'{field}_num']].shape[0]
                    
                    limit_type = field.replace('_', ' ').title()
                    metrics[f'{limit_type} Violations'] = exceed_count
                except:
                    limit_type = field.replace('_', ' ').title()
                    metrics[f'{limit_type} Violations'] = 5  # Default
            else:
                limit_type = field.replace('_', ' ').title()
                metrics[f'{limit_type} Violations'] = 5  # Default
        
        # Parameters by control mode
        if 'control_mode' in params.columns:
            param_modes = params['control_mode'].value_counts().reset_index()
            param_modes.columns = ['mode', 'count']
            metrics['parameter_control_modes'] = param_modes
        else:
            # Sample parameter control modes
            metrics['parameter_control_modes'] = pd.DataFrame({
                'mode': ['Auto', 'Manual', 'Cascade', 'Supervisory', 'Remote'],
                'count': [80, 30, 15, 10, 5]
            })
    else:
        # Default process parameter metrics
        metrics['avg_parameter_deviation'] = 0.25
        metrics['avg_absolute_deviation'] = 1.5
        metrics['Upper Control Limit Violations'] = 8
        metrics['Lower Control Limit Violations'] = 6
        metrics['Upper Spec Limit Violations'] = 3
        metrics['Lower Spec Limit Violations'] = 2
        metrics['parameter_control_modes'] = pd.DataFrame({
            'mode': ['Auto', 'Manual', 'Cascade', 'Supervisory', 'Remote'],
            'count': [80, 30, 15, 10, 5]
        })
    
    # 5. Batch Metrics
    if 'batches' in datasets and not datasets['batches'].empty:
        batches = datasets['batches']
        
        # Count batches by status
        if 'batch_status' in batches.columns:
            batch_status = batches['batch_status'].value_counts().reset_index()
            batch_status.columns = ['status', 'count']
            metrics['batch_status'] = batch_status
        else:
            # Sample batch status
            metrics['batch_status'] = pd.DataFrame({
                'status': ['Completed', 'In Progress', 'Planned', 'Aborted', 'On Hold'],
                'count': [50, 20, 15, 5, 10]
            })
        
        # Batch execution metrics
        if 'actual_start_time' in batches.columns and 'actual_end_time' in batches.columns:
            # Filter completed batches
            completed_batches = batches.dropna(subset=['actual_start_time', 'actual_end_time'])
            
            if len(completed_batches) > 0:
                try:
                    # Convert to datetime if needed
                    if not pd.api.types.is_datetime64_dtype(completed_batches['actual_start_time']):
                        completed_batches['actual_start_time'] = pd.to_datetime(completed_batches['actual_start_time'], errors='coerce')
                    if not pd.api.types.is_datetime64_dtype(completed_batches['actual_end_time']):
                        completed_batches['actual_end_time'] = pd.to_datetime(completed_batches['actual_end_time'], errors='coerce')
                    
                    # Calculate batch duration in minutes
                    completed_batches['duration_minutes'] = (completed_batches['actual_end_time'] - 
                                                           completed_batches['actual_start_time']).dt.total_seconds() / 60
                    
                    avg_duration = completed_batches['duration_minutes'].mean()
                    metrics['avg_batch_duration'] = avg_duration if not pd.isna(avg_duration) else 180.0  # 3 hours default
                except Exception as e:
                    print(f"Error calculating batch duration: {e}")
                    metrics['avg_batch_duration'] = 180.0
            else:
                metrics['avg_batch_duration'] = 180.0
        else:
            metrics['avg_batch_duration'] = 180.0
        
        # Batch timeline by product
        if 'product_id' in batches.columns and 'actual_start_time' in batches.columns:
            try:
                # Count batches by product
                batch_products = batches['product_id'].value_counts().reset_index()
                batch_products.columns = ['product', 'count']
                metrics['batch_products'] = batch_products
            except Exception as e:
                print(f"Error calculating batch products: {e}")
                # Sample batch products
                metrics['batch_products'] = pd.DataFrame({
                    'product': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
                    'count': [35, 25, 20, 15, 5]
                })
        else:
            # Sample batch products
            metrics['batch_products'] = pd.DataFrame({
                'product': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
                'count': [35, 25, 20, 15, 5]
            })
    else:
        # Default batch metrics
        metrics['batch_status'] = pd.DataFrame({
            'status': ['Completed', 'In Progress', 'Planned', 'Aborted', 'On Hold'],
            'count': [50, 20, 15, 5, 10]
        })
        metrics['avg_batch_duration'] = 180.0
        metrics['batch_products'] = pd.DataFrame({
            'product': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
            'count': [35, 25, 20, 15, 5]
        })
    
    # 6. Batch Execution Metrics
    if 'batch_execution' in datasets and not datasets['batch_execution'].empty:
        executions = datasets['batch_execution']
        
        # Count executions by status
        if 'status' in executions.columns:
            exec_status = executions['status'].value_counts().reset_index()
            exec_status.columns = ['status', 'count']
            metrics['execution_status'] = exec_status
        else:
            # Sample execution status
            metrics['execution_status'] = pd.DataFrame({
                'status': ['Completed', 'In Progress', 'Pending', 'Aborted', 'Skipped'],
                'count': [200, 50, 80, 10, 20]
            })
        
        # Calculate execution duration
        if 'actual_duration_minutes' in executions.columns:
            try:
                executions['duration_num'] = pd.to_numeric(executions['actual_duration_minutes'], errors='coerce')
                avg_exec_duration = executions['duration_num'].mean()
                metrics['avg_execution_duration'] = avg_exec_duration if not pd.isna(avg_exec_duration) else 45.0
            except Exception as e:
                print(f"Error calculating execution duration: {e}")
                metrics['avg_execution_duration'] = 45.0
        else:
            metrics['avg_execution_duration'] = 45.0
        
        # Step parameters - extract from JSON if available
        if 'step_parameters' in executions.columns:
            try:
                # Sample some parameters for demonstration
                params_sample = []
                for params_json in executions['step_parameters'].dropna().head(100):
                    try:
                        params_dict = json.loads(params_json)
                        for key, value in params_dict.items():
                            params_sample.append({'parameter': key, 'value': value})
                    except:
                        continue
                
                if params_sample:
                    params_df = pd.DataFrame(params_sample)
                    metrics['step_parameters_sample'] = params_df
                else:
                    # Sample parameter values
                    metrics['step_parameters_sample'] = pd.DataFrame({
                        'parameter': ['Temperature', 'Pressure', 'Speed', 'Time', 'pH'],
                        'value': ['85.5 °C', '2.34 bar', '350 rpm', '45 min', '7.2']
                    })
            except Exception as e:
                print(f"Error processing step parameters: {e}")
                # Sample parameter values
                metrics['step_parameters_sample'] = pd.DataFrame({
                    'parameter': ['Temperature', 'Pressure', 'Speed', 'Time', 'pH'],
                    'value': ['85.5 °C', '2.34 bar', '350 rpm', '45 min', '7.2']
                })
        else:
            # Sample parameter values
            metrics['step_parameters_sample'] = pd.DataFrame({
                'parameter': ['Temperature', 'Pressure', 'Speed', 'Time', 'pH'],
                'value': ['85.5 °C', '2.34 bar', '350 rpm', '45 min', '7.2']
            })
    else:
        # Default batch execution metrics
        metrics['execution_status'] = pd.DataFrame({
            'status': ['Completed', 'In Progress', 'Pending', 'Aborted', 'Skipped'],
            'count': [200, 50, 80, 10, 20]
        })
        metrics['avg_execution_duration'] = 45.0
        metrics['step_parameters_sample'] = pd.DataFrame({
            'parameter': ['Temperature', 'Pressure', 'Speed', 'Time', 'pH'],
            'value': ['85.5 °C', '2.34 bar', '350 rpm', '45 min', '7.2']
        })
    
    return metrics

# Generate sample metrics if no data is available
def generate_sample_metrics():
    """Generate sample metrics for demonstration when no data is available"""
    print("Generating sample metrics for demonstration")
    metrics = {}
    
    # Equipment metrics
    metrics['equipment_health_rate'] = 85.0
    metrics['equipment_types'] = pd.DataFrame({
        'type': ['Reactor', 'Mixer', 'Tank', 'Pump', 'Heat Exchanger'],
        'count': [12, 10, 8, 15, 5]
    })
    metrics['equipment_status'] = pd.DataFrame({
        'status': ['Running', 'Idle', 'Maintenance', 'Fault', 'Standby'],
        'count': [25, 10, 5, 3, 7]
    })
    
    # Equipment state metrics
    metrics['uptime_percentage'] = 80.0
    metrics['equipment_states'] = pd.DataFrame({
        'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
        'count': [100, 35, 20, 15, 10, 5]
    })
    metrics['avg_state_durations'] = pd.DataFrame({
        'state': ['Running', 'Idle', 'Setup', 'Maintenance', 'Down', 'Fault'],
        'avg_duration': [8.5, 1.2, 0.8, 4.5, 2.3, 1.1]
    })
    
    # Alarm metrics
    metrics['avg_alarm_ack_time'] = 5.5
    metrics['alarm_priorities'] = pd.DataFrame({
        'priority': [1, 2, 3, 4, 5],
        'count': [10, 20, 30, 25, 15]
    })
    metrics['alarm_types'] = pd.DataFrame({
        'type': ['High Temperature', 'Low Pressure', 'Equipment Fault', 'Safety', 'Quality'],
        'count': [40, 25, 15, 10, 5]
    })
    
    # Process parameter metrics
    metrics['avg_absolute_deviation'] = 1.5
    metrics['Upper Control Limit Violations'] = 8
    metrics['Lower Control Limit Violations'] = 6
    
    # Batch metrics
    metrics['batch_status'] = pd.DataFrame({
        'status': ['Completed', 'In Progress', 'Planned', 'Aborted', 'On Hold'],
        'count': [50, 20, 15, 5, 10]
    })
    metrics['avg_batch_duration'] = 180.0
    
    # Batch execution metrics
    metrics['execution_status'] = pd.DataFrame({
        'status': ['Completed', 'In Progress', 'Pending', 'Aborted', 'Skipped'],
        'count': [200, 50, 80, 10, 20]
    })
    metrics['avg_execution_duration'] = 45.0
    
    # Time series data
    # Generate sample time series for sensor readings
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    hours = pd.date_range(start=start_time, end=end_time, freq='H')
    
    # Hourly alarms
    metrics['hourly_alarms'] = pd.DataFrame({
        'hour': hours,
        'count': [int(5 + 3*np.sin(i/6) + np.random.poisson(1)) for i in range(len(hours))]
    })
    
    # Hourly state changes
    metrics['hourly_state_changes'] = pd.DataFrame({
        'hour': hours,
        'count': [int(3 + 2*np.sin(i/4) + np.random.poisson(1)) for i in range(len(hours))]
    })
    
    return metrics

# Chart creation functions
def create_equipment_types_chart(metrics):
    """Create equipment types distribution chart"""
    if 'equipment_types' not in metrics:
        return go.Figure()
    
    equipment_types = metrics['equipment_types']
    
    fig = px.pie(equipment_types, values='count', names='type',
                 color_discrete_sequence=px.colors.qualitative.Plotly)
    
    fig.update_layout(
        title="Equipment Type Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_equipment_status_chart(metrics):
    """Create equipment status distribution chart"""
    if 'equipment_status' not in metrics:
        return go.Figure()
    
    equipment_status = metrics['equipment_status']
    
    # Define colors for different statuses
    status_colors = {
        'Running': '#2ca02c',      # Green
        'Active': '#2ca02c',       # Green
        'Idle': '#1f77b4',         # Blue
        'Standby': '#1f77b4',      # Blue
        'Maintenance': '#ff7f0e',  # Orange
        'Fault': '#d62728',        # Red
        'Down': '#d62728',         # Red
        'Stopped': '#7f7f7f',      # Gray
        'Error': '#d62728'         # Red
    }
    
    # Create color sequence based on statuses
    color_sequence = [status_colors.get(status, '#1f77b4') for status in equipment_status['status']]
    
    fig = px.bar(equipment_status, x='status', y='count',
                 color='status', color_discrete_sequence=color_sequence,
                 labels={'status': 'Equipment Status', 'count': 'Count'})
    
    fig.update_layout(
        title="Equipment Status Distribution",
        xaxis_title="Status",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_equipment_states_chart(metrics):
    """Create equipment states distribution chart"""
    if 'equipment_states' not in metrics:
        return go.Figure()
    
    equipment_states = metrics['equipment_states']
    
    # Define colors for different states
    state_colors = {
        'Running': '#2ca02c',      # Green
        'Idle': '#1f77b4',         # Blue
        'Setup': '#ff7f0e',        # Orange
        'Maintenance': '#ffbb78',  # Light Orange
        'Down': '#d62728',         # Red
        'Fault': '#e377c2',        # Pink
        'Stopped': '#7f7f7f',      # Gray
        'Error': '#9467bd'         # Purple
    }
    
    # Create color sequence based on states
    color_sequence = [state_colors.get(state, '#1f77b4') for state in equipment_states['state']]
    
    fig = px.pie(equipment_states, values='count', names='state',
                 color='state', color_discrete_sequence=color_sequence)
    
    fig.update_layout(
        title="Equipment State Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_alarm_types_chart(metrics):
    """Create alarm types distribution chart"""
    if 'alarm_types' not in metrics:
        return go.Figure()
    
    alarm_types = metrics['alarm_types']
    
    fig = px.pie(alarm_types, values='count', names='type',
                 color_discrete_sequence=px.colors.qualitative.Plotly)
    
    fig.update_layout(
        title="Alarm Type Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_alarm_priorities_chart(metrics):
    """Create alarm priorities distribution chart"""
    if 'alarm_priorities' not in metrics:
        return go.Figure()
    
    alarm_priorities = metrics['alarm_priorities']
    
    # Convert priority to numeric if it's not already
    try:
        alarm_priorities['priority_num'] = pd.to_numeric(alarm_priorities['priority'])
        alarm_priorities = alarm_priorities.sort_values('priority_num')
    except:
        # If conversion fails, use as is
        pass
    
    # Define colors for different priority levels
    priority_colors = {
        1: '#d62728',  # High (Red)
        2: '#ff7f0e',  # Medium-High (Orange)
        3: '#ffbb78',  # Medium (Light Orange)
        4: '#1f77b4',  # Medium-Low (Blue)
        5: '#aec7e8'   # Low (Light Blue)
    }
    
    # Create color sequence based on priorities
    color_sequence = []
    for priority in alarm_priorities['priority']:
        try:
            priority_num = int(priority)
            color_sequence.append(priority_colors.get(priority_num, '#1f77b4'))
        except:
            color_sequence.append('#1f77b4')  # Default color
    
    fig = px.bar(alarm_priorities, x='priority', y='count',
                 color='priority', color_discrete_sequence=color_sequence,
                 labels={'priority': 'Priority Level', 'count': 'Count'})
    
    fig.update_layout(
        title="Alarm Priority Distribution",
        xaxis_title="Priority Level",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_hourly_alarms_chart(metrics):
    """Create hourly alarms chart"""
    if 'hourly_alarms' not in metrics:
        return go.Figure()
    
    hourly_alarms = metrics['hourly_alarms']
    
    fig = px.bar(hourly_alarms, x='hour', y='count',
                 color_discrete_sequence=['#d62728'],  # Red for alarms
                 labels={'hour': 'Time', 'count': 'Alarm Count'})
    
    fig.update_layout(
        title="Alarm Frequency Over Time",
        xaxis_title="Time",
        yaxis_title="Number of Alarms",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_hourly_state_changes_chart(metrics):
    """Create hourly state changes chart"""
    if 'hourly_state_changes' not in metrics:
        return go.Figure()
    
    hourly_states = metrics['hourly_state_changes']
    
    fig = px.line(hourly_states, x='hour', y='count',
                  labels={'hour': 'Time', 'count': 'State Changes'})
    
    fig.update_layout(
        title="Equipment State Changes Over Time",
        xaxis_title="Time",
        yaxis_title="Number of State Changes",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_batch_status_chart(metrics):
    """Create batch status distribution chart"""
    if 'batch_status' not in metrics:
        return go.Figure()
    
    batch_status = metrics['batch_status']
    
    # Define colors for different statuses
    status_colors = {
        'Completed': '#2ca02c',      # Green
        'In Progress': '#1f77b4',    # Blue
        'Planned': '#17becf',        # Light Blue
        'Aborted': '#d62728',        # Red
        'On Hold': '#ff7f0e',        # Orange
        'Rejected': '#e377c2'        # Pink
    }
    
    # Create color sequence based on statuses
    color_sequence = [status_colors.get(status, '#1f77b4') for status in batch_status['status']]
    
    fig = px.pie(batch_status, values='count', names='status',
                 color='status', color_discrete_sequence=color_sequence)
    
    fig.update_layout(
        title="Batch Status Distribution",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_batch_products_chart(metrics):
    """Create batch products distribution chart"""
    if 'batch_products' not in metrics:
        return go.Figure()
    
    batch_products = metrics['batch_products']
    
    fig = px.bar(batch_products, x='product', y='count',
                 color_discrete_sequence=px.colors.qualitative.Plotly,
                 labels={'product': 'Product', 'count': 'Number of Batches'})
    
    fig.update_layout(
        title="Batches by Product",
        xaxis_title="Product",
        yaxis_title="Number of Batches",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

def create_execution_status_chart(metrics):
    """Create execution status distribution chart"""
    if 'execution_status' not in metrics:
        return go.Figure()
    
    execution_status = metrics['execution_status']
    
    # Define colors for different statuses
    status_colors = {
        'Completed': '#2ca02c',      # Green
        'In Progress': '#1f77b4',    # Blue
        'Pending': '#17becf',        # Light Blue
        'Aborted': '#d62728',        # Red
        'Skipped': '#7f7f7f',        # Gray
        'Paused': '#ff7f0e',         # Orange
        'Completed with Issues': '#e377c2',  # Pink
        'Verified': '#9467bd',       # Purple
        'Reworked': '#bcbd22'        # Yellow
    }
    
    # Create color sequence based on statuses
    color_sequence = [status_colors.get(status, '#1f77b4') for status in execution_status['status']]
    
    fig = px.bar(execution_status, x='status', y='count',
                 color='status', color_discrete_sequence=color_sequence,
                 labels={'status': 'Execution Status', 'count': 'Count'})
    
    fig.update_layout(
        title="Batch Step Execution Status",
        xaxis_title="Status",
        yaxis_title="Count",
        margin=dict(t=40, b=40, l=10, r=10),
        height=400,
        showlegend=False
    )
    
    return fig

def create_parameter_control_modes_chart(metrics):
    """Create parameter control modes distribution chart"""
    if 'parameter_control_modes' not in metrics:
        return go.Figure()
    
    control_modes = metrics['parameter_control_modes']
    
    # Define colors for different modes
    mode_colors = {
        'Auto': '#2ca02c',       # Green
        'Manual': '#d62728',     # Red
        'Cascade': '#ff7f0e',    # Orange
        'Remote': '#1f77b4',     # Blue
        'Supervisory': '#9467bd' # Purple
    }
    
    # Create color sequence based on modes
    color_sequence = [mode_colors.get(mode, '#1f77b4') for mode in control_modes['mode']]
    
    fig = px.pie(control_modes, values='count', names='mode',
                 color='mode', color_discrete_sequence=color_sequence)
    
    fig.update_layout(
        title="Process Parameter Control Modes",
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        margin=dict(t=40, b=40, l=10, r=10),
        height=400
    )
    
    return fig

# Set up the Dash application
def create_dashboard(datasets, metrics):
    """Create a Dash dashboard to visualize the metrics"""
    # Initialize the Dash app
    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Define the layout
    app.layout = dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.H1("ISA-95 Level 2 Process Monitoring Dashboard", className="text-center mb-4"),
                html.H5("Production Management and Control", className="text-center text-muted mb-5")
            ], width=12)
        ]),
        
        # Top metrics cards - Row 1
        dbc.Row([
            # Equipment Health
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Equipment Health", className="card-title"),
                        html.H3(f"{metrics.get('equipment_health_rate', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Equipment Uptime
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Equipment Uptime", className="card-title"),
                        html.H3(f"{metrics.get('uptime_percentage', 0):.1f}%", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Average Alarm Response
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Avg Alarm Response", className="card-title"),
                        html.H3(f"{metrics.get('avg_alarm_ack_time', 0):.1f} min", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Parameter Deviation
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Parameter Deviation", className="card-title"),
                        html.H3(f"{metrics.get('avg_absolute_deviation', 0):.2f}", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3)
        ]),
        
        # Top metrics cards - Row 2
        dbc.Row([
            # Average Batch Duration
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Avg Batch Duration", className="card-title"),
                        html.H3(f"{metrics.get('avg_batch_duration', 0):.1f} min", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Average Step Duration
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Avg Step Duration", className="card-title"),
                        html.H3(f"{metrics.get('avg_execution_duration', 0):.1f} min", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Control Limit Violations
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Control Limit Violations", className="card-title"),
                        html.H3(f"{metrics.get('Upper Control Limit Violations', 0) + metrics.get('Lower Control Limit Violations', 0)}", 
                               className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3),
            
            # Total Equipment
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Total Equipment", className="card-title"),
                        html.H3(f"{metrics.get('total_equipment', 0)}", className="card-text text-primary")
                    ])
                ], className="mb-4 text-center")
            ], width=3)
        ]),
        
        # Equipment Section
        dbc.Row([
            dbc.Col([
                html.H3("Equipment Analysis", className="text-center mb-4")
            ], width=12)
        ]),
        
        # Equipment Analysis
        dbc.Row([
            # Equipment Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Equipment Types"),
                    dbc.CardBody(
                        dcc.Graph(id='equipment-types', figure=create_equipment_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Equipment Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Equipment Status"),
                    dbc.CardBody(
                        dcc.Graph(id='equipment-status', figure=create_equipment_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Equipment State Analysis
        dbc.Row([
            # Equipment State Distribution
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Equipment State Distribution"),
                    dbc.CardBody(
                        dcc.Graph(id='equipment-states', figure=create_equipment_states_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Equipment State Changes Over Time
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Equipment State Changes Over Time"),
                    dbc.CardBody(
                        dcc.Graph(id='hourly-state-changes', figure=create_hourly_state_changes_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Alarms Section
        dbc.Row([
            dbc.Col([
                html.H3("Alarm Analysis", className="text-center mb-4")
            ], width=12)
        ]),
        
        # Alarm Analysis
        dbc.Row([
            # Alarm Types
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Alarm Types"),
                    dbc.CardBody(
                        dcc.Graph(id='alarm-types', figure=create_alarm_types_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Alarm Priorities
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Alarm Priorities"),
                    dbc.CardBody(
                        dcc.Graph(id='alarm-priorities', figure=create_alarm_priorities_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Hourly Alarms
        dbc.Row([
            # Alarm Frequency Over Time
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Alarm Frequency Over Time"),
                    dbc.CardBody(
                        dcc.Graph(id='hourly-alarms', figure=create_hourly_alarms_chart(metrics))
                    )
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Batch Section
        dbc.Row([
            dbc.Col([
                html.H3("Batch Analysis", className="text-center mb-4")
            ], width=12)
        ]),
        
        # Batch Analysis
        dbc.Row([
            # Batch Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Batch Status"),
                    dbc.CardBody(
                        dcc.Graph(id='batch-status', figure=create_batch_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Batches by Product
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Batches by Product"),
                    dbc.CardBody(
                        dcc.Graph(id='batch-products', figure=create_batch_products_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Batch Execution Analysis
        dbc.Row([
            # Execution Status
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Batch Step Execution Status"),
                    dbc.CardBody(
                        dcc.Graph(id='execution-status', figure=create_execution_status_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6),
            
            # Parameter Control Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Process Parameter Control Modes"),
                    dbc.CardBody(
                        dcc.Graph(id='parameter-control-modes', figure=create_parameter_control_modes_chart(metrics))
                    )
                ], className="mb-4")
            ], width=6)
        ]),
        
        # Step Parameters Table
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Process Parameter Samples"),
                    dbc.CardBody(
                        dash_table.DataTable(
                            id='parameter-table',
                            columns=[
                                {"name": "Parameter", "id": "parameter"},
                                {"name": "Value", "id": "value"}
                            ],
                            data=metrics.get('step_parameters_sample', pd.DataFrame()).to_dict('records'),
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px'
                            },
                            style_header={
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight': 'bold'
                            },
                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': 'rgb(248, 248, 248)'
                                }
                            ],
                            page_size=10
                        )
                    )
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Data Status Section
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Data Sources Status"),
                    dbc.CardBody([
                        html.Div([
                            html.Span("Equipment: ", className="fw-bold"),
                            html.Span("✓ Available" if 'equipment' in datasets else "✗ Not Available", 
                                    className="text-success" if 'equipment' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.Span("Equipment States: ", className="fw-bold"),
                            html.Span("✓ Available" if 'equipment_states' in datasets else "✗ Not Available", 
                                    className="text-success" if 'equipment_states' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.Span("Alarms: ", className="fw-bold"),
                            html.Span("✓ Available" if 'alarms' in datasets else "✗ Not Available", 
                                    className="text-success" if 'alarms' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.Span("Process Parameters: ", className="fw-bold"),
                            html.Span("✓ Available" if 'process_parameters' in datasets else "✗ Not Available", 
                                    className="text-success" if 'process_parameters' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.Span("Batches: ", className="fw-bold"),
                            html.Span("✓ Available" if 'batches' in datasets else "✗ Not Available", 
                                    className="text-success" if 'batches' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.Span("Batch Execution: ", className="fw-bold"),
                            html.Span("✓ Available" if 'batch_execution' in datasets else "✗ Not Available", 
                                    className="text-success" if 'batch_execution' in datasets else "text-danger"),
                        ], className="mb-2"),
                        html.Div([
                            html.P("Note: Missing data sources are supplemented with sample data for demonstration purposes.", 
                                  className="text-muted mt-3"),
                        ])
                    ])
                ], className="mb-4")
            ], width=12)
        ]),
        
        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P("ISA-95 Level 2 Process Monitoring Dashboard", className="text-center text-muted")
            ], width=12)
        ])
    ], fluid=True)
    
    return app

# Main function to run the dashboard
def main():
    """Main function to run the ISA-95 Level 2 dashboard"""
    # Load all data
    print("Loading data...")
    datasets = load_all_data()
    
    # Calculate metrics
    print("Calculating metrics...")
    metrics = calculate_metrics(datasets)
    
    # Create and run the dashboard
    print("Creating dashboard...")
    app = create_dashboard(datasets, metrics)
    
    print("Dashboard ready! Running on http://127.0.0.1:8061/")
    app.run_server(debug=True, port=8061)

if __name__ == "__main__":
    main()