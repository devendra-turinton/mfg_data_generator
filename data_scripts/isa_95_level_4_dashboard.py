import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback
import dash_bootstrap_components as dbc
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')

# Function to load all datasets
def load_all_data():
    """Load all available ISA-95 Level 4 datasets and return a dictionary of dataframes"""
    data_path = "data/"
    datasets = {}
    
    # List of all potential datasets
    dataset_files = [
        "products.csv",
        "materials.csv",
        "bill_of_materials.csv",
        "customers.csv",
        "customer_orders.csv",
        "order_lines.csv",
        "suppliers.csv",
        "purchase_orders.csv",
        "purchase_order_lines.csv",
        "personnel.csv",
        "facilities.csv",
        "storage_locations.csv",
        "shifts.csv",
        "inventory_transactions.csv",
        "material_lots.csv",
        "material_consumption.csv",
        "production_schedules.csv",
        "scheduled_production.csv",
        "costs.csv",
        "cogs.csv"
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
                # Convert date columns to datetime
                for col in df.columns:
                    if 'date' in col.lower() or 'time' in col.lower() or col == 'timestamp':
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except:
                            pass  # Skip if conversion fails
                # Store in dictionary
                datasets[dataset_name] = df
                print(f"Loaded {dataset_name} with {len(df)} records")
            except Exception as e:
                print(f"Error loading {file}: {e}")
    
    return datasets

# Calculate key metrics for dashboard
def calculate_metrics(datasets):
    """Calculate key metrics from the datasets for the dashboard"""
    metrics = {}
    
    # 1. Inventory value and metrics
    if 'material_lots' in datasets:
        material_lots = datasets['material_lots']
        if 'lot_quantity' in material_lots.columns and 'cost_per_unit' in material_lots.columns:
            # Convert to numeric and handle errors
            material_lots['lot_quantity_num'] = pd.to_numeric(material_lots['lot_quantity'], errors='coerce')
            material_lots['cost_per_unit_num'] = pd.to_numeric(material_lots['cost_per_unit'], errors='coerce')
            # Calculate total value
            material_lots['value'] = material_lots['lot_quantity_num'] * material_lots['cost_per_unit_num']
            total_value = material_lots['value'].sum()
            metrics['total_inventory_value'] = total_value
            
            # Inventory by status
            if 'status' in material_lots.columns:
                inventory_by_status = material_lots.groupby('status')['value'].sum().reset_index()
                metrics['inventory_by_status'] = inventory_by_status
            
            # Inventory by material type (join with materials table)
            if 'materials' in datasets and 'material_id' in material_lots.columns:
                materials = datasets['materials']
                if 'material_type' in materials.columns:
                    lot_materials = material_lots.merge(
                        materials[['material_id', 'material_type']], 
                        on='material_id', 
                        how='left'
                    )
                    inventory_by_type = lot_materials.groupby('material_type')['value'].sum().reset_index()
                    metrics['inventory_by_material_type'] = inventory_by_type
            
            # Calculate inventory turnover metrics
            if 'expiration_date' in material_lots.columns:
                material_lots['expiration_date'] = pd.to_datetime(material_lots['expiration_date'])
                material_lots['days_until_expiry'] = (material_lots['expiration_date'] - datetime.now()).dt.days
                
                # Near-expiry inventory (within 30 days)
                near_expiry = material_lots[material_lots['days_until_expiry'] <= 30]
                metrics['near_expiry_value'] = near_expiry['value'].sum()
                
                # Expired inventory
                expired = material_lots[material_lots['days_until_expiry'] < 0]
                metrics['expired_value'] = expired['value'].sum()
    
    # 2. Supply Chain Performance
    if 'purchase_orders' in datasets and 'purchase_order_lines' in datasets:
        purchase_orders = datasets['purchase_orders']
        po_lines = datasets['purchase_order_lines']
        
        # Purchase order performance
        if 'status' in purchase_orders.columns:
            po_status = purchase_orders['status'].value_counts().reset_index()
            po_status.columns = ['status', 'count']
            metrics['po_status'] = po_status
        
        # Supplier performance
        if 'suppliers' in datasets and 'supplier_id' in purchase_orders.columns:
            suppliers = datasets['suppliers']
            if 'quality_rating' in suppliers.columns:
                suppliers['quality_rating_num'] = pd.to_numeric(suppliers['quality_rating'], errors='coerce')
                avg_supplier_rating = suppliers['quality_rating_num'].mean()
                metrics['avg_supplier_rating'] = avg_supplier_rating
                
                # Top suppliers by order volume
                po_by_supplier = purchase_orders.groupby('supplier_id').size().reset_index(name='order_count')
                po_by_supplier = po_by_supplier.merge(
                    suppliers[['supplier_id', 'supplier_name', 'quality_rating_num']], 
                    on='supplier_id', 
                    how='left'
                )
                po_by_supplier = po_by_supplier.sort_values('order_count', ascending=False).head(10)
                metrics['top_suppliers'] = po_by_supplier
    
    # 3. Production Performance
    if 'scheduled_production' in datasets:
        production = datasets['scheduled_production']
        
        # Total scheduled production
        if 'scheduled_quantity' in production.columns:
            production['scheduled_quantity_num'] = pd.to_numeric(production['scheduled_quantity'], errors='coerce')
            total_production = production['scheduled_quantity_num'].sum()
            metrics['total_scheduled_production'] = total_production
            
            # Production status breakdown
            if 'status' in production.columns:
                production_by_status = production.groupby('status')['scheduled_quantity_num'].sum().reset_index()
                metrics['production_by_status'] = production_by_status
            
            # Production schedule adherence
            if 'production_schedules' in datasets:
                schedules = datasets['production_schedules']
                if 'status' in schedules.columns:
                    schedule_performance = schedules['status'].value_counts().reset_index()
                    schedule_performance.columns = ['status', 'count']
                    metrics['schedule_performance'] = schedule_performance
    
    # 4. Order fulfillment metrics
    if 'customer_orders' in datasets and 'order_lines' in datasets:
        orders = datasets['customer_orders']
        lines = datasets['order_lines']
        
        # Order status distribution
        if 'status' in orders.columns:
            order_status = orders['status'].value_counts().reset_index()
            order_status.columns = ['status', 'count']
            metrics['order_status'] = order_status
        
        # Calculate on-time delivery
        if 'promised_delivery_date' in lines.columns and 'shipping_date' in lines.columns:
            # Filter only shipped lines
            shipped_lines = lines[lines['shipped_quantity'] > 0].copy()
            if len(shipped_lines) > 0:
                # Calculate if shipped on time
                shipped_lines['on_time'] = shipped_lines['shipping_date'] <= shipped_lines['promised_delivery_date']
                on_time_rate = shipped_lines['on_time'].mean() * 100
                metrics['on_time_delivery_rate'] = on_time_rate
        
        # Order value analysis
        if 'order_value' in orders.columns:
            orders['order_value_num'] = pd.to_numeric(orders['order_value'], errors='coerce')
            total_order_value = orders['order_value_num'].sum()
            metrics['total_order_value'] = total_order_value
            
            # Average order value
            avg_order_value = orders['order_value_num'].mean()
            metrics['avg_order_value'] = avg_order_value
            
            # Order value by customer type
            if 'customers' in datasets and 'customer_id' in orders.columns:
                customers = datasets['customers']
                if 'customer_type' in customers.columns:
                    order_customers = orders.merge(
                        customers[['customer_id', 'customer_type']], 
                        on='customer_id', 
                        how='left'
                    )
                    order_value_by_type = order_customers.groupby('customer_type')['order_value_num'].sum().reset_index()
                    metrics['order_value_by_customer_type'] = order_value_by_type
    
    # 5. COGS metrics
    if 'cogs' in datasets:
        cogs = datasets['cogs']
        # Total COGS
        if 'total_cogs' in cogs.columns:
            cogs['total_cogs_num'] = pd.to_numeric(cogs['total_cogs'], errors='coerce')
            total_cogs = cogs['total_cogs_num'].sum()
            metrics['total_cogs'] = total_cogs
            
            # COGS breakdown
            cost_components = ['direct_materials_cost', 'direct_labor_cost', 
                              'manufacturing_overhead_cost', 'packaging_cost', 
                              'quality_cost', 'other_cost']
            
            cogs_breakdown = {}
            for component in cost_components:
                if component in cogs.columns:
                    cogs[f'{component}_num'] = pd.to_numeric(cogs[component], errors='coerce')
                    cogs_breakdown[component] = cogs[f'{component}_num'].sum()
            
            metrics['cogs_breakdown'] = cogs_breakdown
            
            # COGS by product
            if 'product_id' in cogs.columns:
                cogs_by_product = cogs.groupby('product_id')['total_cogs_num'].sum().reset_index()
                metrics['cogs_by_product'] = cogs_by_product
                
                # Try to get product names if available
                if 'products' in datasets and 'product_id' in datasets['products'].columns:
                    products = datasets['products']
                    product_info = products[['product_id', 'product_name', 'product_family']].drop_duplicates()
                    cogs_by_product = cogs_by_product.merge(product_info, on='product_id', how='left')
            
            # Calculate gross margin if revenue data available
            if 'total_order_value' in metrics and total_cogs > 0:
                gross_margin = ((metrics['total_order_value'] - total_cogs) / metrics['total_order_value']) * 100
                metrics['gross_margin'] = gross_margin
    
    # 6. Inventory transaction metrics
    if 'inventory_transactions' in datasets:
        transactions = datasets['inventory_transactions']
        
        # Transaction counts by type
        if 'transaction_type' in transactions.columns:
            transaction_counts = transactions['transaction_type'].value_counts().reset_index()
            transaction_counts.columns = ['transaction_type', 'count']
            metrics['transaction_counts'] = transaction_counts
            
            # Create timeline of transactions
            if 'timestamp' in transactions.columns:
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_dtype(transactions['timestamp']):
                    transactions['timestamp'] = pd.to_datetime(transactions['timestamp'], errors='coerce')
                
                # Group by date and transaction type
                transactions['date'] = transactions['timestamp'].dt.date
                timeline = transactions.groupby(['date', 'transaction_type']).size().reset_index(name='count')
                metrics['transaction_timeline'] = timeline
                
                # Calculate transaction velocity (transactions per day)
                date_range = (transactions['timestamp'].max() - transactions['timestamp'].min()).days
                if date_range > 0:
                    transaction_velocity = len(transactions) / date_range
                    metrics['transaction_velocity'] = transaction_velocity
    
    # 7. Material consumption metrics
    if 'material_consumption' in datasets:
        consumption = datasets['material_consumption']
        
        # Total consumption
        if 'quantity' in consumption.columns:
            consumption['quantity_num'] = pd.to_numeric(consumption['quantity'], errors='coerce')
            total_consumption = consumption['quantity_num'].sum()
            metrics['total_material_consumption'] = total_consumption
            
            # Consumption variance analysis
            if 'planned_consumption' in consumption.columns and 'consumption_variance' in consumption.columns:
                consumption['planned_consumption_num'] = pd.to_numeric(consumption['planned_consumption'], errors='coerce')
                consumption['consumption_variance_num'] = pd.to_numeric(consumption['consumption_variance'], errors='coerce')
                
                # Calculate variance percentage
                consumption['variance_pct'] = (consumption['consumption_variance_num'] / consumption['planned_consumption_num']) * 100
                avg_variance_pct = consumption['variance_pct'].mean()
                metrics['avg_consumption_variance_pct'] = avg_variance_pct
                
                # Consumption by work order
                if 'work_order_id' in consumption.columns:
                    consumption_by_wo = consumption.groupby('work_order_id').agg({
                        'quantity_num': 'sum',
                        'planned_consumption_num': 'sum',
                        'consumption_variance_num': 'sum'
                    }).reset_index()
                    consumption_by_wo = consumption_by_wo.sort_values('quantity_num', ascending=False).head(10)
                    metrics['top_consuming_work_orders'] = consumption_by_wo
    
    # 8. Cost metrics
    if 'costs' in datasets:
        costs = datasets['costs']
        
        # Total costs
        if 'amount' in costs.columns:
            costs['amount_num'] = pd.to_numeric(costs['amount'], errors='coerce')
            total_costs = costs['amount_num'].sum()
            metrics['total_costs'] = total_costs
            
            # Costs by type
            if 'cost_type' in costs.columns:
                costs_by_type = costs.groupby('cost_type')['amount_num'].sum().reset_index()
                metrics['costs_by_type'] = costs_by_type
                
            # Costs by cost center
            if 'cost_center' in costs.columns:
                costs_by_center = costs.groupby('cost_center')['amount_num'].sum().reset_index()
                metrics['costs_by_center'] = costs_by_center
            
            # Cost variance analysis
            if 'planned_cost' in costs.columns and 'variance' in costs.columns:
                costs['planned_cost_num'] = pd.to_numeric(costs['planned_cost'], errors='coerce')
                costs['variance_num'] = pd.to_numeric(costs['variance'], errors='coerce')
                
                # Calculate cost performance
                cost_variance_total = costs['variance_num'].sum()
                planned_cost_total = costs['planned_cost_num'].sum()
                if planned_cost_total > 0:
                    cost_variance_pct = (cost_variance_total / planned_cost_total) * 100
                    metrics['cost_variance_pct'] = cost_variance_pct
    
    # 9. Personnel metrics
    if 'personnel' in datasets:
        personnel = datasets['personnel']
        
        # Total personnel count
        metrics['total_personnel'] = len(personnel)
        
        # Personnel by department
        if 'department' in personnel.columns:
            personnel_by_dept = personnel['department'].value_counts().reset_index()
            personnel_by_dept.columns = ['department', 'count']
            metrics['personnel_by_department'] = personnel_by_dept
        
        # Personnel by status
        if 'status' in personnel.columns:
            personnel_by_status = personnel['status'].value_counts().reset_index()
            personnel_by_status.columns = ['status', 'count']
            metrics['personnel_by_status'] = personnel_by_status
            
            # Active personnel percentage
            active_pct = (personnel['status'] == 'Active').mean() * 100
            metrics['active_personnel_pct'] = active_pct
    
    # 10. Facility utilization metrics
    if 'facilities' in datasets and 'storage_locations' in datasets:
        facilities = datasets['facilities']
        locations = datasets['storage_locations']
        
        # Facility count by type
        if 'facility_type' in facilities.columns:
            facility_types = facilities['facility_type'].value_counts().reset_index()
            facility_types.columns = ['facility_type', 'count']
            metrics['facility_types'] = facility_types
        
        # Storage utilization
        if 'current_utilization' in locations.columns:
            locations['current_utilization_num'] = pd.to_numeric(locations['current_utilization'], errors='coerce')
            avg_utilization = locations['current_utilization_num'].mean()
            metrics['avg_storage_utilization'] = avg_utilization
            
            # Utilization by facility
            if 'facility_id' in locations.columns:
                util_by_facility = locations.groupby('facility_id')['current_utilization_num'].mean().reset_index()
                util_by_facility = util_by_facility.merge(
                    facilities[['facility_id', 'facility_name']], 
                    on='facility_id', 
                    how='left'
                )
                util_by_facility = util_by_facility.sort_values('current_utilization_num', ascending=False)
                metrics['utilization_by_facility'] = util_by_facility
    
    # 11. Quality metrics
    if 'material_lots' in datasets:
        lots = datasets['material_lots']
        
        # Quality status distribution
        if 'quality_status' in lots.columns:
            quality_distribution = lots['quality_status'].value_counts().reset_index()
            quality_distribution.columns = ['quality_status', 'count']
            metrics['quality_distribution'] = quality_distribution
            
            # Calculate quality rate
            if 'Released' in lots['quality_status'].values:
                quality_rate = (lots['quality_status'] == 'Released').mean() * 100
                metrics['quality_rate'] = quality_rate
    
    # 12. Bill of Materials complexity
    if 'bill_of_materials' in datasets:
        bom = datasets['bill_of_materials']
        
        # BOM complexity by product
        if 'product_id' in bom.columns:
            bom_complexity = bom.groupby('product_id').size().reset_index(name='component_count')
            
            # Add product info if available
            if 'products' in datasets:
                products = datasets['products']
                bom_complexity = bom_complexity.merge(
                    products[['product_id', 'product_name', 'product_family']], 
                    on='product_id', 
                    how='left'
                )
            
            bom_complexity = bom_complexity.sort_values('component_count', ascending=False).head(10)
            metrics['bom_complexity'] = bom_complexity
    
    return metrics

# Set up the Dash application
def create_dashboard(datasets, metrics):
    """Create a comprehensive Dash dashboard for ISA-95 Level 4"""
    # Initialize the Dash app
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Define the layout
    app.layout = dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.H1("ISA-95 Level 4 Business Planning & Logistics Dashboard", 
                       className="text-center mb-2"),
                html.H5("Comprehensive Manufacturing Operations Analytics", 
                       className="text-center text-muted mb-4")
            ], width=12)
        ]),
        
        # Navigation Tabs
        dbc.Tabs([
            # Executive Summary Tab
            dbc.Tab(label="Executive Summary", tab_id="executive", children=[
                html.Div([
                    # KPI Cards Row
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Inventory Value", className="card-title text-muted"),
                                    html.H3(f"${metrics.get('total_inventory_value', 0):,.2f}", 
                                           className="card-text text-primary")
                                ])
                            ], className="mb-3")
                        ], width=2),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total COGS", className="card-title text-muted"),
                                    html.H3(f"${metrics.get('total_cogs', 0):,.2f}", 
                                           className="card-text text-danger")
                                ])
                            ], className="mb-3")
                        ], width=2),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Order Value", className="card-title text-muted"),
                                    html.H3(f"${metrics.get('total_order_value', 0):,.2f}", 
                                           className="card-text text-success")
                                ])
                            ], className="mb-3")
                        ], width=2),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Gross Margin", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('gross_margin', 0):.1f}%", 
                                           className="card-text text-info")
                                ])
                            ], className="mb-3")
                        ], width=2),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("On-Time Delivery", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('on_time_delivery_rate', 0):.1f}%", 
                                           className="card-text text-success")
                                ])
                            ], className="mb-3")
                        ], width=2),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Quality Rate", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('quality_rate', 0):.1f}%", 
                                           className="card-text text-warning")
                                ])
                            ], className="mb-3")
                        ], width=2),
                    ]),
                    
                    # Main Charts
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Cost of Goods Sold Breakdown"),
                                dbc.CardBody(
                                    dcc.Graph(id='cogs-breakdown', figure=create_cogs_breakdown_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Order Status Distribution"),
                                dbc.CardBody(
                                    dcc.Graph(id='order-status', figure=create_order_status_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Revenue by Customer Type"),
                                dbc.CardBody(
                                    dcc.Graph(id='revenue-by-customer', 
                                            figure=create_revenue_by_customer_type_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=12)
                    ])
                ], className="mt-3")
            ]),
            
            # Inventory Management Tab
            dbc.Tab(label="Inventory Management", tab_id="inventory", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Inventory Value by Status"),
                                dbc.CardBody(
                                    dcc.Graph(id='inventory-status', 
                                            figure=create_inventory_status_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Inventory by Material Type"),
                                dbc.CardBody(
                                    dcc.Graph(id='inventory-type', 
                                            figure=create_inventory_by_type_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Inventory Transactions Timeline"),
                                dbc.CardBody(
                                    dcc.Graph(id='transaction-timeline', 
                                            figure=create_transaction_timeline(metrics))
                                )
                            ], className="mb-4")
                        ], width=12)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Inventory Aging Analysis"),
                                dbc.CardBody([
                                    html.H5(f"Near Expiry Value: ${metrics.get('near_expiry_value', 0):,.2f}", 
                                           className="text-warning"),
                                    html.H5(f"Expired Value: ${metrics.get('expired_value', 0):,.2f}", 
                                           className="text-danger"),
                                    html.Hr(),
                                    dcc.Graph(id='transaction-types', 
                                            figure=create_transaction_types_chart(metrics))
                                ])
                            ], className="mb-4")
                        ], width=12)
                    ])
                ], className="mt-3")
            ]),
            
            # Production Planning Tab
            dbc.Tab(label="Production Planning", tab_id="production", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Production Schedule Status"),
                                dbc.CardBody(
                                    dcc.Graph(id='schedule-status', 
                                            figure=create_schedule_status_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Production Status by Quantity"),
                                dbc.CardBody(
                                    dcc.Graph(id='production-status', 
                                            figure=create_production_status_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Material Consumption Analysis"),
                                dbc.CardBody([
                                    html.H5(f"Total Consumption: {metrics.get('total_material_consumption', 0):,.0f} units"),
                                    html.H5(f"Average Variance: {metrics.get('avg_consumption_variance_pct', 0):.1f}%"),
                                    html.Hr(),
                                    html.H6("Top Consuming Work Orders"),
                                    create_consumption_table(metrics)
                                ])
                            ], className="mb-4")
                        ], width=12)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("BOM Complexity Analysis"),
                                dbc.CardBody(
                                    dcc.Graph(id='bom-complexity', 
                                            figure=create_bom_complexity_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=12)
                    ])
                ], className="mt-3")
            ]),
            
            # Supply Chain Tab
            dbc.Tab(label="Supply Chain", tab_id="supply-chain", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Average Supplier Rating", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('avg_supplier_rating', 0):.1f}/5.0", 
                                           className="card-text text-primary")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Purchase Orders", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('po_status', pd.DataFrame())['count'].sum():,}", 
                                           className="card-text text-info")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Customers", className="card-title text-muted"),
                                    html.H3(f"{len(datasets.get('customers', [])):,}", 
                                           className="card-text text-success")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Average Order Value", className="card-title text-muted"),
                                    html.H3(f"${metrics.get('avg_order_value', 0):,.2f}", 
                                           className="card-text text-warning")
                                ])
                            ], className="mb-3")
                        ], width=3)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Purchase Order Status"),
                                dbc.CardBody(
                                    dcc.Graph(id='po-status', 
                                            figure=create_po_status_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Top Suppliers by Order Volume"),
                                dbc.CardBody(
                                    dcc.Graph(id='top-suppliers', 
                                            figure=create_top_suppliers_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ])
                ], className="mt-3")
            ]),
            
            # Cost Analysis Tab
            dbc.Tab(label="Cost Analysis", tab_id="cost", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Costs", className="card-title text-muted"),
                                    html.H3(f"${metrics.get('total_costs', 0):,.2f}", 
                                           className="card-text text-danger")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Cost Variance", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('cost_variance_pct', 0):.1f}%", 
                                           className="card-text text-warning")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Transaction Velocity", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('transaction_velocity', 0):.1f}/day", 
                                           className="card-text text-info")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Avg Storage Utilization", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('avg_storage_utilization', 0):.1f}%", 
                                           className="card-text text-success")
                                ])
                            ], className="mb-3")
                        ], width=3)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Costs by Type"),
                                dbc.CardBody(
                                    dcc.Graph(id='costs-by-type', 
                                            figure=create_costs_by_type_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Costs by Cost Center"),
                                dbc.CardBody(
                                    dcc.Graph(id='costs-by-center', 
                                            figure=create_costs_by_center_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Top Products by COGS"),
                                dbc.CardBody(
                                    dcc.Graph(id='top-products', 
                                            figure=create_top_products_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=12)
                    ])
                ], className="mt-3")
            ]),
            
            # Organization Tab
            dbc.Tab(label="Organization", tab_id="organization", children=[
                html.Div([
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Personnel", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('total_personnel', 0):,}", 
                                           className="card-text text-primary")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Active Personnel", className="card-title text-muted"),
                                    html.H3(f"{metrics.get('active_personnel_pct', 0):.1f}%", 
                                           className="card-text text-success")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Total Facilities", className="card-title text-muted"),
                                    html.H3(f"{len(datasets.get('facilities', [])):,}", 
                                           className="card-text text-info")
                                ])
                            ], className="mb-3")
                        ], width=3),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H6("Storage Locations", className="card-title text-muted"),
                                    html.H3(f"{len(datasets.get('storage_locations', [])):,}", 
                                           className="card-text text-warning")
                                ])
                            ], className="mb-3")
                        ], width=3)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Personnel by Department"),
                                dbc.CardBody(
                                    dcc.Graph(id='personnel-dept', 
                                            figure=create_personnel_by_dept_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6),
                        
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Facility Types"),
                                dbc.CardBody(
                                    dcc.Graph(id='facility-types', 
                                            figure=create_facility_types_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=6)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardHeader("Storage Utilization by Facility"),
                                dbc.CardBody(
                                    dcc.Graph(id='storage-utilization', 
                                            figure=create_storage_utilization_chart(metrics))
                                )
                            ], className="mb-4")
                        ], width=12)
                    ])
                ], className="mt-3")
            ])
        ], id="tabs", active_tab="executive"),
        
        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P("ISA-95 Level 4 Manufacturing Operations Dashboard - Business Planning & Logistics", 
                      className="text-center text-muted"),
                html.P(f"Data Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                      className="text-center text-muted small")
            ], width=12)
        ])
    ], fluid=True)
    
    return app

# Chart creation functions
def create_cogs_breakdown_chart(metrics):
    """Create COGS breakdown chart"""
    if 'cogs_breakdown' not in metrics:
        return go.Figure()
    
    cogs_breakdown = metrics['cogs_breakdown']
    labels = [label.replace('_cost', '').replace('_', ' ').title() for label in cogs_breakdown.keys()]
    values = list(cogs_breakdown.values())
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=.4,
        marker_colors=px.colors.qualitative.Set3
    )])
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_inventory_status_chart(metrics):
    """Create inventory by status chart"""
    if 'inventory_by_status' not in metrics:
        return go.Figure()
    
    inventory_by_status = metrics['inventory_by_status']
    
    fig = px.bar(inventory_by_status, x='status', y='value', 
                 color='status', color_discrete_sequence=px.colors.qualitative.Vivid,
                 labels={'status': 'Status', 'value': 'Value ($)'})
    
    fig.update_layout(
        xaxis_title="Status",
        yaxis_title="Value ($)",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_inventory_by_type_chart(metrics):
    """Create inventory by material type chart"""
    if 'inventory_by_material_type' not in metrics:
        return go.Figure()
    
    inventory_by_type = metrics['inventory_by_material_type']
    
    fig = px.pie(inventory_by_type, values='value', names='material_type',
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_order_status_chart(metrics):
    """Create order status chart"""
    if 'order_status' not in metrics:
        return go.Figure()
    
    order_status = metrics['order_status']
    
    fig = px.pie(order_status, values='count', names='status', 
                 color_discrete_sequence=px.colors.qualitative.Bold)
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_revenue_by_customer_type_chart(metrics):
    """Create revenue by customer type chart"""
    if 'order_value_by_customer_type' not in metrics:
        return go.Figure()
    
    revenue_data = metrics['order_value_by_customer_type']
    
    fig = px.bar(revenue_data, x='customer_type', y='order_value_num',
                 color='customer_type', color_discrete_sequence=px.colors.qualitative.Safe,
                 labels={'customer_type': 'Customer Type', 'order_value_num': 'Revenue ($)'})
    
    fig.update_layout(
        xaxis_title="Customer Type",
        yaxis_title="Revenue ($)",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_transaction_timeline(metrics):
    """Create transaction timeline chart"""
    if 'transaction_timeline' not in metrics:
        return go.Figure()
    
    timeline = metrics['transaction_timeline']
    timeline['date'] = pd.to_datetime(timeline['date'])
    timeline = timeline.sort_values('date')
    
    fig = px.line(timeline, x='date', y='count', color='transaction_type',
                  labels={'date': 'Date', 'count': 'Transaction Count', 'transaction_type': 'Type'},
                  color_discrete_sequence=px.colors.qualitative.Plotly)
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Transaction Count",
        legend_title="Transaction Type",
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_transaction_types_chart(metrics):
    """Create transaction types distribution chart"""
    if 'transaction_counts' not in metrics:
        return go.Figure()
    
    transaction_counts = metrics['transaction_counts']
    
    fig = px.bar(transaction_counts, x='transaction_type', y='count',
                 color='transaction_type', color_discrete_sequence=px.colors.qualitative.Alphabet,
                 labels={'transaction_type': 'Transaction Type', 'count': 'Count'})
    
    fig.update_layout(
        xaxis_title="Transaction Type",
        yaxis_title="Count",
        showlegend=False,
        height=300,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_schedule_status_chart(metrics):
    """Create production schedule status chart"""
    if 'schedule_performance' not in metrics:
        return go.Figure()
    
    schedule_data = metrics['schedule_performance']
    
    fig = px.pie(schedule_data, values='count', names='status',
                 color_discrete_sequence=px.colors.qualitative.Prism)
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_production_status_chart(metrics):
    """Create production status by quantity chart"""
    if 'production_by_status' not in metrics:
        return go.Figure()
    
    production_data = metrics['production_by_status']
    
    fig = px.bar(production_data, x='status', y='scheduled_quantity_num',
                 color='status', color_discrete_sequence=px.colors.qualitative.Vivid,
                 labels={'status': 'Status', 'scheduled_quantity_num': 'Quantity'})
    
    fig.update_layout(
        xaxis_title="Status",
        yaxis_title="Scheduled Quantity",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_consumption_table(metrics):
    """Create material consumption table"""
    if 'top_consuming_work_orders' not in metrics:
        return html.Div("No consumption data available")
    
    consumption_data = metrics['top_consuming_work_orders']
    
    table = dash_table.DataTable(
        id='consumption-table',
        columns=[
            {"name": "Work Order", "id": "work_order_id"},
            {"name": "Actual", "id": "quantity_num", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Planned", "id": "planned_consumption_num", "type": "numeric", "format": {"specifier": ",.0f"}},
            {"name": "Variance", "id": "consumption_variance_num", "type": "numeric", "format": {"specifier": ",.0f"}}
        ],
        data=consumption_data.to_dict('records'),
        style_cell={'textAlign': 'left'},
        style_data_conditional=[
            {
                'if': {'column_id': 'consumption_variance_num', 'filter_query': '{consumption_variance_num} > 0'},
                'color': 'red'
            },
            {
                'if': {'column_id': 'consumption_variance_num', 'filter_query': '{consumption_variance_num} < 0'},
                'color': 'green'
            }
        ],
        page_size=10
    )
    
    return table

def create_bom_complexity_chart(metrics):
    """Create BOM complexity chart"""
    if 'bom_complexity' not in metrics:
        return go.Figure()
    
    bom_data = metrics['bom_complexity']
    
    # Use product name if available, otherwise product ID
    if 'product_name' in bom_data.columns:
        labels = bom_data['product_name']
    else:
        labels = bom_data['product_id']
    
    fig = px.bar(bom_data, x=labels, y='component_count',
                 color='component_count', color_continuous_scale='Viridis',
                 labels={'component_count': 'Number of Components'})
    
    fig.update_layout(
        xaxis_title="Product",
        yaxis_title="Component Count",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig

def create_po_status_chart(metrics):
    """Create purchase order status chart"""
    if 'po_status' not in metrics:
        return go.Figure()
    
    po_status = metrics['po_status']
    
    fig = px.pie(po_status, values='count', names='status',
                 color_discrete_sequence=px.colors.qualitative.Light24)
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_top_suppliers_chart(metrics):
    """Create top suppliers chart"""
    if 'top_suppliers' not in metrics:
        return go.Figure()
    
    suppliers_data = metrics['top_suppliers']
    
    # Create a bar chart with quality rating as color
    fig = px.bar(suppliers_data, x='supplier_name', y='order_count',
                 color='quality_rating_num', color_continuous_scale='RdYlGn',
                 labels={'order_count': 'Order Count', 'quality_rating_num': 'Quality Rating'})
    
    fig.update_layout(
        xaxis_title="Supplier",
        yaxis_title="Order Count",
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig

def create_costs_by_type_chart(metrics):
    """Create costs by type chart"""
    if 'costs_by_type' not in metrics:
        return go.Figure()
    
    costs_by_type = metrics['costs_by_type']
    
    fig = px.bar(costs_by_type, x='cost_type', y='amount_num', 
                 color='cost_type', color_discrete_sequence=px.colors.qualitative.D3,
                 labels={'cost_type': 'Cost Type', 'amount_num': 'Amount ($)'})
    
    fig.update_layout(
        xaxis_title="Cost Type",
        yaxis_title="Amount ($)",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_costs_by_center_chart(metrics):
    """Create costs by cost center chart"""
    if 'costs_by_center' not in metrics:
        return go.Figure()
    
    costs_by_center = metrics['costs_by_center']
    
    fig = px.pie(costs_by_center, values='amount_num', names='cost_center',
                 color_discrete_sequence=px.colors.qualitative.Pastel2,
                 labels={'cost_center': 'Cost Center', 'amount_num': 'Amount ($)'})
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_top_products_chart(metrics):
    """Create top products by COGS chart"""
    if 'cogs_by_product' not in metrics:
        return go.Figure()
    
    cogs_by_product = metrics['cogs_by_product']
    cogs_by_product = cogs_by_product.sort_values('total_cogs_num', ascending=False).head(10)
    
    # Use product name if available, otherwise product ID
    if 'product_name' in cogs_by_product.columns:
        labels = cogs_by_product['product_name']
    else:
        labels = cogs_by_product['product_id']
    
    # Color by product family if available
    if 'product_family' in cogs_by_product.columns:
        color_col = 'product_family'
    else:
        color_col = labels
    
    fig = px.bar(cogs_by_product, x=labels, y='total_cogs_num',
                 color=color_col, color_discrete_sequence=px.colors.qualitative.Set2,
                 labels={'total_cogs_num': 'COGS Amount ($)'})
    
    fig.update_layout(
        xaxis_title="Product",
        yaxis_title="COGS Amount ($)",
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig

def create_personnel_by_dept_chart(metrics):
    """Create personnel by department chart"""
    if 'personnel_by_department' not in metrics:
        return go.Figure()
    
    personnel_data = metrics['personnel_by_department']
    
    fig = px.bar(personnel_data, x='department', y='count',
                 color='department', color_discrete_sequence=px.colors.qualitative.Antique,
                 labels={'department': 'Department', 'count': 'Personnel Count'})
    
    fig.update_layout(
        xaxis_title="Department",
        yaxis_title="Personnel Count",
        showlegend=False,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig

def create_facility_types_chart(metrics):
    """Create facility types chart"""
    if 'facility_types' not in metrics:
        return go.Figure()
    
    facility_data = metrics['facility_types']
    
    fig = px.pie(facility_data, values='count', names='facility_type',
                 color_discrete_sequence=px.colors.qualitative.Safe)
    
    fig.update_layout(
        showlegend=True,
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    return fig

def create_storage_utilization_chart(metrics):
    """Create storage utilization by facility chart"""
    if 'utilization_by_facility' not in metrics:
        return go.Figure()
    
    util_data = metrics['utilization_by_facility']
    
    # Create a horizontal bar chart for better readability
    fig = px.bar(util_data, y='facility_name', x='current_utilization_num',
                 orientation='h',
                 color='current_utilization_num', color_continuous_scale='RdYlGn_r',
                 labels={'facility_name': 'Facility', 'current_utilization_num': 'Utilization (%)'})
    
    fig.update_layout(
        xaxis_title="Utilization (%)",
        yaxis_title="Facility",
        height=400,
        margin=dict(t=20, b=20, l=20, r=20)
    )
    
    # Add a reference line at 80% utilization
    fig.add_vline(x=80, line_dash="dash", line_color="red", annotation_text="Target: 80%")
    
    return fig

# Main function to run the dashboard
def main():
    # Load all data
    print("Loading ISA-95 Level 4 data...")
    datasets = load_all_data()
    
    if not datasets:
        print("No data found! Please ensure data files are in the 'data' directory.")
        return
    
    # Calculate metrics
    print("Calculating comprehensive metrics...")
    metrics = calculate_metrics(datasets)
    
    # Create and run the dashboard
    print("Creating ISA-95 Level 4 dashboard...")
    app = create_dashboard(datasets, metrics)
    
    print("\n" + "="*60)
    print("ISA-95 Level 4 Dashboard ready!")
    print("Access the dashboard at: http://127.0.0.1:8063/")
    print("="*60 + "\n")
    
    app.run_server(debug=True, host='0.0.0.0', port=8063)

if __name__ == "__main__":
    main()