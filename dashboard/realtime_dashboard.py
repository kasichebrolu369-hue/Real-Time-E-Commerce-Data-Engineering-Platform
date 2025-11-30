"""
Real-Time E-Commerce Analytics Dashboard
========================================

PURPOSE:
Interactive real-time dashboard for monitoring e-commerce metrics,
ML model predictions, and business KPIs.

FEATURES:
- Real-time business metrics
- Customer churn predictions
- Product recommendations
- Sales analytics
- Interactive visualizations
- Auto-refresh capability

USAGE:
streamlit run dashboard/realtime_dashboard.py --server.port 8501
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pickle
import sys
import os
from datetime import datetime, timedelta
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="E-Commerce Real-Time Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for beautiful styling
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');
    
    /* Global Styles */
    .main {
        background: #f5f7fa;
        font-family: 'Poppins', sans-serif;
    }
    
    /* Header Styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 30px;
        border-radius: 15px;
        text-align: center;
        margin-bottom: 30px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        animation: fadeIn 1s ease-in;
    }
    
    .main-header h1 {
        font-size: 3rem;
        font-weight: 700;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .main-header p {
        font-size: 1.2rem;
        margin-top: 10px;
        opacity: 0.9;
    }
    
    /* Metric Cards */
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #667eea;
    }
    
    div[data-testid="stMetricDelta"] {
        font-size: 1rem;
    }
    
    div[data-testid="metric-container"] {
        background: white;
        padding: 20px;
        border-radius: 15px;
        box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        border-left: 5px solid #667eea;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 10px 25px rgba(102, 126, 234, 0.3);
    }
    
    /* Section Headers */
    .section-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px 25px;
        border-radius: 10px;
        margin: 20px 0 15px 0;
        font-size: 1.5rem;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2);
    }
    
    /* Chart Containers */
    .stPlotlyChart {
        background: white;
        padding: 20px;
        border-radius: 15px;
        box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    
    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background: white;
        border-radius: 10px;
        padding: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 600;
        border: none;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
        box-shadow: 0 4px 10px rgba(102, 126, 234, 0.4);
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    section[data-testid="stSidebar"] .stMarkdown {
        color: white;
    }
    
    section[data-testid="stSidebar"] h1, 
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: white;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 25px;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2);
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 15px rgba(102, 126, 234, 0.4);
    }
    
    /* DataFrames */
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 10px rgba(0,0,0,0.1);
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 10px;
        border-left: 5px solid #667eea;
    }
    
    /* Animations */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes slideIn {
        from { transform: translateX(-100%); }
        to { transform: translateX(0); }
    }
    
    /* Divider */
    hr {
        margin: 30px 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #667eea, transparent);
    }
    
    /* Success/Warning/Error Messages */
    .element-container .stSuccess {
        background-color: #d4edda;
        border-left: 5px solid #28a745;
    }
    
    .element-container .stWarning {
        background-color: #fff3cd;
        border-left: 5px solid #ffc107;
    }
    
    .element-container .stError {
        background-color: #f8d7da;
        border-left: 5px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_transactions():
    """Load transaction data from parquet or CSV files"""
    try:
        data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        
        # Try to load from directory structure (CSV or Parquet)
        trans_dir = os.path.join(data_path, 'transactions')
        if os.path.exists(trans_dir):
            # Check for CSV files first
            csv_files = []
            for root, dirs, files in os.walk(trans_dir):
                csv_files.extend([os.path.join(root, f) for f in files if f.endswith('.csv')])
            
            if csv_files:
                # Load all CSV files
                dfs = [pd.read_csv(f) for f in csv_files]
                df = pd.concat(dfs, ignore_index=True)
            else:
                # Try Parquet
                try:
                    df = pd.read_parquet(trans_dir)
                except:
                    return None
        else:
            # Try single files
            csv_file = os.path.join(data_path, 'transactions.csv')
            parquet_file = os.path.join(data_path, 'transactions.parquet')
            
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                df = pd.read_parquet(parquet_file)
            else:
                return None
        
        # Convert timestamps
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        return df
    except Exception as e:
        st.error(f"Error loading transactions: {e}")
        return None

@st.cache_data(ttl=300)
def load_users():
    """Load user data from parquet or CSV files"""
    try:
        data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        
        users_dir = os.path.join(data_path, 'users')
        if os.path.exists(users_dir):
            # Check for CSV first
            csv_file = os.path.join(users_dir, 'users.csv')
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
            else:
                try:
                    df = pd.read_parquet(users_dir)
                except:
                    return None
        else:
            # Try single files
            csv_file = os.path.join(data_path, 'users.csv')
            parquet_file = os.path.join(data_path, 'users.parquet')
            
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                df = pd.read_parquet(parquet_file)
            else:
                return None
        
        return df
    except Exception as e:
        st.error(f"Error loading users: {e}")
        return None

@st.cache_data(ttl=300)
def load_products():
    """Load product data from parquet or CSV files"""
    try:
        data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        
        prod_dir = os.path.join(data_path, 'products')
        if os.path.exists(prod_dir):
            # Check for CSV first
            csv_file = os.path.join(prod_dir, 'products.csv')
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
            else:
                try:
                    df = pd.read_parquet(prod_dir)
                except:
                    return None
        else:
            # Try single files
            csv_file = os.path.join(data_path, 'products.csv')
            parquet_file = os.path.join(data_path, 'products.parquet')
            
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file)
            elif os.path.exists(parquet_file):
                df = pd.read_parquet(parquet_file)
            else:
                return None
        
        return df
    except Exception as e:
        st.error(f"Error loading products: {e}")
        return None

@st.cache_data(ttl=300)
def load_churn_predictions():
    """Load churn prediction results"""
    try:
        models_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        churn_file = os.path.join(models_path, 'churn_predictions.csv')
        
        if os.path.exists(churn_file):
            df = pd.read_csv(churn_file)
            return df
        else:
            return None
    except Exception as e:
        st.error(f"Error loading churn predictions: {e}")
        return None

@st.cache_data(ttl=300)
def load_recommendations():
    """Load recommendation results"""
    try:
        models_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models')
        rec_file = os.path.join(models_path, 'user_recommendations.csv')
        
        if os.path.exists(rec_file):
            df = pd.read_csv(rec_file)
            return df
        else:
            return None
    except Exception as e:
        st.error(f"Error loading recommendations: {e}")
        return None

# ============================================================================
# ANALYTICS FUNCTIONS
# ============================================================================

def calculate_kpis(df_transactions, df_users):
    """Calculate key performance indicators"""
    if df_transactions is None or len(df_transactions) == 0:
        return None
    
    # Filter completed transactions
    completed = df_transactions[df_transactions['status'] == 'completed']
    
    kpis = {
        'total_revenue': completed['total_amount'].sum(),
        'total_transactions': len(completed),
        'avg_order_value': completed['total_amount'].mean(),
        'total_customers': df_transactions['user_id'].nunique(),
        'total_products_sold': completed['quantity'].sum(),
        'completion_rate': (len(completed) / len(df_transactions)) * 100 if len(df_transactions) > 0 else 0
    }
    
    # Recent activity (last 30 days)
    if 'transaction_date' in completed.columns:
        recent_date = completed['transaction_date'].max() - timedelta(days=30)
        recent = completed[completed['transaction_date'] >= recent_date]
        
        kpis['recent_revenue'] = recent['total_amount'].sum()
        kpis['recent_transactions'] = len(recent)
        kpis['recent_customers'] = recent['user_id'].nunique()
    
    return kpis

def get_daily_revenue(df):
    """Calculate daily revenue trend"""
    if df is None or 'transaction_date' not in df.columns:
        return None
    
    completed = df[df['status'] == 'completed'].copy()
    completed['date'] = completed['transaction_date'].dt.date
    
    daily = completed.groupby('date').agg({
        'total_amount': 'sum',
        'transaction_id': 'count',
        'user_id': 'nunique'
    }).reset_index()
    
    daily.columns = ['date', 'revenue', 'transactions', 'customers']
    return daily

def get_category_performance(df_trans, df_products):
    """Analyze performance by product category"""
    if df_trans is None:
        return None
    
    completed = df_trans[df_trans['status'] == 'completed'].copy()
    
    # Check if category exists in transactions directly
    if 'category' in completed.columns:
        category_stats = completed.groupby('category').agg({
            'total_amount': 'sum',
            'transaction_id': 'count',
            'quantity': 'sum'
        }).reset_index()
    elif df_products is not None and 'category' in df_products.columns:
        # Merge with products to get categories
        merged = completed.merge(df_products[['product_id', 'category']], on='product_id', how='left')
        if 'category' in merged.columns:
            category_stats = merged.groupby('category').agg({
                'total_amount': 'sum',
                'transaction_id': 'count',
                'quantity': 'sum'
            }).reset_index()
        else:
            return None
    else:
        return None
    
    category_stats.columns = ['category', 'revenue', 'transactions', 'units_sold']
    category_stats = category_stats.sort_values('revenue', ascending=False)
    
    return category_stats

def get_customer_segments(df_users, df_churn):
    """Analyze customer segments"""
    if df_users is None:
        return None
    
    segment_counts = df_users['customer_segment'].value_counts().reset_index()
    segment_counts.columns = ['segment', 'count']
    
    # Add churn risk if available
    if df_churn is not None and 'churn_risk' in df_churn.columns:
        churn_by_segment = df_churn.groupby('churn_risk')['user_id'].count().reset_index()
        churn_by_segment.columns = ['churn_risk', 'count']
        return segment_counts, churn_by_segment
    
    return segment_counts, None

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def plot_revenue_trend(daily_data):
    """Create enhanced revenue trend chart"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=daily_data['date'],
        y=daily_data['revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#667eea', width=4, shape='spline'),
        fill='tozeroy',
        fillcolor='rgba(102, 126, 234, 0.3)',
        marker=dict(size=8, color='#764ba2', line=dict(width=2, color='white')),
        hovertemplate='<b>Date:</b> %{x}<br><b>Revenue:</b> $%{y:,.0f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=dict(text='📈 Daily Revenue Trend', font=dict(size=20, color='#667eea', family='Poppins')),
        xaxis_title='Date',
        yaxis_title='Revenue ($)',
        hovermode='x unified',
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', size=12),
        xaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)'),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)')
    )
    
    return fig

def plot_category_performance(category_data):
    """Create enhanced category performance chart"""
    colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#30cfd0']
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=category_data['category'],
        y=category_data['revenue'],
        marker=dict(
            color=colors[:len(category_data)],
            line=dict(color='white', width=2)
        ),
        text=category_data['revenue'].apply(lambda x: f'${x/1e6:.1f}M' if x >= 1e6 else f'${x/1e3:.0f}K'),
        textposition='outside',
        textfont=dict(size=14, color='#667eea', family='Poppins', weight='bold'),
        hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<br>Transactions: %{customdata[0]:,}<extra></extra>',
        customdata=category_data[['transactions']].values
    ))
    
    fig.update_layout(
        title=dict(text='📦 Revenue by Category', font=dict(size=20, color='#667eea', family='Poppins')),
        xaxis_title='Category',
        yaxis_title='Revenue ($)',
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', size=12),
        xaxis=dict(tickangle=-45),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)')
    )
    
    return fig

def plot_customer_segments(segment_data):
    """Create enhanced customer segment donut chart"""
    colors = ['#667eea', '#764ba2', '#f093fb', '#4facfe']
    
    fig = go.Figure(data=[go.Pie(
        labels=segment_data['segment'],
        values=segment_data['count'],
        hole=0.5,
        marker=dict(colors=colors, line=dict(color='white', width=3)),
        textposition='outside',
        textinfo='label+percent',
        textfont=dict(size=14, family='Poppins'),
        hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title=dict(text='👥 Customer Segments Distribution', font=dict(size=20, color='#667eea', family='Poppins')),
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', size=12),
        annotations=[dict(text='Segments', x=0.5, y=0.5, font_size=20, showarrow=False, font=dict(color='#667eea', family='Poppins'))]
    )
    
    return fig

def plot_churn_risk(churn_data):
    """Create churn risk visualization"""
    if churn_data is None or 'churn_risk' not in churn_data.columns:
        return None
    
    risk_counts = churn_data['churn_risk'].value_counts().reset_index()
    risk_counts.columns = ['churn_risk', 'count']
    
    colors = {'High': '#dc3545', 'Medium': '#ffc107', 'Low': '#28a745'}
    emojis = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}
    
    fig = go.Figure()
    
    for risk in ['High', 'Medium', 'Low']:
        data = risk_counts[risk_counts['churn_risk'] == risk]
        if len(data) > 0:
            fig.add_trace(go.Bar(
                x=[f"{emojis[risk]} {risk}"],
                y=data['count'].values,
                name=risk,
                marker=dict(
                    color=colors.get(risk, '#1f77b4'),
                    line=dict(color='white', width=2)
                ),
                text=data['count'].values,
                textposition='outside',
                textfont=dict(size=16, color=colors[risk], family='Poppins', weight='bold'),
                hovertemplate=f'<b>{risk} Risk</b><br>Customers: %{{y:,}}<extra></extra>'
            ))
    
    fig.update_layout(
        title=dict(text='🔮 Churn Risk Distribution', font=dict(size=20, color='#667eea', family='Poppins')),
        xaxis_title='Risk Level',
        yaxis_title='Number of Customers',
        showlegend=False,
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', size=12),
        yaxis=dict(showgrid=True, gridwidth=1, gridcolor='rgba(0,0,0,0.1)')
    )
    
    return fig

def plot_funnel(df):
    """Create enhanced conversion funnel"""
    if df is None:
        return None
    
    status_counts = df['status'].value_counts()
    
    stages = ['completed', 'pending', 'cancelled']
    labels = ['✅ Completed', '⏳ Pending', '❌ Cancelled']
    values = [status_counts.get(stage, 0) for stage in stages]
    colors = ["#28a745", "#ffc107", "#dc3545"]
    
    fig = go.Figure(go.Funnel(
        y=labels,
        x=values,
        textposition="inside",
        textinfo="value+percent initial",
        textfont=dict(size=14, color='white', family='Poppins', weight='bold'),
        marker=dict(
            color=colors,
            line=dict(color='white', width=3)
        ),
        connector={"line": {"color": "#667eea", "width": 3}},
        hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percentage: %{percentInitial}<extra></extra>'
    ))
    
    fig.update_layout(
        title=dict(text='🎯 Transaction Funnel', font=dict(size=20, color='#667eea', family='Poppins')),
        height=450,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Poppins', size=12)
    )
    
    return fig

# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Animated Header
    st.markdown('''
    <div class="main-header">
        <h1>🛒 E-Commerce Analytics Hub</h1>
        <p>Real-Time Business Intelligence & Predictive Insights</p>
    </div>
    ''', unsafe_allow_html=True)
    
    # Enhanced Sidebar
    st.sidebar.markdown("## 🎛️ Dashboard Controls")
    st.sidebar.markdown("---")
    
    # Auto-refresh option with better styling
    st.sidebar.markdown("### ⚡ Live Updates")
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=False)
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Date filter
    st.sidebar.markdown("### 📅 Filters")
    
    # Load data
    with st.spinner("Loading data..."):
        df_transactions = load_transactions()
        df_users = load_users()
        df_products = load_products()
        df_churn = load_churn_predictions()
        df_recommendations = load_recommendations()
    
    if df_transactions is None:
        st.error("⚠️ No transaction data found. Please run data generators first.")
        st.stop()
    
    # Date range filter
    if 'transaction_date' in df_transactions.columns:
        min_date = df_transactions['transaction_date'].min().date()
        max_date = df_transactions['transaction_date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            df_transactions = df_transactions[
                (df_transactions['transaction_date'].dt.date >= date_range[0]) &
                (df_transactions['transaction_date'].dt.date <= date_range[1])
            ]
    
    # Category filter
    if df_products is not None and 'category' in df_products.columns:
        categories = ['All'] + sorted(df_products['category'].unique().tolist())
        selected_category = st.sidebar.selectbox("Category", categories)
        
        if selected_category != 'All':
            product_ids = df_products[df_products['category'] == selected_category]['product_id'].tolist()
            df_transactions = df_transactions[df_transactions['product_id'].isin(product_ids)]
    
    # Calculate KPIs
    kpis = calculate_kpis(df_transactions, df_users)
    
    # ========================================================================
    # KEY METRICS WITH ENHANCED STYLING
    # ========================================================================
    
    st.markdown('<div class="section-header">📊 Key Performance Indicators</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="💰 Total Revenue",
            value=f"${kpis['total_revenue']/1e9:.2f}B" if kpis['total_revenue'] >= 1e9 else f"${kpis['total_revenue']/1e6:.1f}M",
            delta=f"${kpis.get('recent_revenue', 0)/1e6:.1f}M (30d)"
        )
    
    with col2:
        st.metric(
            label="🛍️ Total Orders",
            value=f"{kpis['total_transactions']/1000:.1f}K" if kpis['total_transactions'] >= 1000 else f"{kpis['total_transactions']:,}",
            delta=f"{kpis.get('recent_transactions', 0)/1000:.1f}K (30d)"
        )
    
    with col3:
        st.metric(
            label="📈 Avg Order Value",
            value=f"${kpis['avg_order_value']/1000:.1f}K" if kpis['avg_order_value'] >= 1000 else f"${kpis['avg_order_value']:.0f}"
        )
    
    with col4:
        st.metric(
            label="👥 Total Customers",
            value=f"{kpis['total_customers']/1000:.1f}K" if kpis['total_customers'] >= 1000 else f"{kpis['total_customers']:,}",
            delta=f"{kpis.get('recent_customers', 0)/1000:.1f}K (30d)"
        )
    
    with col5:
        st.metric(
            label="✅ Completion Rate",
            value=f"{kpis['completion_rate']:.1f}%",
            delta=f"{kpis['completion_rate'] - 68:.1f}% vs industry" if kpis['completion_rate'] > 68 else None
        )
    
    st.markdown("---")
    
    # ========================================================================
    # MAIN CHARTS WITH ENHANCED LAYOUT
    # ========================================================================
    
    st.markdown('<div class="section-header">📊 Performance Analytics</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue trend
        daily_data = get_daily_revenue(df_transactions)
        if daily_data is not None:
            fig = plot_revenue_trend(daily_data)
            st.plotly_chart(fig, width='stretch')
    
    with col2:
        # Transaction funnel
        fig = plot_funnel(df_transactions)
        if fig:
            st.plotly_chart(fig, width='stretch')
    
    st.markdown("---")
    
    # ========================================================================
    # CATEGORY & CUSTOMER ANALYSIS WITH ENHANCED LAYOUT
    # ========================================================================
    
    st.markdown('<div class="section-header">🎯 Business Insights</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Category performance
        category_data = get_category_performance(df_transactions, df_products)
        if category_data is not None:
            fig = plot_category_performance(category_data)
            st.plotly_chart(fig, width='stretch')
    
    with col2:
        # Customer segments
        if df_users is not None:
            segment_data, _ = get_customer_segments(df_users, df_churn)
            if segment_data is not None:
                fig = plot_customer_segments(segment_data)
                st.plotly_chart(fig, width='stretch')
    
    st.markdown("---")
    
    # ========================================================================
    # ML MODELS SECTION WITH ENHANCED STYLING
    # ========================================================================
    
    st.markdown('<div class="section-header">🤖 Machine Learning Insights</div>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["🔮 Churn Prediction", "🎯 Recommendations"])
    
    with tab1:
        if df_churn is not None and 'churn_risk' in df_churn.columns:
            col1, col2, col3 = st.columns(3)
            
            high_risk = len(df_churn[df_churn['churn_risk'] == 'High'])
            medium_risk = len(df_churn[df_churn['churn_risk'] == 'Medium'])
            low_risk = len(df_churn[df_churn['churn_risk'] == 'Low'])
            
            with col1:
                st.metric("🔴 High Risk", f"{high_risk:,}", help="Customers likely to churn")
            with col2:
                st.metric("🟡 Medium Risk", f"{medium_risk:,}", help="Customers showing churn signals")
            with col3:
                st.metric("🟢 Low Risk", f"{low_risk:,}", help="Engaged customers")
            
            # Churn risk chart
            fig = plot_churn_risk(df_churn)
            if fig:
                st.plotly_chart(fig, width='stretch')
            
            # High-risk customers table
            st.subheader("⚠️ High-Risk Customers (Top 10)")
            high_risk_users = df_churn[df_churn['churn_risk'] == 'High'].nlargest(10, 'churn_probability')
            
            if len(high_risk_users) > 0:
                # Show available columns
                available_cols = ['user_id', 'churn_probability', 'churn_risk']
                display_cols = [col for col in available_cols if col in high_risk_users.columns]
                
                if display_cols:
                    display_df = high_risk_users[display_cols].copy()
                    # Format probability as percentage
                    if 'churn_probability' in display_df.columns:
                        display_df['churn_probability'] = display_df['churn_probability'].apply(lambda x: f"{x*100:.1f}%")
                    st.dataframe(display_df, width='stretch')
        else:
            st.info("🔄 Churn prediction model not found. Run `python scripts/ml/train_churn_prediction.py` first.")
    
    with tab2:
        if df_recommendations is not None:
            st.metric("✅ Users with Recommendations", f"{df_recommendations['user_id'].nunique():,}")
            
            # Search for user recommendations
            st.subheader("🔍 Search User Recommendations")
            
            if 'user_id' in df_recommendations.columns:
                user_ids = sorted(df_recommendations['user_id'].unique())
                selected_user = st.selectbox("Select User ID", user_ids[:100])  # Show first 100
                
                if selected_user:
                    user_recs = df_recommendations[df_recommendations['user_id'] == selected_user]
                    
                    if len(user_recs) > 0:
                        st.write(f"**Top Recommendations for {selected_user}:**")
                        
                        # Get product details if available
                        if df_products is not None and 'recommended_products' in user_recs.columns:
                            # Parse recommended products
                            rec_list = user_recs.iloc[0]['recommended_products']
                            if isinstance(rec_list, str):
                                rec_list = rec_list.strip('[]').replace("'", "").split(', ')
                            
                            for idx, prod_id in enumerate(rec_list[:10], 1):
                                prod_info = df_products[df_products['product_id'] == prod_id]
                                if len(prod_info) > 0:
                                    prod = prod_info.iloc[0]
                                    st.write(f"{idx}. **{prod['product_name']}** - ${prod['price']:.2f} ({prod['category']})")
                                else:
                                    st.write(f"{idx}. {prod_id}")
        else:
            st.info("🔄 Recommendations not found. Run `python scripts/ml/train_recommendations.py` first.")
    
    st.divider()
    
    # ========================================================================
    # ENHANCED FOOTER
    # ========================================================================
    
    st.markdown("---")
    
    # Footer with beautiful cards
    footer_col1, footer_col2, footer_col3, footer_col4 = st.columns(4)
    
    with footer_col1:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center;'>
            <h3 style='margin: 0;'>📊</h3>
            <p style='margin: 5px 0;'><b>Data Source</b></p>
            <p style='margin: 0; font-size: 0.9rem;'>Real-time CSV/Parquet</p>
        </div>
        """, unsafe_allow_html=True)
    
    with footer_col2:
        st.markdown(f"""
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center;'>
            <h3 style='margin: 0;'>🕒</h3>
            <p style='margin: 5px 0;'><b>Last Updated</b></p>
            <p style='margin: 0; font-size: 0.9rem;'>{datetime.now().strftime('%H:%M:%S')}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with footer_col3:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center;'>
            <h3 style='margin: 0;'>🤖</h3>
            <p style='margin: 5px 0;'><b>ML Models</b></p>
            <p style='margin: 0; font-size: 0.9rem;'>Random Forest + NMF</p>
        </div>
        """, unsafe_allow_html=True)
    
    with footer_col4:
        st.markdown(f"""
        <div style='background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center;'>
            <h3 style='margin: 0;'>📝</h3>
            <p style='margin: 5px 0;'><b>Records</b></p>
            <p style='margin: 0; font-size: 0.9rem;'>{kpis['total_transactions']:,} Transactions</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: #667eea; font-size: 0.9rem;'>✨ Built with Streamlit • Powered by Python & ML ✨</p>", unsafe_allow_html=True)

# ============================================================================
# RUN DASHBOARD
# ============================================================================

if __name__ == "__main__":
    main()
