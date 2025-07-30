import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_csv(file):
    try:
        df = pd.read_csv(file)
        
        required_cols = ['customer_id', 'month', 'units_consumed', 'peak_load_kw']
        if not all(col in df.columns for col in required_cols):
            st.error(f"❌ Missing required columns in {file.name}. Required: {required_cols}")
            return None
        
        df['units_consumed'] = pd.to_numeric(df['units_consumed'], errors='coerce')
        df['peak_load_kw'] = pd.to_numeric(df['peak_load_kw'], errors='coerce')
        
        if 'is_anomaly' in df.columns:
            df['is_anomaly'] = pd.to_numeric(df['is_anomaly'], errors='coerce')
        
        df = df.dropna(subset=['customer_id', 'month', 'units_consumed', 'peak_load_kw'])
        
        if df.empty:
            st.error(f"❌ No valid data found in {file.name}")
            return None
            
        return df
        
    except Exception as e:
        st.error(f"❌ Error reading {file.name}: {str(e)}")
        return None

def merge_data(csv_list):
    if not csv_list:
        return pd.DataFrame()
    
    combined_df = pd.concat(csv_list, ignore_index=True)
    
    try:
        combined_df['month_date'] = pd.to_datetime(combined_df['month'], format='%Y-%m')
    except:
        combined_df['month_date'] = pd.to_datetime(combined_df['month'], errors='coerce')
    
    combined_df = combined_df.dropna(subset=['month_date'])
    combined_df = combined_df.sort_values(['customer_id', 'month_date'])
    
    return combined_df

def analyze_customer_trends(df):
    if df.empty:
        return pd.DataFrame()
    
    trend_data = []
    
    for customer_id, customer_data in df.groupby('customer_id'):
        customer_data = customer_data.sort_values('month_date')
        
        if len(customer_data) < 2:
            continue
            
        for i in range(1, len(customer_data)):
            current_row = customer_data.iloc[i]
            prev_row = customer_data.iloc[i-1]
            
            current_units = current_row['units_consumed']
            prev_units = prev_row['units_consumed']
            current_peak_load = current_row['peak_load_kw']
            prev_peak_load = prev_row['peak_load_kw']
            
            units_change = current_units - prev_units
            units_change_pct = ((current_units - prev_units) / prev_units * 100) if prev_units > 0 else 0
            
            peak_load_change = current_peak_load - prev_peak_load
            peak_load_change_pct = ((current_peak_load - prev_peak_load) / prev_peak_load * 100) if prev_peak_load > 0 else 0
            
            is_suspicious = False
            reasons = []
            risk_level = "Low"
            
            if peak_load_change_pct <= -40:
                reasons.append("Significant peak load drop")
                is_suspicious = True
                risk_level = "High"
            
            if units_change_pct <= -40:
                reasons.append("Sudden drop in units consumed")
                is_suspicious = True
                if risk_level != "High":
                    risk_level = "High"
            
            if current_units < 50:
                reasons.append("Extremely low consumption")
                is_suspicious = True
                if risk_level == "Low":
                    risk_level = "Medium"
            
            if current_units > 200 and current_peak_load < 1.0:
                reasons.append("Peak load inconsistent with consumption")
                is_suspicious = True
                if risk_level == "Low":
                    risk_level = "Medium"
            
            if units_change_pct <= -25 and peak_load_change_pct <= -25:
                reasons.append("Both consumption and peak load dropped significantly")
                is_suspicious = True
                if risk_level == "Low":
                    risk_level = "Medium"
            
            trend_data.append({
                'customer_id': customer_id,
                'prev_month': prev_row['month'],
                'current_month': current_row['month'],
                'prev_units': prev_units,
                'current_units': current_units,
                'units_change': units_change,
                'units_change_pct': round(units_change_pct, 2),
                'prev_peak_load': prev_peak_load,
                'current_peak_load': current_peak_load,
                'peak_load_change': round(peak_load_change, 2),
                'peak_load_change_pct': round(peak_load_change_pct, 2),
                'is_suspicious': is_suspicious,
                'risk_level': risk_level,
                'reasons': ', '.join(reasons) if reasons else 'Normal'
            })
    
    return pd.DataFrame(trend_data)

def get_customer_summary_stats(df, trend_df):
    if df.empty or trend_df.empty:
        return pd.DataFrame(columns=[
            'customer_id', 'total_months', 'avg_units', 'avg_peak_load',
            'units_volatility', 'peak_load_volatility', 'suspicious_periods',
            'total_comparisons', 'overall_risk', 'latest_month', 'latest_units',
            'latest_peak_load'
        ])
        
    required_cols = ['customer_id', 'month', 'units_consumed', 'peak_load_kw']
    if not all(col in df.columns for col in required_cols):
        st.error("❌ Missing required columns in input data")
        return pd.DataFrame(columns=[
            'customer_id', 'total_months', 'avg_units', 'avg_peak_load',
            'units_volatility', 'peak_load_volatility', 'suspicious_periods',
            'total_comparisons', 'overall_risk', 'latest_month', 'latest_units',
            'latest_peak_load'
        ])
        
    customer_stats = []
    
    for customer_id in df['customer_id'].unique():
        customer_data = df[df['customer_id'] == customer_id].copy()
        
        if customer_id in trend_df['customer_id'].unique():
            customer_trends = trend_df[trend_df['customer_id'] == customer_id]
            
            total_months = len(customer_data)
            avg_units = customer_data['units_consumed'].mean()
            avg_peak_load = customer_data['peak_load_kw'].mean()
            
            suspicious_periods = customer_trends['is_suspicious'].sum() if 'is_suspicious' in customer_trends.columns else 0
            total_comparisons = len(customer_trends)
            
            if total_comparisons > 0:
                suspicious_ratio = suspicious_periods / total_comparisons
                if suspicious_ratio > 0.5:
                    overall_risk = "High"
                elif suspicious_ratio > 0.25:
                    overall_risk = "Medium"
                else:
                    overall_risk = "Low"
            else:
                overall_risk = "Low"
            
            units_std = customer_data['units_consumed'].std()
            peak_load_std = customer_data['peak_load_kw'].std()
            latest_data = customer_data.loc[customer_data['month'] == customer_data['month'].max()]
            
            if not latest_data.empty:
                customer_stats.append({
                    'customer_id': customer_id,
                    'total_months': total_months,
                    'avg_units': round(avg_units, 2),
                    'avg_peak_load': round(avg_peak_load, 2),
                    'units_volatility': round(units_std, 2),
                    'peak_load_volatility': round(peak_load_std, 2),
                    'suspicious_periods': suspicious_periods,
                    'total_comparisons': total_comparisons,
                    'overall_risk': overall_risk,
                    'latest_month': latest_data['month'].iloc[0],
                    'latest_units': latest_data['units_consumed'].iloc[0],
                    'latest_peak_load': latest_data['peak_load_kw'].iloc[0]
                })
    
    if not customer_stats:
        return pd.DataFrame(columns=[
            'customer_id', 'total_months', 'avg_units', 'avg_peak_load',
            'units_volatility', 'peak_load_volatility', 'suspicious_periods',
            'total_comparisons', 'overall_risk', 'latest_month', 'latest_units',
            'latest_peak_load'
        ])
        
    return pd.DataFrame(customer_stats)

def create_customer_usage_chart(df, customer_id):
    customer_data = df[df['customer_id'] == customer_id].copy().sort_values('month_date')
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Scatter(
            x=customer_data['month_date'],
            y=customer_data['units_consumed'],
            mode='lines+markers',
            name='Units Consumed',
            line=dict(color='blue', width=3),
            marker=dict(size=8)
        )
    )
    
    avg_usage = customer_data['units_consumed'].mean()
    fig.add_hline(
        y=avg_usage,
        line_dash="dash",
        line_color="orange",
        annotation_text=f"Average: {avg_usage:.1f} units",
        annotation_position="top right"
    )
    
    fig.update_layout(
        title=f'📊 Usage Trend - {customer_id}',
        xaxis_title='Month',
        yaxis_title='Units Consumed',
        height=400
    )
    
    return fig

def create_customer_risk_chart(trend_df, customer_id):
    customer_trends = trend_df[trend_df['customer_id'] == customer_id]
    
    if customer_trends.empty:
        return None
    
    colors = []
    for _, row in customer_trends.iterrows():
        if row['risk_level'] == 'High':
            colors.append('red')
        elif row['risk_level'] == 'Medium':
            colors.append('orange')
        else:
            colors.append('green')
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Bar(
            x=pd.to_datetime(customer_trends['current_month']),
            y=customer_trends['units_change_pct'],
            marker_color=colors,
            name='Units Change %'
        )
    )
    
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)
    fig.add_hline(y=-25, line_dash="dot", line_color="orange", annotation_text="Warning Threshold")
    fig.add_hline(y=-40, line_dash="dot", line_color="red", annotation_text="Critical Threshold")
    
    fig.update_layout(
        title=f'⚠️ Risk Analysis - {customer_id}',
        xaxis_title='Month',
        yaxis_title='Units Change (%)',
        height=400
    )
    
    return fig

def create_risk_assessment_chart(customer_stats):
    risk_counts = customer_stats['overall_risk'].value_counts()
    colors = {'Low': 'green', 'Medium': 'orange', 'High': 'red'}
    
    fig = go.Figure(data=[
        go.Pie(
            labels=risk_counts.index,
            values=risk_counts.values,
            marker_colors=[colors.get(risk, 'gray') for risk in risk_counts.index],
            textinfo='label+percent+value',
            textfont_size=14
        )
    ])
    
    fig.update_layout(
        title="Overall Risk Assessment Distribution",
        height=400,
        font=dict(size=12)
    )
    
    return fig

def display_interface():
    st.set_page_config(
        page_title="NBPDCL - Electricity Theft Detection System",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
    <style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        position: relative;
    }
    .nbpdcl-logo {
        position: absolute;
        top: 15px;
        right: 20px;
        background: rgba(255,255,255,0.2);
        padding: 8px 15px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.9rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #2a5298;
    }
    .risk-high { border-left-color: #dc3545 !important; }
    .risk-medium { border-left-color: #ffc107 !important; }
    .risk-low { border-left-color: #28a745 !important; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="main-header">
        <div class="nbpdcl-logo">NBPDCL</div>
        <h1>⚡ NBPDCL Electricity Theft Detection System</h1>
        <p>Advanced AI-powered analysis for North Bihar Power Distribution Company Limited</p>
        <p style="font-size: 0.9rem; opacity: 0.8; margin-top: 0.5rem;">
            🎓 Summer Internship Project 2025 | Intelligent detection of electricity consumption anomalies and potential theft
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("📁 NBPDCL Data Upload")
        st.markdown("Upload CSV files containing monthly electricity usage data from NBPDCL database")
        
        uploaded_files = st.file_uploader(
            "Choose CSV files",
            type=['csv'],
            accept_multiple_files=True,
            help="Upload multiple CSV files, each representing data for different months from NBPDCL system"
        )
        
        st.header("⚙️ NBPDCL Analysis Settings")
        st.markdown("Configure detection parameters for Bihar region")
        
        peak_load_threshold = st.slider(
            "Peak Load Drop Threshold (%)",
            min_value=20, max_value=60, value=40,
            help="Percentage drop in peak load to consider suspicious for NBPDCL customers"
        )
        
        units_threshold = st.slider(
            "Units Consumption Drop Threshold (%)",
            min_value=20, max_value=60, value=40,
            help="Percentage drop in units consumption to consider suspicious for NBPDCL analysis"
        )
        
        min_units_threshold = st.number_input(
            "Minimum Units Threshold",
            min_value=10, max_value=200, value=50,
            help="Minimum units below which consumption is considered extremely low for Bihar region"
        )
    
    if 'combined_data' not in st.session_state:
        st.session_state.combined_data = pd.DataFrame()
    if 'trend_analysis' not in st.session_state:
        st.session_state.trend_analysis = pd.DataFrame()
    if 'customer_stats' not in st.session_state:
        st.session_state.customer_stats = pd.DataFrame()
    if 'analysis_complete' not in st.session_state:
        st.session_state.analysis_complete = False
    
    if uploaded_files:
        with st.spinner("🔄 Processing uploaded files..."):
            valid_dfs = []
            
            for file in uploaded_files:
                df = load_csv(file)
                if df is not None:
                    valid_dfs.append(df)
                    st.sidebar.success(f"✅ {file.name} loaded successfully")
            
            if valid_dfs:
                st.session_state.combined_data = merge_data(valid_dfs)
                st.session_state.trend_analysis = analyze_customer_trends(st.session_state.combined_data)
                st.session_state.customer_stats = get_customer_summary_stats(
                    st.session_state.combined_data, 
                    st.session_state.trend_analysis
                )
                
                st.session_state.analysis_complete = True
                
                total_customers = st.session_state.combined_data['customer_id'].nunique()
                total_records = len(st.session_state.combined_data)
                unique_months = st.session_state.combined_data['month'].nunique()
                
                st.sidebar.markdown("### 📊 NBPDCL Data Summary")
                st.sidebar.metric("Files Loaded", len(valid_dfs))
                st.sidebar.metric("Total Records", total_records)
                st.sidebar.metric("NBPDCL Customers", total_customers)
                st.sidebar.metric("Analysis Months", unique_months)
    
    if st.session_state.analysis_complete:
        st.markdown("## 📊 NBPDCL Executive Summary")
        st.markdown("*Analysis results for North Bihar Power Distribution Company Limited*")
        
        col1, col2, col3, col4 = st.columns(4)
        
        total_customers = len(st.session_state.customer_stats)
        
        try:
            high_risk_customers = len(st.session_state.customer_stats[
                st.session_state.customer_stats['overall_risk'] == 'High'
            ])
            medium_risk_customers = len(st.session_state.customer_stats[
                st.session_state.customer_stats['overall_risk'] == 'Medium'
            ])
            
            if 'is_suspicious' in st.session_state.trend_analysis.columns:
                avg_suspicious_rate = (st.session_state.trend_analysis['is_suspicious'].sum() / 
                                     len(st.session_state.trend_analysis)) * 100
            else:
                avg_suspicious_rate = 0
        except Exception as e:
            st.error(f"Error calculating metrics: {str(e)}")
            high_risk_customers = 0
            medium_risk_customers = 0
            avg_suspicious_rate = 0

        with col1:
            st.metric(
                "Total NBPDCL Customers", 
                total_customers,
                help="Total number of NBPDCL customers analyzed"
            )
        
        with col2:
            st.metric(
                "High Risk Customers",
                high_risk_customers,
                delta=f"{(high_risk_customers/total_customers)*100:.1f}%" if total_customers > 0 else "0%",
                delta_color="inverse",
                help="NBPDCL customers with multiple suspicious patterns"
            )
        
        with col3:
            st.metric(
                "Medium Risk Customers",
                medium_risk_customers,
                delta=f"{(medium_risk_customers/total_customers)*100:.1f}%" if total_customers > 0 else "0%",
                delta_color="off",
                help="NBPDCL customers with some concerning patterns"
            )
        
        with col4:
            st.metric(
                "Avg Suspicious Rate",
                f"{avg_suspicious_rate:.1f}%",
                help="Percentage of month-to-month comparisons flagged as suspicious"
            )
        
        st.markdown("## 🎯 NBPDCL Risk Assessment Overview")
        st.markdown("*Risk distribution analysis for North Bihar customers*")
        
        risk_chart = create_risk_assessment_chart(st.session_state.customer_stats)
        st.plotly_chart(risk_chart, use_container_width=True)
        
        st.markdown("### 📋 Quick Stats")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.info(f"📈 **Total Data Points**\n{len(st.session_state.combined_data)} records")
        
        with col2:
            total_months = st.session_state.combined_data['month'].nunique()
            st.info(f"📅 **Analysis Period**\n{total_months} months")
        
        with col3:
            if high_risk_customers > 0:
                st.error(f"🚨 **Action Required**\n{high_risk_customers} high-risk customers")
            else:
                st.success("✅ **All Clear**\nNo high-risk customers")
        
        st.markdown("---")
        
        st.markdown("## 👥 NBPDCL Customer Database")
        st.markdown("*Customer risk analysis for Bihar region*")
        
        risk_filter = st.selectbox(
            "Filter by Risk Level:",
            ["All", "High", "Medium", "Low"]
        )
        
        filtered_stats = st.session_state.customer_stats.copy()
        if risk_filter != "All":
            filtered_stats = filtered_stats[filtered_stats['overall_risk'] == risk_filter]
        
        if not filtered_stats.empty:
            st.dataframe(
                filtered_stats[['customer_id', 'overall_risk', 'avg_units', 'suspicious_periods']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'customer_id': 'Customer ID',
                    'overall_risk': 'Risk Level',
                    'avg_units': 'Avg Units',
                    'suspicious_periods': 'Suspicious Periods'
                }
            )
        
        st.markdown("## 🔍 NBPDCL Customer Profile Analysis")
        st.markdown("*Detailed consumption analysis for individual NBPDCL customers*")
        
        selected_customer = st.selectbox(
            "Select NBPDCL customer to view detailed analysis:",
            options=[""] + sorted(st.session_state.combined_data['customer_id'].unique().tolist())
        )
        
        if selected_customer:
            customer_info = st.session_state.customer_stats[
                st.session_state.customer_stats['customer_id'] == selected_customer
            ].iloc[0]
            
            st.markdown(f"### 📊 NBPDCL Customer {selected_customer} - Profile Overview")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                risk_color = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
                st.metric(
                    "Risk Level",
                    f"{risk_color.get(customer_info['overall_risk'], '')} {customer_info['overall_risk']}"
                )
            
            with col2:
                st.metric("Total Months", customer_info['total_months'])
            
            with col3:
                st.metric("Suspicious Periods", customer_info['suspicious_periods'])
            
            with col4:
                st.metric("Average Units", f"{customer_info['avg_units']:.2f}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                usage_chart = create_customer_usage_chart(st.session_state.combined_data, selected_customer)
                st.plotly_chart(usage_chart, use_container_width=True)
            
            with col2:
                risk_chart = create_customer_risk_chart(st.session_state.trend_analysis, selected_customer)
                if risk_chart:
                    st.plotly_chart(risk_chart, use_container_width=True)
                else:
                    st.info("No trend data available for this customer")
            
            customer_trends = st.session_state.trend_analysis[
                st.session_state.trend_analysis['customer_id'] == selected_customer
            ]
            
            if not customer_trends.empty:
                st.markdown(f"### 📈 Recent Activity for {selected_customer}")
                
                st.dataframe(
                    customer_trends[['current_month', 'current_units', 'units_change_pct', 'risk_level', 'reasons']].head(5),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'current_month': 'Month',
                        'current_units': 'Units',
                        'units_change_pct': 'Change %',
                        'risk_level': 'Risk',
                        'reasons': 'Reasons'
                    }
                )
        
        st.markdown("## 📥 NBPDCL Export Reports")
        st.markdown("*Download analysis results for NBPDCL management and field teams*")
        
        col1, col2 = st.columns(2)
        
        with col1:
            csv_buffer_stats = io.StringIO()
            st.session_state.customer_stats.to_csv(csv_buffer_stats, index=False)
            
            st.download_button(
                label="📊 Download NBPDCL Customer Summary (CSV)",
                data=csv_buffer_stats.getvalue(),
                file_name=f"nbpdcl_customer_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            csv_buffer_trends = io.StringIO()
            st.session_state.trend_analysis.to_csv(csv_buffer_trends, index=False)
            
            st.download_button(
                label="🔍 Download NBPDCL Detailed Analysis (CSV)",
                data=csv_buffer_trends.getvalue(),
                file_name=f"nbpdcl_detailed_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    else:
        st.markdown("## 🚀 Getting Started")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            ### 📋 How to Use This System
            
            1. **📁 Upload Data**: Use the sidebar to upload CSV files containing monthly electricity usage data
            2. **👀 Review Summary**: View executive summary and risk assessment
            3. **🔍 Analyze Customers**: Select individual customers for detailed analysis
            4. **📥 Export Results**: Download analysis results for reporting
            
            ### 📊 Required Data Format
            Each CSV file must contain these columns:
            - `customer_id`: Unique identifier for each consumer
            - `month`: Format YYYY-MM (e.g., "2023-01")
            - `units_consumed`: Number of units consumed
            - `peak_load_kw`: Peak load in kilowatts
            - `is_anomaly` (optional): Pre-labeled anomaly flag (0 or 1)
            
            ### 🎯 Detection Criteria
            The system flags suspicious patterns based on:
            - **Peak Load Drop**: ≥40% decrease from previous month
            - **Consumption Drop**: ≥40% decrease in units from previous month
            - **Extremely Low Usage**: Less than 50 units consumed
            - **Load Inconsistency**: High consumption but very low peak load
            """)
        
        with col2:
            st.markdown("### 📋 Sample Data")
            sample_data = pd.DataFrame({
                'customer_id': ['CUST001', 'CUST001', 'CUST002', 'CUST002'],
                'month': ['2023-01', '2023-02', '2023-01', '2023-02'],
                'units_consumed': [320.69, 180.50, 450.25, 430.75],
                'peak_load_kw': [3.78, 2.10, 4.02, 3.85],
                'is_anomaly': [0, 1, 0, 0]
            })
            st.dataframe(sample_data, use_container_width=True, hide_index=True)

def main():
    display_interface()

if __name__ == "__main__":
    main()