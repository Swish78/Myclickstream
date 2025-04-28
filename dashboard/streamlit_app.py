import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession

# Initialize Spark session
def init_spark():
    return SparkSession.builder \
        .appName("Clickstream Dashboard") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

# Load data from S3
def load_data(spark, features_path, segments_path):
    features_df = spark.read.parquet(features_path).toPandas()
    segments_df = spark.read.parquet(segments_path).toPandas()
    return pd.merge(features_df, segments_df, on='user_id')

# Create visualizations
def create_segment_size_chart(data):
    segment_sizes = data['segment'].value_counts().reset_index()
    segment_sizes.columns = ['Segment', 'Count']
    
    fig = px.bar(
        segment_sizes,
        x='Segment',
        y='Count',
        title='User Segment Sizes',
        labels={'Count': 'Number of Users'}
    )
    return fig

def create_behavior_radar_chart(data):
    metrics = [
        'total_events', 'total_searches', 'total_cart_adds',
        'total_purchases', 'avg_purchase_amount'
    ]
    
    segment_means = data.groupby('segment')[metrics].mean()
    
    fig = go.Figure()
    for segment in segment_means.index:
        fig.add_trace(go.Scatterpolar(
            r=segment_means.loc[segment],
            theta=metrics,
            name=f'Segment {segment}'
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        showlegend=True,
        title='Segment Behavior Patterns'
    )
    return fig

def create_device_distribution(data):
    device_cols = ['desktop', 'mobile', 'tablet']
    device_means = data.groupby('segment')[device_cols].mean()
    
    fig = px.bar(
        device_means.reset_index(),
        x='segment',
        y=device_cols,
        title='Device Usage by Segment',
        labels={'value': 'Proportion', 'variable': 'Device Type'},
        barmode='group'
    )
    return fig

# Main dashboard
st.title('Clickstream User Segmentation Dashboard')

# Initialize session state
if 'data' not in st.session_state:
    spark = init_spark()
    st.session_state.data = load_data(
        spark,
        st.secrets['features_path'],
        st.secrets['segments_path']
    )
    spark.stop()

# Sidebar filters
st.sidebar.header('Filters')
selected_segments = st.sidebar.multiselect(
    'Select Segments',
    options=sorted(st.session_state.data['segment'].unique()),
    default=sorted(st.session_state.data['segment'].unique())
)

# Filter data
filtered_data = st.session_state.data[st.session_state.data['segment'].isin(selected_segments)]

# Display metrics
st.header('Key Metrics')
col1, col2, col3 = st.columns(3)

with col1:
    st.metric('Total Users', len(filtered_data))
with col2:
    st.metric('Average Purchase Value', f"${filtered_data['avg_purchase_amount'].mean():.2f}")
with col3:
    st.metric('Conversion Rate', f"{(filtered_data['total_purchases'] > 0).mean()*100:.1f}%")

# Display charts
st.plotly_chart(create_segment_size_chart(filtered_data))

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(create_behavior_radar_chart(filtered_data))
with col2:
    st.plotly_chart(create_device_distribution(filtered_data))

# Detailed segment analysis
st.header('Segment Details')
if st.checkbox('Show Detailed Segment Statistics'):
    st.dataframe(filtered_data.groupby('segment').agg({
        'total_events': 'mean',
        'total_searches': 'mean',
        'total_cart_adds': 'mean',
        'total_purchases': 'mean',
        'avg_purchase_amount': 'mean',
        'total_sessions': 'mean',
        'unique_pages_visited': 'mean'
    }).round(2))