import streamlit as st
import pandas as pd 
import numpy as np
import dask.dataframe as dd 
import plotly.graph_objects as go
import plotly.express as px 
from src.data.processing import get_flight_paths 

st.set_page_config(layout="wide")

st.title('2021 U.S. Flight Delays')

@st.cache
def get_flights():
    flights = dd.read_parquet(
        'data/processed/flights.parquet',
        engine='pyarrow',
        columns=[
            'YEAR',
            'MONTH',
            'AIRLINE',
            'ORIGIN_NAME',
            'DEST_NAME',
            'ORIGIN_LON',
            'ORIGIN_LAT',
            'DEST_LON',
            'DEST_LAT',
            'DEP_DELAY'
        ]
    )
    return flights

@st.cache
def get_airlines(flights):
    return np.sort(flights['AIRLINE'].unique())

flights = get_flights()
airlines = get_airlines(flights)
months = {
    1: 'Jan',
    2: 'Feb',
    3: 'Mar',
    4: 'Apr',
    5: 'May',
    6: 'Jun',
    7: 'Jul',
    8: 'Aug',
    9: 'Sep',
    10: 'Oct',
    11: 'Nov',
    12: 'Dec'
}

col1, col2, col3 = st.columns([1, 3, 2])
with col1: 
    airline = st.selectbox(
        'Select Airline',
        options=airlines
    )
    month = st.select_slider(
        'Select Month',
        options=np.arange(1, 13),
        format_func=lambda x: months[x]
    )

@st.cache
def query(flights, airline, month):
    flights_filtered = flights[
        (flights['MONTH'] == month) & 
        (flights['AIRLINE'] == airline) 
    ].reset_index(drop=True)
    flight_paths = get_flight_paths(flights_filtered)
    flight_paths_json = flight_paths.compute().to_json(orient='split')
    return flight_paths_json 

@st.cache
def update_hist(flight_paths_json):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    fig_hist = px.histogram(
        flight_paths,
        x='AVG_DELAY',
        y='FLIGHTS',
        labels={'AVG_DELAY': 'avg. delay (min)', 'FLIGHTS': 'flights'},
        range_x=[-30, 100],
        height=750
    )
    fig_hist.layout.xaxis.fixedrange = True 
    fig_hist.layout.yaxis.fixedrange = True 
    return fig_hist 

flight_paths_json = query(flights, airline, month) 

@st.cache
def get_avg_delay_range(flight_paths_json):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    min, max = round(flight_paths['AVG_DELAY'].min()), round(flight_paths['AVG_DELAY'].max())
    return min, max

avg_delay_range = get_avg_delay_range(flight_paths_json) 

with col1: 
    selected_avg_delay_range = st.slider(
        'Select Avg. Delay Range',
        min_value=avg_delay_range[0],
        max_value=avg_delay_range[1],
        value=(avg_delay_range[0], avg_delay_range[1])
    )

def filter_flight_paths(flight_paths_json, selected_avg_delay_range):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    min_avg_delay, max_avg_delay = selected_avg_delay_range[0], selected_avg_delay_range[1]
    flight_paths_filtered = flight_paths[
        (flight_paths['AVG_DELAY'] >= min_avg_delay - 1) & 
        (flight_paths['AVG_DELAY'] <= max_avg_delay + 1)
    ].reset_index() 
    flight_paths_filtered_json = flight_paths_filtered.to_json(orient='split')
    return flight_paths_filtered_json 

flight_paths_filtered_json = filter_flight_paths(flight_paths_json, selected_avg_delay_range)

@st.cache
def update_map(flight_paths_json):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    fig_map = go.Figure()
    for i in range(len(flight_paths)):
        fig_map.add_trace(
            go.Scattergeo(
                locationmode='USA-states',
                lon=[flight_paths['ORIGIN_LON'][i], flight_paths['DEST_LON'][i]],
                lat=[flight_paths['ORIGIN_LAT'][i], flight_paths['DEST_LAT'][i]],
                mode='lines',
                hoverinfo='text',
                text='{0} - {1}<br>{2} flights<br>avg. delay: {3} minutes'.format(
                    flight_paths['ORIGIN_NAME'][i],
                    flight_paths['DEST_NAME'][i],
                    flight_paths['FLIGHTS'][i],
                    round(flight_paths['AVG_DELAY'][i])
                ),
                line={'width': 1, 'color': 'red'},
                opacity=0.1
            )
        )
    fig_map.update_layout(
        height=750,
        showlegend=False,
        geo={
            'scope': 'north america',
            'projection_type': 'azimuthal equal area',
            'showland': True,
            'landcolor': 'rgb(243, 243, 243)',
            'countrycolor': 'rgb(204, 204, 204)'
        } 
    )
    return fig_map

fig_hist = update_hist(flight_paths_json)
fig_map = update_map(flight_paths_filtered_json)

with col2:
    st.plotly_chart(fig_map) 
with col3:
    st.plotly_chart(fig_hist)

def summarize_flights(flight_paths_json, selected_avg_delay_range):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    num_flight_paths = len(flight_paths)
    num_flights = round(flight_paths['FLIGHTS'].sum())
    min_avg_delay, max_avg_delay = round(selected_avg_delay_range[0]), round(selected_avg_delay_range[1])
    summary = '{0} flight path(s) with an average delay between {1} and {2} minutes, representing {3} total flight(s)'.format(num_flight_paths, min_avg_delay, max_avg_delay, num_flights)
    return summary

flight_summary = summarize_flights(flight_paths_filtered_json, selected_avg_delay_range)
with col1:
    st.write(flight_summary)