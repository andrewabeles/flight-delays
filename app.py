from dash import Dash, dcc, html, Input, Output
import pandas as pd
import numpy as np
import dask.dataframe as dd
import plotly.graph_objects as go 
import plotly.express as px

app = Dash(__name__)
server = app.server

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

app.layout = html.Div([
    html.H1('2021 U.S. Flight Delays'),
    html.Div([
        dcc.Slider(
            1,
            12,
            step=None,
            value=1,
            marks={
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
            },
            id='month-slider'
        ),
        dcc.Dropdown(
            np.sort(flights['AIRLINE'].unique()),
            'Alaska Airlines Inc.',
            id='airline-dropdown'
        )
    ], style={'width': '50%'}),
    html.Div(id='flights-summary'),
    dcc.Loading(
        id='loading',  
        children=dcc.Graph(id='map'),
        parent_style={'width': '50%', 'display': 'inline-block'}
    ),
    dcc.Graph(id='hist', style={'width': '50%', 'display': 'inline-block'}),
    dcc.Store(id='flight-paths'),
    dcc.Store(id='flight-paths-filtered')
])

@app.callback(
    Output('flight-paths', 'data'),
    Input('month-slider', 'value'),
    Input('airline-dropdown', 'value')
)
def query(month, airline):
    flights_filtered = flights[
        (flights['MONTH'] == month) & 
        (flights['AIRLINE'] == airline)
    ].reset_index(drop=True)
    flight_paths = (flights_filtered
                        .groupby(['ORIGIN_NAME', 'DEST_NAME', 'ORIGIN_LON', 'ORIGIN_LAT', 'DEST_LON', 'DEST_LAT'])
                        .agg({'YEAR': 'size', 'DEP_DELAY': 'mean'})
                        .rename(columns={'YEAR': 'FLIGHTS', 'DEP_DELAY': 'AVG_DELAY'})
                        .reset_index()
                    )
    flight_paths_json = flight_paths.compute().to_json(orient='split')
    return flight_paths_json

@app.callback(
    Output('flight-paths-filtered', 'data'),
    Input('flight-paths', 'data'),
    Input('hist', 'selectedData')
)
def filter_flight_paths(flight_paths_json, hist_selection):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    avg_delay_range = None
    if hist_selection is not None and 'range' in hist_selection:
        print(hist_selection)
        avg_delay_range = hist_selection['range']['x']
        min_avg_delay, max_avg_delay = avg_delay_range[0], avg_delay_range[1]
        flight_paths = flight_paths[
            (flight_paths['AVG_DELAY'] >= min_avg_delay) & 
            (flight_paths['AVG_DELAY'] <= max_avg_delay)
        ].reset_index()
    flight_paths_json = flight_paths.to_json(orient='split')
    return flight_paths_json

@app.callback(
    Output('hist', 'figure'),
    Input('flight-paths', 'data')
)
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

@app.callback(
    Output('map', 'figure'),
    Input('flight-paths-filtered', 'data')
)
def update_map(flight_paths_json):
    flight_paths = pd.read_json(flight_paths_json, orient='split')

    fig_map = go.Figure()

    for i in range(len(flight_paths)):
        fig_map.add_trace(
            go.Scattergeo(
                locationmode = 'USA-states',
                lon = [flight_paths['ORIGIN_LON'][i], flight_paths['DEST_LON'][i]],
                lat = [flight_paths['ORIGIN_LAT'][i], flight_paths['DEST_LAT'][i]],
                mode = 'lines',
                hoverinfo = 'text',
                text = '{0} - {1}<br>{2} flights<br>avg. delay: {3} minutes'.format(flight_paths['ORIGIN_NAME'][i], flight_paths['DEST_NAME'][i], flight_paths['FLIGHTS'][i], round(flight_paths['AVG_DELAY'][i])),
                line = dict(width = 1, color = 'red'),
                opacity = 1
            )
        )

    fig_map.update_layout(
        height = 750,
        showlegend = False,
        geo = dict(
            scope = 'north america',
            projection_type = 'azimuthal equal area',
            showland = True,
            landcolor = 'rgb(243, 243, 243)',
            countrycolor = 'rgb(204, 204, 204)'
        )
    )

    return fig_map

@app.callback(
    Output('flights-summary', 'children'),
    Input('flight-paths-filtered', 'data'),
    Input('hist', 'selectedData')
)
def summarize_flights(flight_paths_json, hist_selection):
    flight_paths = pd.read_json(flight_paths_json, orient='split')
    num_flight_paths = len(flight_paths)
    num_flights = round(flight_paths['FLIGHTS'].sum())
    summary = '{0} flight paths representing {1} total flights'.format(num_flight_paths, num_flights)
    if hist_selection is not None and 'range' in hist_selection:
        avg_delay_min, avg_delay_max = round(hist_selection['range']['x'][0]), round(hist_selection['range']['x'][1])
        summary = '{0} flight paths with an average delay between {1} and {2} minutes, representing {3} total flights'.format(num_flight_paths, avg_delay_min, avg_delay_max, num_flights)
    return summary

if __name__ == '__main__':
    app.run_server(debug=True)