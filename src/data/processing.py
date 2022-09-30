import os
from zipfile import ZipFile
import dask.dataframe as dd

def unzip_flight_files(input_dir, output_dir):
    if not os.path.exists('{0}/flights'.format(output_dir)):
        os.makedirs('{0}/flights'.format(output_dir))
    months=['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for m in months:
        with ZipFile('{0}/flights/{1}.zip'.format(input_dir, m), 'r') as zf:
            with open('{0}/flights/{1}.csv'.format(output_dir, m), 'wb') as f:
                f.write(zf.read('T_ONTIME_REPORTING.csv'))

def flights_csv_to_parquet(input_dir, output_dir):
    column_dtypes={
        'CANCELLATION_CODE': 'object',
        'ARR_TIME': 'float64',
        'ARR_DELAY_GROUP': 'float64',
        'DEP_DELAY_GROUP': 'float64',
        'DEP_TIME': 'float64',
        'WHEELS_OFF': 'float64',
        'WHEELS_ON': 'float64'
    }
    flights = dd.read_csv('{0}/flights/*.csv'.format(input_dir), dtype=column_dtypes)
    df = join_to_airports_and_airlines(flights)
    df = df.dropna(subset=['DEP_DELAY'])
    dd.to_parquet(df, '{0}/flights.parquet'.format(output_dir), engine='pyarrow')

def join_to_airports_and_airlines(flights):
    airlines = dd.read_csv('data/raw/airlines.csv')
    airports = dd.read_csv('data/raw/airports.csv')
    df = flights.merge(
        airlines,
        left_on='OP_UNIQUE_CARRIER',
        right_on='CODE'
    )
    df = df.rename(columns={'CODE': 'AIRLINE_CODE', 'NAME': 'AIRLINE'})
    df = df.merge(
        airports,
        left_on='ORIGIN',
        right_on='id'
    )
    df = df.rename(columns={
        'id': 'ORIGIN_ID', 
        'name': 'ORIGIN_NAME', 
        'lon': 'ORIGIN_LON', 
        'lat': 'ORIGIN_LAT'
    })
    df = df.merge(
        airports,
        left_on='DEST',
        right_on='id'
    )
    df = df.rename(columns={
        'id': 'DEST_ID',
        'name': 'DEST_NAME',
        'lon': 'DEST_LON',
        'lat': 'DEST_LAT'
    })
    return df

def get_flight_paths(flights):
    """Aggregates flights by path (origin and destination)."""
    flight_paths = (flights
                        .groupby(['ORIGIN_NAME', 'DEST_NAME', 'ORIGIN_LON', 'ORIGIN_LAT', 'DEST_LON', 'DEST_LAT'])
                        .agg({'YEAR': 'size', 'DEP_DELAY': 'mean'})
                        .rename(columns={'YEAR': 'FLIGHTS', 'DEP_DELAY': 'AVG_DELAY'})
                        .reset_index()
                    )
    return flight_paths