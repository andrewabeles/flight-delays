import os
from zipfile import ZipFile
import dask.dataframe as dd

def unzip_flight_files(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    months=['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for m in months:
        with ZipFile('{0}/{1}.zip'.format(input_dir, m), 'r') as zf:
            with open('{0}/{1}.csv'.format(output_dir, m), 'wb') as f:
                f.write(zf.read('T_ONTIME_REPORTING.csv'))

def flights_zip_to_parquet(input_dir, output_dir):
    column_dtypes={
        'CANCELLATION_CODE': 'object',
        'ARR_TIME': 'float64',
        'ARR_DELAY_GROUP': 'float64',
        'DEP_DELAY_GROUP': 'float64',
        'DEP_TIME': 'float64',
        'WHEELS_OFF': 'float64',
        'WHEELS_ON': 'float64'
    }
    df = dd.read_csv(input_dir, dype=column_dtypes)
    dd.to_parquet(df, output_dir + '/flights.parquet', engine='pyarrow')

def get_flight_paths(flights):
    """Aggregates flights by path (origin and destination)."""
    flight_paths = (flights
                        .groupby(['AIRPORT_ORIGIN', 'AIRPORT_DEST', 'LON_ORIGIN', 'LAT_ORIGIN', 'LON_DEST', 'LAT_DEST'])
                        .agg({'FLIGHTS': 'sum', 'TOTAL_DELAY': 'sum'})
                        .rename(columns={'FLIGHTS': 'FLIGHTS', 'TOTAL_DELAY': 'TOTAL_DELAY'})
                        .reset_index()
                    )
    flight_paths['AVG_DELAY'] = flight_paths['TOTAL_DELAY'] / flight_paths['FLIGHTS']
    return flight_paths