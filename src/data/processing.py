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