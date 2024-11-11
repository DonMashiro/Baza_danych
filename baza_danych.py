import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy import text

stations_df = pd.read_csv('clean_stations.csv')
measurements_df = pd.read_csv('clean_measure.csv')

engine = create_engine('sqlite:///database.db', echo=True)
meta = MetaData()

stations = Table(
    'stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),  
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)

measurements = Table(
    'measurements', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),  
    Column('date', String),  
    Column('precip', Float),
    Column('tobs', Float),
)

meta.create_all(engine)
conn = engine.connect()

stations_df.to_sql('stations', conn, if_exists='replace', index=False)
measurements_df.to_sql('measurements', conn, if_exists='replace', index=False)

result = conn.execute(text("SELECT * FROM stations LIMIT 5")).fetchall()
print("Stations data (first 5 rows):")
for row in result:
    print(row)

result = conn.execute(text("SELECT * FROM measurements LIMIT 5")).fetchall()
print("\nMeasurements data (first 5 rows):")
for row in result:
    print(row)

conn.close()