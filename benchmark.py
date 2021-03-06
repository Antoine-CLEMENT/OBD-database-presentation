from sqlalchemy import Text, MetaData, Table
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.orm import sessionmaker
import pandas as pd

POSTGRES_PASSWORD = 'no_pass'
PORT = '5436'

# docker container run -d --name psql --rm -e POSTGRES_PASSWORD=no_pass -p 5436:5432 postgres:11-alpine

engine = create_engine(f"postgresql://postgres:{POSTGRES_PASSWORD}@localhost:{PORT}/postgres")

base = declarative_base()
metadata = MetaData(engine)
metadata.reflect()
table = metadata.tables.get('netflix_movies')
if table is not None:
    print('Deleting netflix_movies table')
    base.metadata.drop_all(engine, [table], checkfirst=True)

# Define the table
netflix_movies = Table(
    'netflix_movies', metadata,
    Column('id', Integer, primary_key=True),
    Column('type', Text, nullable=False),
    Column('title', Text, nullable=False),
    extend_existing=True,
)

# Create db
netflix_movies.create()

netflix_data_to_inject = pd.read_csv('netflix_titles.csv', sep=';').to_dict(orient='records')

metadata.reflect()
table = sqlalchemy.Table('netflix_movies', metadata, autoload=True)

# Open the session
Session = sessionmaker(bind=engine)
session = Session()

# Inser the dataframe into the database in one bulk
conn = engine.connect()
conn.execute(table.insert(), netflix_data_to_inject)

# Commit the changes
session.commit()

# Close the session
session.close()
