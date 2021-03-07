from sqlalchemy import Text, MetaData, Table
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.orm import sessionmaker
import pandas as pd
import random
from sqlalchemy.sql import select
from tools import benchmark
import redis

# --- SQL SETTINGS ---

POSTGRES_PASSWORD = 'no_pass'
PORT = '5432'

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
    Column('show_id', Text, primary_key=True),
    Column('type', Text, nullable=False),
    Column('title', Text, nullable=False),
    Column('director', Text, nullable=False),
    Column('cast', Text, nullable=False),
    Column('country', Text, nullable=False),
    Column('date_added', Text, nullable=False),
    Column('release_year', Text, nullable=False),
    Column('rating', Text, nullable=False),
    Column('duration', Text, nullable=False),
    Column('listed_in', Text, nullable=False),
    Column('description', Text, nullable=False),
    extend_existing=True,
)

# Create db
netflix_movies.create()

netflix_data_to_inject = pd.read_csv('netflix_titles.csv').to_dict(orient='records')

metadata.reflect()
table = sqlalchemy.Table('netflix_movies', metadata, autoload=True)

# Open the session
Session = sessionmaker(bind=engine)
session = Session()

# Insert the dataframe into the database in one bulk
conn = engine.connect()
conn.execute(table.insert(), netflix_data_to_inject)

# Commit the changes
session.commit()

# Close the session
session.close()

# --- REDIS SETTINGS ---
r = redis.Redis()
# empties all previous keys
r.flushall()
# check there is no key anymore
r.keys()

# -------Import dataset in redis-------
df = pd.read_csv("netflix_titles.csv")
df.head()
with r.pipeline() as pipe:
    for index, row in df.iterrows():
        pipe.hmset(row.show_id, row.to_dict())
    pipe.execute()


# ------- GET A WHOLE LINE -------#

def hgetall():
    r.hgetall('s7392')


def hgetallrandom():
    i = random.randint(1, 7787)
    r.hgetall('s' + str(i))


def selectFixed():
    s = select([netflix_movies]).where(netflix_movies.c.show_id == 's7392')
    conn.execute(s)


def selectRandom():
    i = random.randint(1, 7787)
    s = select([netflix_movies]).where(netflix_movies.c.show_id == 's' + str(i))
    conn.execute(s)


benchmark({'redis same line': hgetall, 'redis random line': hgetallrandom, 'psql same line': selectFixed,
           'psql random line': selectRandom})


# ----- GET SHOW ID----#
def hmget1():
    r.hmget('s1', 'show_id')


def hmget4000():
    r.hmget('s4000', 'show_id')


def hmget7000():
    r.hmget('s7000', 'show_id')


# benchmark({'get s1':hmget1,'get s4000':hmget4000,'get s7000':hmget7000})


def hmset():
    r.hmset('test', next(df.iterrows())[1].to_dict())


# ----- SET A LINE----#
# benchmark({'hmset': hmset})

# ----- KEY exists?---#
def exist1():
    r.exists('s1')


def exist7000():
    r.exists('s7000')

# benchmark({'s1': exist1, 's2': exist7000})
