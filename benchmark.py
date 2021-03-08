from sqlalchemy import Text, MetaData, Table, insert
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.orm import sessionmaker
from itertools import count
import pandas as pd
import random
from sqlalchemy.sql import select
from tools import benchmark,benchmark_thread
import redis

import matplotlib.pyplot as plt


# --- SQL SETTINGS ---

POSTGRES_PASSWORD = 'no_pass'
PORT = '5432'

engine = create_engine(f"postgresql://postgres:{POSTGRES_PASSWORD}@localhost:{PORT}/postgres",pool_size=50, max_overflow=20)

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


#benchmark({'redis same line': hgetall, 'redis random line': hgetallrandom, 'psql same line': selectFixed,
           #'psql random line': selectRandom})


# ----- GET SHOW ID----#

def hmget4000():
    r.hmget('s4000', 'show_id')


def selectShowId4000():
    s = select([netflix_movies.c.show_id]).where(netflix_movies.c.show_id == 's4000')
    conn.execute(s)


#benchmark({'redis s4000': hmget4000, 'psql s4000': selectShowId4000})

indice_redis = count(start=8000)


def hmset():
    idx = next(indice_redis)
    r.hmset('s' + str(idx), next(df.iterrows())[1].to_dict())


indice_psql = count(start=8000)


def insertPsql():
    idx = next(indice_psql)
    i = insert(netflix_movies).values(show_id='s' + str(idx), type='type', title='title', director='director',
                                      cast='cast', country='country', date_added='date_added',
                                      release_year='release_year', rating='rating', duration='duration',
                                      listed_in='listed_in', description='description')
    conn.execute(i)


# ----- SET A LINE----#
#benchmark({'redis insert': hmset, 'psql insert': insertPsql})

# ----- KEY exists?---#

def exist7000():
    r.exists('s7000')


# benchmark({'s1': exist1, 's2': exist7000})



def sql_director_exists_head():
    req = 'SELECT CASE WHEN EXISTS( \
        SELECT  director FROM netflix_movies WHERE director = \'David Raynr\' ) \
    THEN 1 ELSE 0 END AS res;'
    conn.execute(req)


def sql_director_exists_middle():
    req = 'SELECT CASE WHEN EXISTS( \
        SELECT  director FROM netflix_movies WHERE director = \'Lance Daly\' ) \
    THEN 1 ELSE 0 END AS res;'
    conn.execute(req)


def sql_director_exists_end():
    req = 'SELECT CASE WHEN EXISTS( \
        SELECT  director FROM netflix_movies WHERE director = \'Emma Hatherley\' ) \
    THEN 1 ELSE 0 END AS res;'
    conn.execute(req)



def redis_director_exists_head():
    for show_id in r.scan_iter():
        if (r.hget(show_id,'director')==b'David Raynr'):
            return


def redis_director_exists_middle():
    for show_id in r.scan_iter():
        if (r.hget(show_id, 'director') == b'Lance Daly'):
            return True
    return False


def redis_director_exists_end():
    for show_id in r.scan_iter():
        if (r.hget(show_id, 'director') == b'Emma Hatherley'):
            return True
    return False


"""benchmark({'redis dir exists head': redis_director_exists_head, 'redis dir exists middle': redis_director_exists_middle,
           'redis dir exists end': redis_director_exists_end, 'psql dir exists head': sql_director_exists_head,
           'psql dir exists middle': sql_director_exists_middle, 'psql dir exists end': sql_director_exists_end}, run_nb=10)"""


def sql_indian_films():
    req = 'SELECT show_id FROM netflix_movies WHERE country = \'India\';'
    conn.execute(req)


def redis_indian_films():
    indian_films = []
    for show_id in r.scan_iter():
        if (r.hget(show_id, 'country') == b'India'):
            indian_films.append(show_id)

#benchmark({'redis indian films': redis_director_exists_head, 'psql indian films': redis_director_exists_middle}, run_nb=10)

def increment():
    r.hincrby("s7001", "release_year")

def update():
    req='UPDATE netflix_movies\
        SET release_year = CAST(release_year AS INTEGER) + 1\
        WHERE show_id = \'s7001\''
    conn.execute(req)


#benchmark({'redis incr n': increment,'psql incr': update},run_nb=10000)




############THREADING######################

# ------- GET A WHOLE LINE -------#

def hgetall(r):
    r.hgetall('s7392')


def selectFixed(conn):
    s = select([netflix_movies]).where(netflix_movies.c.show_id == 's7392')
    conn.execute(s)


#benchmark_thread({'redis same line': (hgetall,'r'),  'psql same line': (selectFixed,'psql')},engine,1000,50,True)

# ----- GET SHOW ID----#

def hmget4001(r):
    r.hmget('s4001', 'show_id')


def selectShowId4001(conn):
    s = select([netflix_movies.c.show_id]).where(netflix_movies.c.show_id == 's4001')
    conn.execute(s)

#benchmark_thread({'redis hmget': (hgetall,'r'),  'psql select show id': (selectFixed,'psql')},engine,1000,50,True)

# ----- Incremetnation----#

def increment(r):
    r.hincrby("s7001", "release_year")

def update(conn):
    req='UPDATE netflix_movies\
        SET release_year = CAST(release_year AS INTEGER) + 1\
        WHERE show_id = \'s7001\''
    conn.execute(req)

medians_redis=[]
medians_psql=[]
index=[]
for i in range(10):
    print("STEP")
    median_redis,median_psql=benchmark_thread({'redis incr': (increment,'r'),'psql incr': (update,'sql')},engine,1000,i*5+1,False)
    medians_redis.append(median_redis)
    medians_psql.append(median_psql)
    index.append(i*5+1)
print(index,medians_psql,medians_psql)
plt.plot(index,medians_redis,label='redis')
plt.plot(index,medians_psql,label='psql')
plt.legend()
plt.show()




#print(r.hget("s7001",'release_year'))