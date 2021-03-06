# OBD database presentation 
## redis vs psql


### Prequisites

To run this code, you should have python (at least 3.7), docker and docker-compose installed

In a terminal, run the following commands:

```
 pip install -r requirements.txt
```

And deploy redis and psql in two distinct containers:

```
docker-compose up --build
```

You can then access the databases inside the container by running 

blablabla

blablabla

To run our benchmark tests to compare redis and psql, you just need to execute `benchmark.py`.

This will populate the databases with the recordings from netflix databases, and execute stress on them