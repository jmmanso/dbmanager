# DBmanager
Data downstream/upstream manager for SQL databases



#### Installation
Clone or download this repo on your system. Go inside the root directory of the package, and type
```
pip install .
```

#### Set up connections parameters
Add the following environment variables to `.bashrc` or `.bash_profile`:
```
export DBMANAGER_USER=yourusername
export DBMANAGER_PSWD=yourpassword
export DBMANAGER_HOST=dbhostaddress
export DBMANAGER_PORT=yourport # try 3373
```
Don't forget to source the file.


#### Downloading data

The database connector contains `downstream`, which is a convenience method. As an
argument, it takes a string with SQL code or a string path to a SQL file. Either way,
the SQL code string can be composed of several statements, each one ending with a `;`.
The connector will only fetch data from the last statement. The purpose of this
functionality is to allow for temporary tables to be created in prior steps.

```python
from dbmanager import dbmanager

# Initialize connector
conn = dbmanager.DB_connector()

# This call returns a numpy array with the data and
# list of strings with the column names:
data, cols = conn.downstream("SELECT * FROM my_db.my_table;")

# Alternatively, you could also type the path to a sql script:
data, cols = conn.downstream("/home/files/query.sql")

# If you use pandas, it can be easily transformed
# into a dataframe:
df = pd.DataFrame(data, columns=cols)
```



#### Uploading data

Data can be written to the database via the `upstream` method.
