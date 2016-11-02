"""
DBMANAGER
--------------

Contains the class connector, which provides a SQL API-like instance
to a MySQL database.
"""

import os
import MySQLdb as mdb
import numpy as np
import time


class DB_connector:
    """ Interface to SQL databases in production/stage. Gets and writes data """

    def __init__(self):
        try:
            self.User = os.environ['DBMANAGER_USER']
            self.Pswd = os.environ['DBMANAGER_PSWD']
            self.Port = os.environ['DBMANAGER_PORT']
            self.Host = os.environ['DBMANAGER_HOST']
        except KeyError:
            raise Exception("Unable to load DB connection parameters. Make sure you have set them up as environment variables.")

    def open_connection(self):
        """ Make database connection """
        try:
            self.con = mdb.connect(
                self.Host, self.User, self.Pswd, '', self.Port)
            self.cur = self.con.cursor()
            self.connected = True
        except:
            raise Exception("Error: Not able to connect to database.")

    def check_connection(self):
        """ Return connection status """
        try:
            self.con.ping()
            self.connected = True
        except:
            self.connected = False
        #
        return self.connected

    def close_connection(self):
        """ Close database connection """
        try:
            self.con.close()
        except:
            pass
        self.connected = False

    def sql_reader(self, sql_filepath):
        """ Reads and returns a SQL string from file """
        try:
            q = open(sql_filepath, 'r')

        except:
            raise Exception("Error: SQL file not found.")

        sql_query = q.read()

        # Remove \t
        sql_query = sql_query.replace('\t', ' ')
        q.close()
        return sql_query

    def sql_statement_splitter(self, query_string):
        """ Takes a sql string and splits it into
        the different statements, which ought to be
        separated by ;
        """
        # Do some formatting of white space and colons, making sure the last
        # character is a ;
        query_string = self.colon_formatter(query_string)
        # Make the split
        query_splits = query_string.split(';')
        # Since the last character was a ;, the last statement is an empty string.
        # Remove it:
        query_splits = query_splits[:-1]
        # The split removed the ; in each statement, so add it again to each
        # one:
        statements = [statement + ';' for statement in query_splits]
        return statements

    def colon_formatter(self, query_string):
        """ Takes a sql statement, removes trailing white space,
         and adds the semicolon at the end, in case it is missing. """

        # Remove trailing whitespaces
        query_string = query_string.strip()

        last_character = query_string[-1]
        #
        if last_character != ';':
            query_string = query_string + ';'

        return query_string

    def pull_single_statement(
            self, query_string, open_conn=True, close_conn=True):
        """ Takes a SQL query string containing ONE
        statement, and it should be of the SELECT kind. """
        #
        # Just in case, make sure the statement closes with a colon:
        sql_query = self.colon_formatter(query_string)

        if open_conn:
            self.open_connection()
        #
        self.cur.execute(sql_query)
        sql_data = np.array(self.cur.fetchall())
        col_names = [i[0] for i in self.cur.description]
        if close_conn:
            self.close_connection()
        #
        # Return tuple with data array (in the form of tuples) and column names
        # (list of strings)
        return sql_data, col_names

    def pull_multi_statement(
            self, query_string, naptime=3, open_conn=True, close_conn=True):
        """ Takes a SQL string containing multiple write statements,
        such as creating and populating temp tables, and one last SELECT statement
        that returns data. """
        # Remove trailing white spaces
        query_string = query_string.strip()
        # format
        query_string = self.colon_formatter(query_string)
        # split into chunks
        statements = self.sql_statement_splitter(query_string)

        N = len(statements)

        # Manually open connection
        if open_conn:
            self.open_connection()
        # Execute all statements that handle temporary steps
        for i in range(N - 1):
            try:
                self.upstream(statements[i], open_conn=False, close_conn=False)
                print 'Succesfully executed statement %s' % i
            except:
                raise Exception('Database error at statement %s ' % i)
            #
            time.sleep(naptime)

        # Now execute the last statment, which returns the data you want. Note that this
        # has to use the connection that was open during the previous
        # statements, hence open_conn=False
        try:
            data, cols = self.pull_single_statement(
                statements[N - 1], open_conn=False, close_conn=False)
        except:
            raise Exception('Error trying to fetch data')
        #
        if close_conn:
            self.close_connection()

        return data, cols

    def push_multi_statement(self, sql, naptime=3,
                             open_conn=True, close_conn=True):
        """ Takes a SQL string containing multiple write statements,
        such as creating and populating temp tables, returns no data. """
        if ' ' in sql:
            query_string = sql

        # If you are loading the sql query from file:
        else:
            query_string = self.sql_reader(sql)

        # Remove trailing white spaces
        query_string = query_string.strip()

        query_string = self.colon_formatter(query_string)
        statements = self.sql_statement_splitter(query_string)

        N = len(statements)

        # Manually open connection
        if open_conn:
            self.open_connection()
        # Execute all statements that handle temporary steps
        for i in range(N):
            try:
                self.upstream(statements[i], open_conn=False, close_conn=False)
                print 'Succesfully executed statement %s' % i
            except:
                raise Exception('Database error at statement %s ' % i)
            #
            time.sleep(naptime)

        # Manually close connection
        if close_conn:
            self.close_connection()
        #
        return None

    def downstream(self, sql, naptime=3, open_conn=True, close_conn=True):
        """ Fetches data from a freeform SQL query, which can be a string or a path to
        a text file.

        If the query contains multiple statements, they are split
        and executed one at a time, this is a constraint imposed by the
        MySql connector. Thus, if there is more than one ";" in the SQL string,
        this function will execute it as multi-statment.
         """
        # First, we will figure out if the argument 'sql' is a file path to be loaded or a SQL code string.
        # If it is a code string, it will have whitespaces ' '. This is the discriminative characteristic we
        # are gonna use.
        if ' ' in sql:
            sql_query = sql

        # If you are loading the sql query from file:
        else:
            sql_query = self.sql_reader(sql)

        # Add last semicolon if it is missing
        sql_query = self.colon_formatter(sql_query)

        # Next, we need to determine if the query string has multiple statements.
        # Such cases would be creation of temporary tables, and a last statement
        # that actually returns data.
        if sql_query.count(';') > 1:
            run_multistatement = True
        else:
            run_multistatement = False

        if run_multistatement:
            sql_data, col_names = self.pull_multi_statement(
                sql_query, naptime=naptime, open_conn=open_conn, close_conn=close_conn)
        else:
            sql_data, col_names = self.pull_single_statement(
                sql_query, open_conn=open_conn, close_conn=close_conn)

        return sql_data, col_names

    def upstream(self, sql, open_conn=True, close_conn=True):
        """ Executes a freeform sql one-statment query that can write"""
        # Same as in function downstream:
        if ' ' in sql:
            sql_query = sql

        # If you are loading the sql query from file:
        else:
            sql_query = self.sql_reader(sql)
        #
        sql_query = self.colon_formatter(sql_query)
        if open_conn:
            self.open_connection()
        #
        self.cur.execute(sql_query)
        self.con.commit()
        if close_conn:
            self.close_connection()

    def upstream_dataframe(self, table_name, frame,
                           open_conn=True, close_conn=True):
        """ Write a dataframe to an existing table in the database.
        This inserts the data in batch mode.  """
        # Create a list of tuples, containing the values for each row:
        params = []
        for i in range(frame.values.shape[0]):
            # Get the object vector
            vec = frame.iloc[i].values.tolist()
            # If there are any nan's, transform to NULL
            for j in range(len(vec)):
                # nans and infs are float type
                if type(vec[j]).__name__ in ('int', 'float'):
                    if vec[j] != vec[j] or vec[j] == np.inf or vec[
                            j] == -np.inf or vec[j] < -1:
                        # Python type None is converted to SQL NULL by the
                        # connector
                        vec[j] = None
            #
            # Gather all rows together in a master list of tuples
            params.append(tuple(vec))
        # Build the chunk of the SQL query defining the column names and value
        # pointers:
        col_string = ''
        val_string = ''
        for col_name in list(frame.columns):
            col_string += col_name + ','
            val_string += '%s,'
        # Remove the last comma:
        col_string = col_string[:-1]
        val_string = val_string[:-1]
        # Assemble SQL string
        sql_q = 'INSERT IGNORE INTO ' + table_name + \
            ' (' + col_string + ') VALUES ( ' + val_string + ')'
        # Upload
        if open_conn:
            self.open_connection()
        # Make some prep per Brijesh's suggestion
        self.upstream(
            "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            open_conn=False,
            close_conn=False)
        #
        # Write data
        self.cur.executemany(sql_q, params)
        self.con.commit()
        if close_conn:
            self.close_connection()







#
