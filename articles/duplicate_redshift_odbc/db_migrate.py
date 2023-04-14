from sqlalchemy import MetaData
from sqlalchemy.types import VARCHAR
from pyodbc import connect, SQL_WVARCHAR
from utils import get_logger

LOGGER = get_logger(__name__)


class DuplicateSchema():
    def __init__(self, origin_engine, destination_engine):
        # SQLalchemy engines for origin and destination DB.
        self.origin_engine = origin_engine
        self.destination_engine = destination_engine
        # Meta data objects.
        self.origin_engine_meta = MetaData(bind=origin_engine)
        self.destination_engine_meta = MetaData(bind=destination_engine)
        # Schema tracker variable for _meta_refresch method.
        self.previous_schema = ''

    def _meta_refresh(self, schema_name):
        """Refresh origin db metadata, when necessary.

        The relfect() method takes a while to run. This function makes sure
        that it only runs when users switch to new Redshift schema.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        if self.previous_schema != schema_name:
            LOGGER.info("Set sqlalchemy meta schema to %s.", schema_name)
            self.origin_engine_meta.clear()
            self.origin_engine_meta.reflect(schema=schema_name)
            self.previous_schema = schema_name
        return None

    def setup_schema(self, schema_name):
        """Create schema in destination database.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Creating the %s schema.", schema_name)

        with self.destination_engine.connect() as con:

            query_template = 'CREATE SCHEMA IF NOT EXISTS {0};'

            con.execute(query_template.format(schema_name))

        return None

    def _remove_identity_clause(self, table_name):
        """Removes indenity and default column values.

        Trying to insert values into an identity/serial column causes errors.
        As a work around, it is possible to remove these restrictions from
        the metadata file, so that they do not get implemented in destination
        database.

        Args:
            table_name (str): Name of the table in destination database.

        Returns:
            None (NoneType)
        """
        table = self.origin_engine_meta.tables[table_name]

        for column in table.columns:
            column.server_default = None

        return None

    def create_1_table(self, schema_name, table_name):
        """Create one empty table in destination db.

        Args:
            schema_name (str): Name of the schema in destination database.
            table_name (str): Name of the table in destination database.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Creating the %s table.", table_name)
        self._meta_refresh(schema_name)

        self._remove_identity_clause(table_name)

        self.origin_engine_meta.tables[table_name].create(bind=self.destination_engine)

        return None

    def create_all_tables(self, schema_name):
        """Creates all tables in a given schema in desitnation db.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Creating all tables in %s schema.", schema_name)

        self._meta_refresh(schema_name)

        tables = self.origin_engine_meta.tables

        for table in tables:
            self._remove_identity_clause(table)

        self.origin_engine_meta.create_all(bind=self.destination_engine)

        return None

    def create_full_schema(self, schema_name):
        """Creates both schema and corresponding table in destination db.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        self._meta_refresh(schema_name)

        self.setup_schema(schema_name)

        self.create_all_tables(schema_name)

        return None

    def get_column_names_types(self, schema_name, table_name):
        """Retrieve a list of column names and data type for a single table.

        Psycopg2 cannot retrieve metadata from external tables. This function circumvents that
        limitation, by building and executing the query manually.

        Args:
            schema_name (str): Name of the schema in destination database.
            table_name (str): Name of the table in destination database.

        Returns:
            col_name_type (list of tuples): each tuple has a column name and data type.
        """
        # Get a list of column names and data types for a giiven table.
        query = """
            SELECT
                columnname,
                external_type
            FROM
                SVV_EXTERNAL_COLUMNS
            WHERE
                schemaname='{0}'
            AND
                tablename='{1}'
        """.format(schema_name, table_name)

        with self.origin_engine.connect() as con:
            result = con.execute(query)

        col_name_type = [' '.join(x) for x in result]

        return col_name_type

    def create_external_table(self, schema_name, table_name):
        """Creates a table in destination db.

        Psycopg2 cannot retrieve metadata from external tables. This function circumvents that
        limitation, by building and executing the query manually.

        Args:
            schema_name (str): Name of the schema in destination database.
            table_name (str): Name of the table in destination database.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Creating the %s table.", table_name)

        col_name_type = self.get_column_names_types(schema_name, table_name)

        columns = ', '.join(col_name_type)

        query = "CREATE TABLE IF NOT EXISTS {0}.{1} ({2})".format(schema_name, table_name, columns)

        con = self.destination_engine.connect()

        with self.destination_engine.connect() as con:
            con.execute(query)

        return None

    def create_external_schema(self, schema_name):
        """Creates a schema and all tables in destiantion db.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        self.setup_schema(schema_name)

        # Get a list of external table names in schema.
        query = """
            SELECT
                tablename
            FROM
                SVV_EXTERNAL_TABLES
            WHERE
                schemaname='{0}'
            """.format(schema_name)

        with self.origin_engine.connect() as con:
            result = con.execute(query)

        table_names = [x[0] for x in result]

        # Create each table in destination db.
        for table_name in table_names:
            self.create_external_table(schema_name, table_name)


class SampleData():
    """Populate newly duplicated tables in new db with sample data from origin db."""
    def __init__(self, origin_engine, destination_engine, odbc_engine):
        self.origin_engine = origin_engine
        self.destination_engine = destination_engine
        self.origin_engine_meta = MetaData(bind=origin_engine)
        self.destination_engine_meta = MetaData(bind=destination_engine)
        self.odbc_connection = connect(**odbc_engine)
        self.previous_schema = ''

    def _meta_refresh(self, schema_name):
        """Refresh destination db metadata, when necessary.

        The relfect() method takes a while to run. This function makes sure
        that it only runs when users switch to new Redshift schema.

        Args:
            schema_name (str): Name of the schema in destination database.

        Returns:
            None (NoneType)
        """
        if self.previous_schema != schema_name:
            LOGGER.info("Set sqlalchemy meta schema to %s.", schema_name)
            self.destination_engine_meta.clear()
            self.destination_engine_meta.reflect(schema=schema_name)
            self.previous_schema = schema_name

        return None

    def _get_data_sample(self, schema_name, table_name, sample_size):
        """Fetch a sample of rows from a single table in origin db.

        Args:
            schema_name (str): Name of the schema in destination database.
            table_name (str): Name of the table in destination database.
            sample_size (int): Number of rows to be sampled from said table.

        Returns:
            rows (list): a list of rows from the table.
        """
        LOGGER.info("Fetch data sample from %s table.", table_name)
        
        query = "SELECT * FROM {0} LIMIT {1}".format(
                    table_name,
                    sample_size
                )

        with self.origin_engine.connect() as con:
            result = con.execute(query)

        rows = list(result)

        return rows

    def _odbc_executemany_args(self, table_name, rows):
        """Generate inputs for the ODBC insert query.
        The query needs to have as many question marks as variables in the data sample.
        Each row of variables must be a tuple in a list.

        Args:
            table_name (str): Name of the table in destination database.
            rows (int): Number of rows to be sampled from said table.

        Returns:
            insert_query (str): An insert sql query, with '?' instead of cell values.
            list_of_tuples (list): a list tuples, each tuple containing one row from the table.
        """
        LOGGER.info("Generate odbc arguments for the %s table.", table_name)

        query_template = "INSERT INTO {table_name} VALUES ({question_marks})"

        # Create as many quetion marks as variables in table.
        question_mark_list = ['?'] * len(rows[0])

        # Generate the query with table name and question marks.
        insert_query = query_template.format(
            table_name=table_name,
            question_marks=", ".join(question_mark_list)
        )

        # Convert a lits of rows to a list of tuples for use in ODBC cursor.
        list_of_tuples = [tuple(x) for x in rows]

        return insert_query, list_of_tuples

    def _odbc_data_types(self, schema_name, table_name):
        """Overwrite pyodbc character limit on VARCHAR.

        There is a well known issue with pyodbc module, which happens
        when trying to insert long strinds into large TEXT or VARCHAR
        columns. Even if the destination column is big enough, the module
        will throw out an overflow error. Manually overriding the data types
        used in the cursor is one workaround. This function generates a list of
        tuples, which modify the data types of large VARCHAR columns.

        Args:
            table_name (str): Name of the table in destination database.

        Returns:
            d_types (list): A sparse list of SQL VARCHAR data types, used to overwrite the
                odbc defaults.
        """
        LOGGER.info("Generate data types for the %s table.", table_name)

        columns = self.destination_engine_meta.tables[table_name].columns

        # Generate a list of None values. If there are no large VARCHAR columns
        # the cursor will use its defaults.
        d_types = [None] * len(columns)

        # If a given column is of type VARCHAR and has more than 100 char length,
        # None value in the list is replaced with a data type tuple.
        for i, column in enumerate(columns):
            if type(column.type) == VARCHAR:
                if column.type.length >= 100:
                    d_types[i] = (SQL_WVARCHAR, 100000, 0)

        return d_types

    def populate_1_table(self, schema_name, table_name, sample_size):
        """Fetch a data sample and insert it into a single destiontion table.
        Args:
            schema_name (str): Name of the schema in destination database.
            table_name (str): Name of the table in destination database.
            sample_size (int): Number of rows to be sampled from said table.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Being populating the %s table.", table_name)

        self._meta_refresh(schema_name)

        rows = self._get_data_sample(schema_name, table_name, sample_size)

        # Do nothing if the origin table has no rows.
        if len(rows) == 0:
            pass

        else:
            insert_query, list_of_tuples = self._odbc_executemany_args(table_name, rows)

            data_types = self._odbc_data_types(schema_name, table_name)

            cursor = self.odbc_connection.cursor()
            # Here, the list of typles is used to overwrite certain column data types.
            cursor.setinputsizes(data_types)
            cursor.fast_executemany = True

            cursor.executemany(insert_query, list_of_tuples)

            cursor.commit()
            cursor.close()

            return None

    def populate_all_tables(self, schema_name, sample_size):
        """Populate all tables in a given destination schema with sample data.
        Args:
            schema_name (str): Name of the schema in destination database.
            sample_size (int): Number of rows to be sampled from said table.

        Returns:
            None (NoneType)
        """
        LOGGER.info("Begin populating tables in the %s schema.", schema_name)

        self._meta_refresh(schema_name)

        table_names = list(self.destination_engine_meta.tables.keys())

        for table in table_names:
            self.populate_1_table(schema_name, table, sample_size)

        return None
