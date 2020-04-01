"""
    Created By : Nazmi Asri
    Email: nazmiasri95@gmail.com
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy.engine.url import make_url, URL
import traceback
import pandas as pd

class SqlRunner:

    def __init__(self, uri, password):
        self.uri = uri
        self.password = password

    @staticmethod
    def get_connection_url(db_uri, password):
        url = make_url(db_uri)
        query = {"charset": "utf8mb4"}
        connection_url = URL(url.drivername, url.username, password, url.host, url.port, url.database, query)
        return connection_url

    def get_sql_engine(self):
        connection_url = SqlRunner.get_connection_url(self.uri, self.password)
        return create_engine(connection_url, echo=False, execution_options=dict(stream_results=True),
                             server_side_cursors=True, encoding="utf-8")

    def run_query(self, sql_query, result_size):
        print("Running Query => ", sql_query)

        engine = self.get_sql_engine()
        query_results = pd.read_sql_query(sql_query, engine, chunksize=result_size)
        return query_results

    @staticmethod
    def pgsql_upsert(table, conn, keys, data_iter):

        @compiles(Insert, 'postgresql')
        def prefix_inserts(insert, compiler, **kw):
            str_val = compiler.visit_insert(insert, **kw)
            fields = str_val[str_val.find("(") + 1:str_val.find(")")].replace(" ", "").split(",")
            generated_directive = ["{0}=%({0})s".format(field) for field in fields if field != "id"]
            str_val += " ON CONFLICT (id) DO UPDATE SET " + ",".join(generated_directive)
            return str_val

        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    @staticmethod
    def mysql_replace_into(table, conn, keys, data_iter):

        @compiles(Insert)
        def replace_string(insert, compiler, **kw):
            str_val = compiler.visit_insert(insert, **kw)
            str_val = str_val.replace("INSERT INTO", "REPLACE INTO")
            return str_val

        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    def to_sql(self, data: pd.DataFrame, table_name: str, chunksize: int = 1, method=None, if_exists="append",
               schema=None):
        url = make_url(self.uri)
        db_name = url.drivername.split('+')
        if ("postgresql" in db_name) or ("pgsql" in db_name):
            method = self.pgsql_upsert if method == "replace" else method
        else:
            method = self.mysql_replace_into if method == "replace" else method

        try:
            data.to_sql(name=table_name,
                        con=self.get_sql_engine(),
                        if_exists=if_exists,
                        schema=schema,
                        chunksize=chunksize,
                        method=method,
                        index=False)
        except Exception as ex:
            print(traceback.format_exc())