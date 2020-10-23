# from pandas.io import sql
import pymysql
import pandas as pd
import ibis

import getpass

from typing import List, Dict, Tuple, Optional, Union


class DBSub(object):        
    """class to be called for a pymysql connection, creating an instance connects immediately 
    and allows operations on the db, returns a db cursor, connection is closed on exit automatically
    
    https://alysivji.github.io/managing-resources-with-context-managers-pythonic.html#:~:text=__enter__%20should%20return,returned%20by%20__enter__%20.
    """
    def __init__(self, 
                 user: str,
                 password: str,
                 host: str,
                 database: str,
                 port: int,
                 verbose: bool = False
    ):
        self.con = pymysql.connect(host=host, user=user, passwd=password, db=database, port=port)
        self.cur = self.con.cursor()
        self.verbose = verbose
        if self.verbose: print("DBSub: init")

    def __enter__(self):
        if self.verbose: print("DBSub: enter")
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.commit()
        if self.verbose: print("DBSub: exit")
        if self.con:
            if self.verbose: print("DBSub: close connection")
            self.con.close()



class DBInterface():
    """
    TODO
    ----
    
    SOLVED
    ------
    * implement generic_write function
    * dataframe2db: update: where condition for strings-id. problem if primary key is a string. exception necessary
    """
    def __init__(self, 
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 host: str = "localhost",
                 database: str = "stocks_db",
                 port: int = 3306,
                 verbose: bool = False
                ) -> None:
        self.host = host
        self.user = user if user else getpass.getuser()
        self.password = password if password else getpass.getpass()
        self.database = database
        self.port = port
        self.verbose = verbose
    


    @property
    def db_settings(self):
        return {'host': self.host, 'user': self.user, 'password': self.password, 'database': self.database, 'port': self.port}



    @property
    def ibiscon(self):
        """
        Example
        -------
            ```
            con = DBInterface().ibiscon
            ```
        """
        return ibis.mysql.connect(**self.db_settings)



#%% query



    def dataframe(self, table: str = None, sql_cmd: str = None) -> pd.DataFrame:
        """ """
        if table:
            sql_cmd = f"SELECT * FROM {table}"
        if sql_cmd:
            dbx = DBSub(**self.db_settings, verbose=self.verbose)
            try:
                df = pd.read_sql(sql_cmd, con=dbx.con)
            finally:
                if self.verbose: print("DBInterface: close connection")
                dbx.con.close()
            return df
        else:
            raise ValueError("please insert sql command")



    def query(self, sql_cmd: str = None) -> Tuple[Tuple, ...]:
        """ """
        try:
            with DBSub(**self.db_settings, verbose=self.verbose) as cur:
                cur.execute(sql_cmd)
                res = cur.fetchall()
            return res
        except Exception as error:
            print("Warning! The execution of the provided SQL command failed!", error)



    def is_row(self, table: str, id: Union[str, int], id_header: Optional[str] = None) -> bool:
        """ Checks if data is available for specific id in a table.
        Returns a Boolean.

        TODO: better print using git dataframe(sql_cmd="SHOW INDEX from stocks_fundamental"))
        """
        id_header = 'id' if id_header is None else id_header
        if type(id) == str:
            sql_cmd = f"SELECT * FROM {table} WHERE {id_header} = '{id}';"
        else:
            sql_cmd = f"SELECT * FROM {table} WHERE {id_header} = {id};"
        res = self.query(sql_cmd)
        res_bool = True if res else False
        if self.verbose: print("is_row:", res_bool) #, res, sql_cmd)
        return res_bool



    def _information_schema(self, db_table: str) -> Dict:
        """Connect to db and get schema of db_table
        
        Parameter
        ---------
        db_table: str

        Returns
        -------
        res: dict(fieldname:datatype)
        """          
        sql_cmd = f"""
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{db_table}' 
        """
        res = self.query(sql_cmd)
        return res
        


    def schema(self, db_table: str) -> Dict:
        """get schema of db table"""
        res = self._information_schema(db_table)
        return ({r[3]:r[7] for r in res})
    


    def column_names(self, db_table: str) -> List[str]:
        """get column names of db_table"""
        res = self._information_schema(db_table)
        return([r[3] for r in res])


    
    def index(self, db_table: str, db_index_name: str = 'id') -> List:
        """Connect to db and get index of db_table"""
        sql_cmd = f"SELECT {db_index_name} FROM {db_table}"
        res = self.query(sql_cmd)
        return([r[0] for r in res])



#%% execute and write


    def execute(self, sql_cmd):
        """
        """
        with DBSub(**self.db_settings, verbose=self.verbose) as cur:
            cur.execute(sql_cmd)
            res = cur.fetchall()
        return res


    def dataframe2db(self, df: pd.DataFrame, db_table: str, if_exist: str = 'fail'):
        """Export a dataframe into an existing table in the database

        Parameters
        ----------
        df : pd.DataFrame
        db_table : str
            name of target table in MariaDB
        if_exists : {'fail', 'replace', 'update'}, default 'fail'
            How to behave if a row in a table already exists.
            * fail: Raise a ValueError.
            * replace: if the row with primery keys exists the row gets fully replaced
            (Missing values get replaced by default NULL)
            * update: update existing rows identified using primary key. Primary keys will not get updated.

        Returns
        -------

        marginal conditions
        -------------------
        * ID have to be the first column of the DataFrame (column[0])
        * Target table and headers have to exist in MariaDB
        * names of DataFrame columns and MariaDB columns have to agree with each other
        * DataFrame.index is not transferred
        """
        def transfer_nan_values_to_sql_null(s: pd.Series) -> str:
            """
            Parameters
            ----------
            s : pd.Series
                Row of DataFrame, NaN's have to be filled with 'NULL'
            
            Returns
            -------
            string : str
            """
            string = "".join(str(', "'+ str(value) +'"') if value != 'NULL' else ', ' + str(value) for value in s.values)
            return string[2:]

        df.reset_index(drop=True, inplace=True)
        try:
            df.fillna('NULL', inplace=True)
        except Exception as error:
            print("fillna-method ist not valid for category-dtypes", error)

        with DBSub(**self.db_settings, verbose=self.verbose) as cur:
            list_primary_keys = self.dataframe(sql_cmd=f"SHOW KEYS FROM {db_table} WHERE Key_name = 'PRIMARY'").Column_name.values
            for num, idx in enumerate(df.index):
                try:
                    if self.is_row(table=db_table, id=df.loc[idx, df.columns[0]], id_header=df.columns[0]) == True:
                        if if_exist == 'fail':
                            print(f"Duplicate entry {df.columns[0]}='{df.loc[idx, df.columns[0]]}' for key 'PRIMARY'. No DB update")
                            continue
                        elif if_exist == 'replace':
                            sql_cmd = "REPLACE INTO {} ({})".format(db_table, df.columns.str.cat(sep=', '))
                            sql_cmd += " VALUES ({})".format(transfer_nan_values_to_sql_null(s=df.iloc[num, :]))
                            print('{}:{}: replace entry'.format(db_table, df.loc[idx, df.columns[0]]))
                        elif if_exist == "update":
                            sql_cmd = f"UPDATE {db_table} SET"
                            for col in df.drop(list_primary_keys, axis=1).columns:
                                sql_cmd += f" {col} = '{df.loc[idx, col]}'," if df.loc[idx, col] != "NULL" else f" {col} = {df.loc[idx, col]},"
                            sql_cmd = sql_cmd[:-1] + " WHERE "
                            sql_cmd += " AND ".join(f"{str(pk)} = '{str(df[pk].iloc[num])}'" for pk in list_primary_keys)
                            sql_cmd += ";"
                            print('{}:{}: update entry'.format(db_table, df.loc[idx, df.columns[0]]))
                        else:
                            raise NotImplementedError("wrong input for if_exist")
                    else:
                        sql_cmd = f"INSERT INTO {db_table} ({df.columns.str.cat(sep=', ')}) VALUES ({transfer_nan_values_to_sql_null(s=df.iloc[num, :])});"
                        print('{}:{}: insert entry'.format(db_table, df.loc[idx, df.columns[0]]))
                except Exception as error:
                    raise KeyError(error)

                if self.verbose: print(f'dataframe2db: sql_cmd to execute:\n{sql_cmd}')
                cur.execute(sql_cmd)
            res = cur.fetchall()
        return res
        