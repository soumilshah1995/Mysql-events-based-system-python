try:
    import mysql
    from mysql.connector import connect
    import json
    import os
    import sys
    import threading

    import json
    from ast import literal_eval
    import datetime
    from time import sleep
    import pandas as pd

    print("Loaded  .. . . . . . . .")
except Exception as e:
    print("Error : {} ".format(e))

# ==============================Settings =====================================

global mysqldataBase
global mysqltableName
global GLOBAL_ROW_COUNTER
global TIME
global PRIMARY_KEY_COLUMN_NAME

PRIMARY_KEY_COLUMN_NAME = 'id'
TIME                = 2
mysqldataBase       = 'mydb'
mysqltableName      = 'netflix'
GLOBAL_ROW_COUNTER  = 0

# =========================================================================


class Settings():

    def __init__(self,
                 mysqlhost='localhost',
                 mysqlport=3308,
                 mysqluser='root',
                 mysqlpassword='password',
                 mysqldataBase='mydb',
                 mysqltableName='netflix',
                 mysqlquery=''):

        self.mysqlhost=mysqlhost
        self.mysqlport = mysqlport
        self.mysqluser = mysqluser
        self.mysqlpassword = mysqlpassword
        self.mysqldataBase = mysqldataBase
        self.mysqltableName = mysqltableName
        self.mysqlquery =mysqlquery


class MySql(object):

    def __init__(self, settings=None):
        self.settings=settings

    def execute(self):
        try:

            self.db = connect(
                host     =      self.settings.mysqlhost,
                port     =      self.settings.mysqlport,
                password =      self.settings.mysqlpassword,
                user     =      self.settings.mysqluser,
                database =      self.settings.mysqldataBase,
            )

            self.cursor = self.db.cursor()
            self.cursor.execute("{}".format(self.settings.mysqlquery))
            myresult = self.cursor.fetchall()
            yield myresult
        except Exception as e:
            print("Error : {} ".format(e))
            return "Invalid Query : {} ".format(e)


def main():

    # Step 1:
    query = "SELECT COUNT(*) FROM {}.{}".format(mysqldataBase, mysqltableName)

    _settings = Settings(
        mysqlport=3308,
        mysqluser='root',
        mysqlpassword='password',
        mysqldataBase=mysqldataBase,
        mysqltableName=mysqltableName,
        mysqlquery=query
    )

    # Step 2: Query the database
    _exec = MySql(settings=_settings)
    response = _exec.execute()
    GLOBAL_ROW_COUNTER = int(list(next(response)[0])[0])

    # GET the COUNT from DATABASE

    while True:

        query = "SELECT COUNT(*) FROM {}.{}".format(mysqldataBase, mysqltableName)
        _settings = Settings(
            mysqlport=3308,
            mysqluser='root',
            mysqlpassword='password',
            mysqldataBase=mysqldataBase,
            mysqltableName=mysqltableName,
            mysqlquery=query)

        _exec = MySql(settings=_settings)
        response = _exec.execute()

        # Get the current Rows
        rows = int(list(next(response)[0])[0])

        print("ROW: {} and Global Counter: {} ".format())

        if (int(rows) > int(GLOBAL_ROW_COUNTER) ):


            query = "SELECT * from {}.{} WHERE {} > {} ".format(mysqldataBase,
                                                                mysqltableName,
                                                                PRIMARY_KEY_COLUMN_NAME,
                                                                str(GLOBAL_ROW_COUNTER))

            # ===========================EVENT DATA ================================
            _settings = Settings(
                mysqlport=3308,
                mysqluser='root',
                mysqlpassword='password',
                mysqldataBase=mysqldataBase,
                mysqltableName=mysqltableName,
                mysqlquery=query)

            _exec = MySql(settings=_settings)
            response_new_data = next(_exec.execute())

            # ==============================COLUMN NAMES===============================

            columnQuery = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA= '{}'   AND  TABLE_NAME = '{}'  ".format(mysqldataBase, mysqltableName)
            _settings.mysqlquery = columnQuery
            _exec = MySql(settings=_settings)
            response_sql = _exec.execute()
            columnRes = next(response_sql)
            columnNames = [name[0] for name in columnRes ]

            #============================   Transform the Data=======================

            # convert the event data into pandas dataframe as its easy for data manipulation
            df = pd.DataFrame(data = response_new_data, columns=columnNames)
            # converts the data into json object
            df1 = df.to_dict("records")

            print("-"*66)
            print("\n")
            print("EVENTS")
            print(df1)
            print("\n")
            print("-"*66)
            GLOBAL_ROW_COUNTER = rows
        else:
            print("Waiting for events")

        sleep(TIME)

