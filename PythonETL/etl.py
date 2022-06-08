from decouple import config
from datetime import datetime
import psycopg2
import pyodbc
import csv
import os

def connectToLocalSQLServer():
    return pyodbc.connect(
        'DRIVER={SQL Server};SERVER='+config('SQLSERVER_HOST_LOCAL')+';DATABASE='+config(
            'SQLSERVER_DATABASE_LOCAL')+';Trusted_Connection=yes;')

def connectToPosgreSQL():
    return psycopg2.connect(
        host=config('POSGRESQL_HOST'),
        user=config('POSGRESQL_USER'),
        password=config('POSGRESQL_PASS'),
        database=config('POSGRESQL_DATABASE')
    )


def connectToSQLServer():
    return pyodbc.connect(
        'DRIVER={SQL Server};SERVER='+config('SQLSERVER_HOST')+';DATABASE='+config(
            'SQLSERVER_DATABASE')+';UID='+config('SQLSERVER_USER')+';PWD='+config('SQLSERVER_PASS')
    )
