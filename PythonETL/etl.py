from decouple import config
from datetime import datetime
import psycopg2
import pyodbc

def connectToLocalPosgreSQL():
    return psycopg2.connect(
        host=config('POSGRESQL_HOST_LOCAL'),
        user=config('POSGRESQL_USER_LOCAL'),
        password=config('POSGRESQL_PASS_LOCAL'),
        database=config('POSGRESQL_DATABASE_LOCAL')
    )

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


def getProducts(cursorPosgreSQL):
    query = "SELECT Product_Id, Product_Name, Product_Price FROM STOCK.tb_Product"
    cursorPosgreSQL.execute(query)
    products = cursorPosgreSQL.fetchall()

    return products


def getPaymentMethod(cursorPosgreSQL):
    query = "SELECT Payment_Method_Id, Payment_Method_Name FROM SALES.tb_Payment_Method"
    cursorPosgreSQL.execute(query)
    payments = cursorPosgreSQL.fetchall()

    return payments


def getInvoicesDate(cursorPosgreSQL):
    query = "SELECT Invoice_Id, Purchase_Date FROM SALES.tb_Invoice"
    cursorPosgreSQL.execute(query)
    invoicedates = cursorPosgreSQL.fetchall()

    return invoicedates


def getFactTableInvoiceData(cursorPosgreSQL):
    query = "SELECT I.Invoice_Id, I.Payment_Method_Id, IDT.Product_Id, IDT.Product_Amount, IDT.SubTotal_Price, I.Total_Price FROM SALES.tb_Invoice AS I INNER JOIN SALES.tb_Invoice_Details AS IDT ON I.Invoice_Id = IDT.Invoice_Id"
    cursorPosgreSQL.execute(query)
    factsinvoice = cursorPosgreSQL.fetchall()

    return factsinvoice


def createTables(cursorSQLServer):
    query = 'EXEC sp_CreateTables'
    cursorSQLServer.execute(query)
    print("Create table tb_DIM_Product, tb_DIM_Payment_Method, tb_DIM_Invoice_Dates, tb_FACT_Table_Invoice successfully")


def loadtbDIMProduct(cursorSQLServer, products):
    for aux in products:
        query = 'INSERT INTO STOCK.tb_DIM_PRODUCT VALUES(?,?,?);'
        cursorSQLServer.execute(
            query, (aux[0], aux[1], aux[2]))
    print("Products load successfully")


def loadtbDIMPaymentMethod(cursorSQLServer, payment):
    for aux in payment:
        query = 'INSERT INTO SALES.tb_DIM_PAYMENT_METHOD VALUES(?,?);'
        cursorSQLServer.execute(
            query, (aux[0], aux[1]))
    print("Payment Method load successfully")


def loadtbDIMInvoiceDates(cursorSQLServer, invoicedates):
    for aux in invoicedates:
        query = 'INSERT INTO SALES.tb_DIM_INVOICE_DATE VALUES (?,?,?,?)'
        cursorSQLServer.execute(
            query, (aux[0], aux[1], aux[1].strftime('%Y'), aux[1].strftime('%m')))
    print("Invoice Date load successfully")


def loadtbDIMFactTableInvoice(cursorSQLServer, factsinvoice):
    cont = 1
    for aux in factsinvoice:
        query = 'INSERT INTO SALES.tb_FACT_TABLE_INVOICE VALUES (?, ?, ?, ?, ?, ?, ?)'
        cursorSQLServer.execute(
            query, (cont, aux[0], aux[1], aux[2], aux[3], aux[4], aux[5]))
        cont += 1
    print("Fact Table Invoice load successfully")


try:
    connectionPostgreSQL = connectToLocalPosgreSQL()
    print('Connection successfully to PosgreSQL')
    with connectionPostgreSQL.cursor() as cursorPosgreSQL:
        print(
            "\n*****************************\n Extract data in progress...\n*****************************\n")
        prod = getProducts(cursorPosgreSQL)
        payment = getPaymentMethod(cursorPosgreSQL)
        invdate = getInvoicesDate(cursorPosgreSQL)
        factinv = getFactTableInvoiceData(cursorPosgreSQL)

        connectionSQLServer = connectToLocalSQLServer()
        print('Connection successfully to SQLServer')

        with connectionSQLServer.cursor() as cursorSQLServer:

            createTables(cursorSQLServer)

            print(
                "\n*****************************\n Load data in progress...\n*****************************\n")
            loadtbDIMProduct(cursorSQLServer, prod)
            loadtbDIMPaymentMethod(cursorSQLServer, payment)
            loadtbDIMInvoiceDates(cursorSQLServer, invdate)
            loadtbDIMFactTableInvoice(cursorSQLServer, factinv)

            print(
                "\n*****************************\n Load data successfully!!\n*****************************\n")

except Exception as ex:
    print("Error with connection ", ex)
finally:
    print("Connection close")
    connectionPostgreSQL.close()
    connectionSQLServer.close()