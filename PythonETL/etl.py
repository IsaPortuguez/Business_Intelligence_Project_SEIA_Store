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


def getProducts(cursorPosgreSQL):
    query = "SELECT Product_Id, Product_Name, Product_Price FROM STOCK.tb_Product"
    cursorPosgreSQL.execute(query)
    products = cursorPosgreSQL.fetchall()

    with open('products.csv', 'w') as csvProducts:
        fieldnames = ['Product_Id', 'Product_Name', 'Product_Price']
        writer = csv.DictWriter(csvProducts, fieldnames=fieldnames)
        writer.writeheader()
        for aux in products:
            writer.writerow(
                {'Product_Id': aux[0], 'Product_Name': aux[1], 'Product_Price': aux[2]})


def getPaymentMethod(cursorPosgreSQL):
    query = "SELECT I.Invoice_Id, PM.Payment_Method_Id, PM.Payment_Method_Name FROM SALES.tb_Invoice AS I JOIN SALES.tb_Payment_Method AS PM ON PM.Payment_Method_Id = I.Payment_Method_Id"
    cursorPosgreSQL.execute(query)
    sales = cursorPosgreSQL.fetchall()

    with open('paymentmethod.csv', 'w') as csvSales:
        fieldnames = ['Invoice_Id', 'Payment_Method_Id', 'Payment_Method']
        writer = csv.DictWriter(csvSales, fieldnames=fieldnames)
        writer.writeheader()
        for aux in sales:
            writer.writerow(
                {'Invoice_Id': aux[0], 'Payment_Method_Id': aux[1], 'Payment_Method': aux[2]})


def getInvoicesDate(cursorPosgreSQL):
    query = "SELECT Invoice_Id, Purchase_Date FROM SALES.tb_Invoice"
    cursorPosgreSQL.execute(query)
    dates = cursorPosgreSQL.fetchall()

    with open('invoicesdates.csv', 'w') as csvDates:
        fieldnames = ['Invoice_Id',
                      'Purchase_Date', 
                      'Purchase_Year', 
                      'Purchase_Month']
        writer = csv.DictWriter(csvDates, fieldnames=fieldnames)
        writer.writeheader()
        for aux in dates:
            writer.writerow(
                {'Invoice_Id': aux[0], 'Purchase_Date': aux[1], 'Purchase_Year': aux[1].strftime('%Y'), 'Purchase_Month': aux[1].strftime('%m')})


def getFactTableInvoiceData(cursorPosgreSQL):
    query = "SELECT I.Invoice_Id, I.Payment_Method_Id, IDT.Product_Id, IDT.Product_Amount, IDT.SubTotal_Price, I.Total_Price FROM SALES.tb_Invoice AS I INNER JOIN SALES.tb_Invoice_Details AS IDT ON I.Invoice_Id = IDT.Invoice_Id"
    cursorPosgreSQL.execute(query)
    facts = cursorPosgreSQL.fetchall()

    with open('factsinvoice.csv', 'w') as csvFacts:
        fieldnames = ['Id', 'Invoice_Purchase_Date_Id', 'Payment_Method_Id', 'Product_Id', 
                      'Product_Amount', 'SubTotal_Price', 'Total_Price']
        writer = csv.DictWriter(csvFacts, fieldnames=fieldnames)

        writer.writeheader()
        count = 0
        for aux in facts:
            writer.writerow(
                {'Id': count, 'Invoice_Purchase_Date_Id': aux[0], 'Payment_Method_Id': aux[1], 'Product_Id': aux[1],
                 'Product_Amount': aux[2], 'SubTotal_Price': aux[3], 'Total_Price': aux[4]})
            count += 1


def createTables(cursorSQLServer):
    query = 'EXEC sp_CreateTables'
    cursorSQLServer.execute(query)
    print("Create table tb_DIM_Product, tb_DIM_Payment_Method, tb_DIM_Invoice_Dates, tb_FACT_Table_Invoice successfully")


def loadtbDIMProduct(cursorSQLServer):
    query = "BULK INSERT STOCK.tb_DIM_Product FROM '"+os.path.dirname(os.path.abspath(
        __file__))+"\\products.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')"
    cursorSQLServer.execute(query)
    cursorSQLServer.commit()
    print("Product load successfully")


def loadtbDIMPaymentMethod(cursorSQLServer):
    query = "BULK INSERT SALES.tb_DIM_Payment_Method FROM '"+os.path.dirname(os.path.abspath(
        __file__))+"\\paymentmethod.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')"
    cursorSQLServer.execute(query)
    cursorSQLServer.commit()
    print("Payment Method load successfully")


def loadtbDIMInvoiceDates(cursorSQLServer):
    query = "BULK INSERT SALES.tb_DIM_Invoice_Dates FROM '"+os.path.dirname(os.path.abspath(
        __file__))+"\\invoicesdates.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')"
    cursorSQLServer.execute(query)
    cursorSQLServer.commit()
    print("Invoice Date load successfully")


def loadtbDIMFactTableInvoice(cursorSQLServer):
    query = "BULK INSERT SALES.tb_FACT_Table_Invoice FROM '"+os.path.dirname(os.path.abspath(
        __file__))+"\\factsinvoice.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')"
    cursorSQLServer.execute(query)
    cursorSQLServer.commit()
    print("Fact Table Invoice load successfully")


try:
    connectionPostgreSQL = connectToPosgreSQL()
    print('Connection successfully to PosgreSQL')
    with connectionPostgreSQL.cursor() as cursorPosgreSQL:
        print(
            "\n*****************************\n Extract data in progress...\n*****************************\n")
        getProducts(cursorPosgreSQL)
        getPaymentMethod(cursorPosgreSQL)
        getInvoicesDate(cursorPosgreSQL)
        getFactTableInvoiceData(cursorPosgreSQL)

        #connectionSQLServer = connectSQLServer()
        connectionSQLServer = connectToLocalSQLServer()
        print('Connection successfully to SQLServer')

        with connectionSQLServer.cursor() as cursorSQLServer:

            createTables(cursorSQLServer)

            print(
                "\n*****************************\n Load data in progress...\n*****************************\n")
            loadtbDIMProduct(cursorSQLServer)
            loadtbDIMPaymentMethod(cursorSQLServer)
            loadtbDIMInvoiceDates(cursorSQLServer)
            loadtbDIMFactTableInvoice(cursorSQLServer)

            print(
                "\n*****************************\n Load data successfully!!\n*****************************\n")

except Exception as ex:
    print("Error with connection ", ex)
finally:
    print("Connection close")
    connectionPostgreSQL.close()
    connectionSQLServer.close()