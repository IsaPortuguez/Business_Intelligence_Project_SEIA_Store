USE [IF5100_2022_SEIA_STORE]

EXEC [dbo].[sp_CreateTables]

SELECT * FROM STOCK.tb_DIM_PRODUCT
SELECT * FROM [SALES].[tb_DIM_PAYMENT_METHOD]
SELECT * FROM [SALES].[tb_DIM_INVOICE_DATE]
SELECT * FROM [SALES].[tb_FACT_TABLE_INVOICE]