UPDATE sales.tb_invoice_details 
SET subtotal_price = (subquery.subtotal)
FROM (select sales.tb_invoice_details.invoice_details_id, (stock.tb_product.product_price * sales.tb_invoice_details.product_amount) as subtotal from sales.tb_invoice_details join stock.tb_product on stock.tb_product.product_id = sales.tb_invoice_details.product_id) 
AS subquery WHERE sales.tb_invoice_details.invoice_details_id = subquery.invoice_details_id 

select I.PRODUCT_ID, I.PRODUCT_AMOUNT, PD.product_price, I.SUBTOTAL_PRICE from sales.tb_invoice_details I
JOIN STOCK.tb_PRODUCT PD ON PD.PRODUCT_ID = I.PRODUCT_ID
WHERE invoice_id = 9;

select * from sales.tb_invoice
WHERE invoice_id = 9;

UPDATE sales.tb_invoice 
SET total_price = (subquery.total)
FROM (select sales.tb_invoice_details.invoice_id, sum( sales.tb_invoice_details.subtotal_price) as total from sales.tb_invoice join sales.tb_invoice_details on sales.tb_invoice_details.invoice_id = sales.tb_invoice.invoice_id group by sales.tb_invoice_details.invoice_id) 
AS subquery WHERE sales.tb_invoice.invoice_id = subquery.invoice_id 
L_DEPOSIT].[tb_CURRENCY]