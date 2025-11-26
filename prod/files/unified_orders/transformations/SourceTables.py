import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.utils import *
@dlt.view 
def bronze_acumatica_soorder(): 
    return spark.table("production.edw_acumatica_bronze.dbo_soorder")
@dlt.view 
def bronze_shopify_order(): 
    return spark.table("production.shopify_bronze.order")
@dlt.view 
def bronze_st_sales_order(): 
    return spark.table("production.mysql_bronze_polywood_scheduler.sales_order")
@dlt.view 
def bronze_st_sales_order_history(): 
    return spark.table("production.mysql_bronze_polywood_scheduler.sales_order_history")