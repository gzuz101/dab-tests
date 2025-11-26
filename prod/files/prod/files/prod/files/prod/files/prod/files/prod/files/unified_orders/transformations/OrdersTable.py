import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from delta.tables import DeltaTable
from pyspark.sql.utils import *
@dlt.table(
    name="unified_orders_dab",
    comment="Final unified orders table with incremental upserts via auto_cdc_flow",
    table_properties={"delta.enableChangeDataFeed": "true"}
)

def unified_orders():
    unified_staging = dlt.read("unified_orders_staging")
    # CDC for Acumatica
    dlt.create_auto_cdc_flow(
        source=unified_staging,
        target="unified_orders",
        keys=["acu_OrderNbr"],
        sequence_by=col("acu_LastModifiedDateTime"),
        stored_as_scd_type= 1
    )
    dlt.create_auto_cdc_flow(
        source=unified_staging,
        target="unified_orders",
        keys=["st_order_number"],
        sequence_by="st_datetime_updated",
        name="cdc_sabertooth"
    )
    dlt.create_auto_cdc_flow(
        source=unified_staging,
        target="unified_orders",
        keys=["shpy_name"],
        sequence_by="shpy_updated_at",
        name="cdc_shopify"
    )
    return unified_staging
















