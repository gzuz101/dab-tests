import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.utils import *

@dlt.view(comment="Union of Sabertooth sales_order + sales_order_history")
def st_sales_order_union():
    return spark.sql("""
        WITH ST_sales_order_union AS (
            SELECT idx, order_number, erp_sales_order_type, customer_number, customer_po_number,
                   order_imported_datetime, order_date, order_converted_date,
                   erp_ship_by_date, erp_ship_by_date_override_flag, order_scheduled_build_by_date,
                   order_scheduled_ship_by_date, order_ship_by_date, order_build_by_date,
                   order_build_completed_date, fully_assembled, built_to_order, allow_split_ship,
                   expedited_order, expedited_order_build_deadline, needs_ship_by_date_synced_to_erp,
                   expedited_order_ship_deadline, warehouse_code, printed, printed_datetime,
                   ship_via, shipping_routed_datetime, freight_label_printed_datetime,
                   truck_loaded_datetime, date_modified, production_status, shipping_status,
                   international_order, secondary_order_created, parent_order_idx, parent_order_type,
                   campus_idx, route_count, route_attempt_count, campus_routed_initial_datetime,
                   force_routing_by_warehouse_code, auto_route_to_alternate_campus,
                   alternate_auto_route_must_ship_after, calculated_order_ship_by_date,
                   calculated_order_build_by_date, update_sales_order_dates_attempt_count,
                   target_campus_idx, consolidation_status, routing_due_date, scheduled_pick_up_date,
                   applied_manufacturing_buffer_days, live_rating_required, allow_route_to_castlegate,
                   has_error, pooled_shipment, datetime_created, datetime_updated,
                   _fivetran_deleted, _fivetran_synced, selected_live_rate_result_idx,
                   0 AS is_archived
            FROM bronze_st_sales_order
            UNION
            SELECT idx, order_number, erp_sales_order_type, customer_number, customer_po_number,
                   order_imported_datetime, order_date, order_converted_date, erp_ship_by_date,
                   NULL AS erp_ship_by_date_override_flag, order_scheduled_build_by_date,
                   order_scheduled_ship_by_date, order_ship_by_date, order_build_by_date,
                   order_build_completed_date, fully_assembled, built_to_order, allow_split_ship,
                   expedited_order, expedited_order_build_deadline, NULL AS needs_ship_by_date_synced_to_erp,
                   expedited_order_ship_deadline, warehouse_code, printed, printed_datetime,
                   ship_via, shipping_routed_datetime, freight_label_printed_datetime,
                   truck_loaded_datetime, date_modified, production_status, shipping_status,
                   international_order, NULL AS secondary_order_created, parent_order_idx,
                   parent_order_type, campus_idx, route_count, NULL AS route_attempt_count,
                   NULL AS campus_routed_initial_datetime, NULL AS force_routing_by_warehouse_code,
                   NULL AS auto_route_to_alternate_campus, NULL AS alternate_auto_route_must_ship_after,
                   calculated_order_ship_by_date, calculated_order_build_by_date,
                   NULL AS update_sales_order_dates_attempt_count, NULL AS target_campus_idx,
                   NULL AS consolidation_status, routing_due_date, scheduled_pick_up_date,
                   applied_manufacturing_buffer_days, live_rating_required,
                   NULL AS allow_route_to_castlegate, NULL AS has_error, NULL AS pooled_shipment,
                   NULL AS datetime_created, datetime_updated, _fivetran_deleted, _fivetran_synced,
                   selected_live_rate_result_idx, 1 AS is_archived
            FROM bronze_st_sales_order_history
        ),
        ST_sales_order_deduped AS (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY datetime_updated DESC) AS rn
                FROM ST_sales_order_union )
            WHERE rn = 1 )
        SELECT * FROM ST_sales_order_deduped

    """)