import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.utils import *

@dlt.view(comment="Joined Acumatica, Shopify, Sabertooth orders")
def unified_orders_staging():
    df =  spark.sql(f"""
        SELECT  aso.Hold as acu_Hold                                            
                ,aso.CuryID as acu_CuryID
                ,aso.NoteID as acu_NoteID
                ,aso.Status as acu_Status
                ,cast(aso.DueDate as timestamp) as acu_DueDate
                ,aso.OpenDoc as acu_OpenDoc
                ,aso.ShipVia as acu_ShipVia
                ,aso.TermsID as acu_TermsID
                ,aso.Approved as acu_Approved
                ,aso.BranchID as acu_BranchID
                ,aso.LineCntr as acu_LineCntr
                ,aso.OrderNbr as acu_OrderNbr
                ,aso.OrderQty as acu_OrderQty
                ,cast(aso.ShipDate as timestamp) as acu_ShipDate
                ,aso.SiteCntr as acu_SiteCntr
                ,aso.UsrSOMBD as acu_UsrSOMBD
                ,aso.UsrSOWBD as acu_UsrSOWBD
                ,aso.Cancelled as acu_Cancelled
                ,aso.CompanyID as acu_CompanyID
                ,aso.Completed as acu_Completed
                ,aso.ContactID as acu_ContactID
                ,aso.ExtRefNbr as acu_ExtRefNbr
                ,aso.Insurance as acu_Insurance
                ,cast(aso.OrderDate as timestamp) as acu_OrderDate
                ,aso.OrderDesc as acu_OrderDesc
                ,aso.OrderType as acu_OrderType
                ,aso.ProjectID as acu_ProjectID
                ,aso.TaxZoneID as acu_TaxZoneID
                ,aso.UsrSOCRSD as acu_UsrSOCRSD
                ,aso.BilledCntr as acu_BilledCntr
                ,cast(aso.CancelDate as timestamp) as acu_CancelDate
                ,aso.CreditHold as acu_CreditHold
                ,aso.CuryInfoID as acu_CuryInfoID
                ,aso.CustomerID as acu_CustomerID
                ,aso.InvoiceNbr as acu_InvoiceNbr
                ,aso.LastSiteID as acu_LastSiteID
                ,aso.CuryDiscTot as acu_CuryDiscTot
                ,aso.CuryMiscTot as acu_CuryMiscTot
                ,aso.CuryPaidAmt as acu_CuryPaidAmt
                ,aso.FinPeriodID as acu_FinPeriodID
                ,cast(aso.InvoiceDate as timestamp) as acu_InvoiceDate
                ,aso.OrderVolume as acu_OrderVolume
                ,aso.OrderWeight as acu_OrderWeight
                ,aso.PaymentCntr as acu_PaymentCntr
                ,cast(aso.RequestDate as timestamp) as acu_RequestDate
                ,aso.Resedential as acu_Resedential
                ,aso.ShipTermsID as acu_ShipTermsID
                ,aso.UsrAllowMFG as acu_UsrAllowMFG
                ,aso.UsrStartMFG as acu_UsrStartMFG
                ,aso.UsrTCDCCode as acu_UsrTCDCCode
                ,aso.CuryTaxTotal as acu_CuryTaxTotal
                ,cast(aso.LastShipDate as timestamp) as acu_LastShipDate
                ,aso.OpenLineCntr as acu_OpenLineCntr
                ,aso.OpenOrderQty as acu_OpenOrderQty
                ,aso.OpenSiteCntr as acu_OpenSiteCntr
                ,aso.OrigOrderNbr as acu_OrigOrderNbr
                ,aso.PMInstanceID as acu_PMInstanceID
                ,aso.ReleasedCntr as acu_ReleasedCntr
                ,aso.ShipComplete as acu_ShipComplete
                ,aso.ShipmentCntr as acu_ShipmentCntr
                ,aso.UsrSOSegment as acu_UsrSOSegment
                ,aso.UsrTCASNType as acu_UsrTCASNType
                ,aso.BillAddressID as acu_BillAddressID
                ,aso.BillContactID as acu_BillContactID
                ,aso.CashAccountID as acu_CashAccountID
                ,aso.CuryLineTotal as acu_CuryLineTotal
                ,aso.DefaultSiteID as acu_DefaultSiteID
                ,aso.OrigOrderType as acu_OrigOrderType
                ,aso.PackageWeight as acu_PackageWeight
                ,aso.SalesPersonID as acu_SalesPersonID
                ,aso.ShipAddressID as acu_ShipAddressID
                ,aso.ShipContactID as acu_ShipContactID
                ,aso.UsrIsSyncToST as acu_UsrIsSyncToST
                ,aso.UsrTCPONumber as acu_UsrTCPONumber
                ,aso.UsrTCRevision as acu_UsrTCRevision
                ,aso.ApprovedCredit as acu_ApprovedCredit
                ,aso.BillSeparately as acu_BillSeparately
                ,aso.CuryFreightAmt as acu_CuryFreightAmt
                ,aso.CuryFreightTot as acu_CuryFreightTot
                ,aso.CuryOrderTotal as acu_CuryOrderTotal
                ,aso.CustomerRefNbr as acu_CustomerRefNbr
                ,aso.ShipSeparately as acu_ShipSeparately
                ,aso.UsrKNMCTaxRate as acu_UsrKNMCTaxRate
                ,cast(aso.CreatedDateTime as timestamp) as acu_CreatedDateTime
                ,aso.CuryFreightCost as acu_CuryFreightCost
                ,aso.OverrideTaxZone as acu_OverrideTaxZone
                ,aso.PackageLineCntr as acu_PackageLineCntr
                ,aso.PaymentMethodID as acu_PaymentMethodID
                ,aso.CuryOpenTaxTotal as acu_CuryOpenTaxTotal
                ,aso.CuryPaymentTotal as acu_CuryPaymentTotal
                ,aso.CustomerOrderNbr as acu_CustomerOrderNbr
                ,aso.OpenShipmentCntr as acu_OpenShipmentCntr
                ,aso.PrepaymentReqPct as acu_PrepaymentReqPct
                ,aso.UnbilledOrderQty as acu_UnbilledOrderQty
                ,aso.UsrSOApplyCredit as acu_UsrSOApplyCredit
                ,aso.UsrSOPenaltyType as acu_UsrSOPenaltyType
                ,cast(aso.UsrSOSchShipDate as timestamp) as acu_UsrSOSchShipDate
                ,aso.UsrTCStoreNumber as acu_UsrTCStoreNumber
                ,aso.ApprovedCreditAmt as acu_ApprovedCreditAmt
                ,aso.CuryOpenDiscTotal as acu_CuryOpenDiscTotal
                ,aso.CuryOpenLineTotal as acu_CuryOpenLineTotal
                ,aso.CuryUnpaidBalance as acu_CuryUnpaidBalance
                ,aso.DestinationSiteID as acu_DestinationSiteID
                ,aso.UsrSOCustPrefWhse as acu_UsrSOCustPrefWhse
                ,aso.UsrTCCustomField1 as acu_UsrTCCustomField1
                ,aso.UsrTCCustomField2 as acu_UsrTCCustomField2
                ,aso.UsrTCCustomField3 as acu_UsrTCCustomField3
                ,aso.UsrTCCustomField4 as acu_UsrTCCustomField4
                ,aso.UsrTCCustomField5 as acu_UsrTCCustomField5
                ,aso.CuryOpenOrderTotal as acu_CuryOpenOrderTotal
                ,aso.CuryPaymentOverall as acu_CuryPaymentOverall
                ,aso.CustomerLocationID as acu_CustomerLocationID
                ,cast(aso.UsrSOCancelledDate as timestamp) as acu_UsrSOCancelledDate
                ,aso.UsrSOHAllowSTFetch as acu_UsrSOHAllowSTFetch
                ,aso.UsrSOOrderComments as acu_UsrSOOrderComments
                ,cast(aso.UsrSORoutedByMESDT as timestamp) as acu_UsrSORoutedByMESDT
                ,aso.CuryUnbilledMiscTot as acu_CuryUnbilledMiscTot
                ,aso.ExternalOrderSource as acu_ExternalOrderSource
                ,aso.FreightAmountSource as acu_FreightAmountSource
                ,aso.UsrSOAllowEarlyShip as acu_UsrSOAllowEarlyShip
                ,cast(aso.UsrSOFetchedByMESDT as timestamp) as acu_UsrSOFetchedByMESDT
                ,cast(aso.UsrSOOrgSchShipDate as timestamp) as acu_UsrSOOrgSchShipDate
                ,aso.UsrSOOriginInstruct as acu_UsrSOOriginInstruct
                ,aso.CuryPrepaymentReqAmt as acu_CuryPrepaymentReqAmt
                ,aso.CuryUnbilledTaxTotal as acu_CuryUnbilledTaxTotal
                ,aso.FreightTaxCategoryID as acu_FreightTaxCategoryID
                ,cast(aso.LastModifiedDateTime as timestamp) as acu_LastModifiedDateTime
                ,cast(aso.UsrReleaseToPlanning as timestamp) as acu_UsrReleaseToPlanning
                ,aso.UsrSOAppliedLeadTime as acu_UsrSOAppliedLeadTime
                ,aso.UsrSOApplyRestockFee as acu_UsrSOApplyRestockFee
                ,aso.UsrSOPillowChargeAmt as acu_UsrSOPillowChargeAmt
                ,aso.UsrSOProductionNotes as acu_UsrSOProductionNotes
                ,aso.UsrSOSurchargeAmount as acu_UsrSOSurchargeAmount
                ,aso.CuryPremiumFreightAmt as acu_CuryPremiumFreightAmt
                ,aso.CuryUnbilledDiscTotal as acu_CuryUnbilledDiscTotal
                ,aso.CuryUnbilledLineTotal as acu_CuryUnbilledLineTotal
                ,aso.ExternalOrderOriginal as acu_ExternalOrderOriginal
                ,aso.OverrideFreightAmount as acu_OverrideFreightAmount
                ,aso.UsrKNMCISMagentoOrder as acu_UsrKNMCISMagentoOrder
                ,aso.UsrKNMCMagentoOrderID as acu_UsrKNMCMagentoOrderID
                ,aso.UsrKNMCStoreCreditAmt as acu_UsrKNMCStoreCreditAmt
                ,aso.UsrSOAllowPartialShip as acu_UsrSOAllowPartialShip
                ,aso.UsrSOOriginalSONumber as acu_UsrSOOriginalSONumber
                ,cast(aso.UsrSOProcessedByMESDT as timestamp) as acu_UsrSOProcessedByMESDT
                ,aso.CuryBilledPaymentTotal as acu_CuryBilledPaymentTotal
                ,aso.CuryUnbilledOrderTotal as acu_CuryUnbilledOrderTotal
                ,aso.UsrKNMCMagentoTaxTotal as acu_UsrKNMCMagentoTaxTotal
                ,aso.UsrOverOrExtremeLength as acu_UsrOverOrExtremeLength
                ,cast(aso.UsrSOImportedIntoMESDT as timestamp) as acu_UsrSOImportedIntoMESDT
                ,aso.UsrSORoutingBufferDays as acu_UsrSORoutingBufferDays
                ,aso.UsrSOUserDefinedField6 as acu_UsrSOUserDefinedField6
                ,aso.UsrSOUserDefinedField7 as acu_UsrSOUserDefinedField7
                ,aso.UsrSOUserDefinedField8 as acu_UsrSOUserDefinedField8
                ,aso.UsrSOUserDefinedField9 as acu_UsrSOUserDefinedField9
                ,aso.ApprovedCreditByPayment as acu_ApprovedCreditByPayment
                ,aso.UsrSODestinationInstruc as acu_UsrSODestinationInstruc
                ,aso.UsrSORBDCalculationType as acu_UsrSORBDCalculationType
                ,aso.UsrSOUserDefinedField10 as acu_UsrSOUserDefinedField10
                ,aso.UsrSOUserDefinedField11 as acu_UsrSOUserDefinedField11
                ,aso.UsrSOUserDefinedField12 as acu_UsrSOUserDefinedField12
                ,aso.UsrSOUserDefinedField13 as acu_UsrSOUserDefinedField13
                ,aso.UsrSOUserDefinedField14 as acu_UsrSOUserDefinedField14
                ,aso.UsrSOUserDefinedField15 as acu_UsrSOUserDefinedField15
                ,aso.UsrSOUserDefinedField16 as acu_UsrSOUserDefinedField16
                ,aso.UsrSOUserDefinedField17 as acu_UsrSOUserDefinedField17
                ,aso.UsrSOUserDefinedField18 as acu_UsrSOUserDefinedField18
                ,aso.UsrSOUserDefinedField19 as acu_UsrSOUserDefinedField19
                ,aso.UsrSOUserDefinedField20 as acu_UsrSOUserDefinedField20
                ,aso.UsrSOUserDefinedField21 as acu_UsrSOUserDefinedField21
                ,aso.UsrSOWarrantyPolicyType as acu_UsrSOWarrantyPolicyType
                ,aso.AvalaraCustomerUsageType as acu_AvalaraCustomerUsageType
                ,aso.CuryUnreleasedPaymentAmt as acu_CuryUnreleasedPaymentAmt
                ,aso.UsrKNMCMagentoRMAOrderID as acu_UsrKNMCMagentoRMAOrderID
                ,aso.UsrSOChargeReturnFreight as acu_UsrSOChargeReturnFreight
                ,aso.UsrSOMagentoStockedOrder as acu_UsrSOMagentoStockedOrder
                ,cast(aso.UsrSOOrgRequiredShipDate as timestamp) as acu_UsrSOOrgRequiredShipDate
                ,aso.UsrSOReplacementOrderNbr as acu_UsrSOReplacementOrderNbr
                ,aso.UsrSOZendeskTicketNumber as acu_UsrSOZendeskTicketNumber
                ,aso.UsrKNMCStoreCreditBalance as acu_UsrKNMCStoreCreditBalance
                ,aso.UsrSOCreditHoldReasonCode as acu_UsrSOCreditHoldReasonCode
                ,aso.UsrSOReplacementOrderType as acu_UsrSOReplacementOrderType
                ,aso.ExternalTaxExemptionNumber as acu_ExternalTaxExemptionNumber
                ,aso.PaymentsNeedValidationCntr as acu_PaymentsNeedValidationCntr
                ,aso.UsrSHWKCHKLiftgateDelivery as acu_UsrSHWKCHKLiftgateDelivery
                ,aso.UsrSHWKNotifyPriorDelivery as acu_UsrSHWKNotifyPriorDelivery
                ,aso.UsrKNMCReturnTrackingNumber as acu_UsrKNMCReturnTrackingNumber
                ,aso.UsrSOCustomerClassification as acu_UsrSOCustomerClassification
                ,aso.UsrSOForceRequestedShipDate as acu_UsrSOForceRequestedShipDate
                ,aso.UsrSOForceRoutingByWhseCode as acu_UsrSOForceRoutingByWhseCode
                ,aso.UsrSOOrderOrderTypeLeadTime as acu_UsrSOOrderOrderTypeLeadTime
                ,aso.UsrKNMCIsStoreCreditAdjusted as acu_UsrKNMCIsStoreCreditAdjusted
                ,aso.UsrSOManuallyOverrideShipVia as acu_UsrSOManuallyOverrideShipVia
                ,aso.UsrSOShipViaInTranslationRan as acu_UsrSOShipViaInTranslationRan
                ,aso.UsrSOAllowSplitShipByLocation as acu_UsrSOAllowSplitShipByLocation
                ,cast(aso.UsrSOOrgRequestedDeliveryDate as timestamp) as acu_UsrSOOrgRequestedDeliveryDate
                ,aso.UsrSOShipViaOutTranslationRan as acu_UsrSOShipViaOutTranslationRan
                ,cast(aso.UsrSOCustomerRequestedShipDate as timestamp) as acu_UsrSOCustomerRequestedShipDate
                ,cast(aso.UsrSOCancellationProcessedByMESDT as timestamp) as acu_UsrSOCancellationProcessedByMESDT
                ,aso.DisableAutomaticDiscountCalculation as acu_DisableAutomaticDiscountCalculation
                ,aso.CuryDiscTot + aso.CuryLineTotal + aso.CuryOpenDiscTotal + aso.CuryUnbilledDiscTotal as acu_CuryOrderDiscTotal
                ,stso.idx as st_idx
                ,stso.order_number as st_order_number
                ,stso.erp_sales_order_type as st_erp_sales_order_type
                ,cast(stso.order_imported_datetime as timestamp) as st_order_imported_datetime
                ,stso.order_date as st_order_date
                ,stso.order_converted_date as st_order_converted_date
                ,stso.erp_ship_by_date as st_erp_ship_by_date
                ,stso.erp_ship_by_date_override_flag as st_erp_ship_by_date_override_flag
                ,stso.order_scheduled_build_by_date as st_order_scheduled_build_by_date
                ,stso.order_scheduled_ship_by_date as st_order_scheduled_ship_by_date
                ,stso.order_ship_by_date as st_order_ship_by_date
                ,stso.order_build_by_date as st_order_build_by_date
                ,cast(stso.order_build_completed_date as timestamp) as st_order_build_completed_date
                ,stso.fully_assembled as st_fully_assembled
                ,stso.built_to_order as st_built_to_order
                ,stso.allow_split_ship as st_allow_split_ship
                ,stso.expedited_order as st_expedited_order
                ,stso.expedited_order_build_deadline as st_expedited_order_build_deadline
                ,stso.expedited_order_ship_deadline as st_expedited_order_ship_deadline
                ,stso.warehouse_code as st_warehouse_code
                ,stso.printed as st_printed
                ,cast(stso.printed_datetime as timestamp) as st_printed_datetime
                ,stso.ship_via as st_ship_via
                ,cast(stso.shipping_routed_datetime as timestamp) as st_shipping_routed_datetime
                ,stso.freight_label_printed_datetime as st_freight_label_printed_datetime
                ,stso.truck_loaded_datetime as st_truck_loaded_datetime
                ,stso.date_modified as st_date_modified
                ,stso.production_status as st_production_status
                ,stso.shipping_status as st_shipping_status
                ,stso.international_order as st_international_order
                ,stso.secondary_order_created as st_secondary_order_created
                ,stso.parent_order_idx as st_parent_order_idx
                ,stso.parent_order_type as st_parent_order_type
                ,stso.campus_idx as st_campus_idx
                ,stso.route_count as st_route_count
                ,stso.campus_routed_initial_datetime as st_campus_routed_initial_datetime
                ,stso.force_routing_by_warehouse_code as st_force_routing_by_warehouse_code
                ,stso.auto_route_to_alternate_campus as st_auto_route_to_alternate_campus
                ,stso.alternate_auto_route_must_ship_after as st_alternate_auto_route_must_ship_after
                ,stso.calculated_order_ship_by_date as st_calculated_order_ship_by_date
                ,stso.calculated_order_build_by_date as st_calculated_order_build_by_date
                ,stso.consolidation_status as st_consolidation_status
                ,stso.routing_due_date as st_routing_due_date
                ,stso.scheduled_pick_up_date as st_scheduled_pick_up_date
                ,stso.applied_manufacturing_buffer_days as st_applied_manufacturing_buffer_days
                ,stso.live_rating_required as st_live_rating_required
                ,stso.allow_route_to_castlegate as st_allow_route_to_castlegate
                ,stso.pooled_shipment as st_pooled_shipment
                ,stso.datetime_created as st_datetime_created
                ,cast(stso.datetime_updated as timestamp) as st_datetime_updated
                ,stso.selected_live_rate_result_idx as st_selected_live_rate_result_idx
                ,stso.parent_order_type as st_ordertype
                ,stso.is_archived as st_is_archived
                ,sso.id as shpy_id
                ,sso.note as shpy_note
                ,sso.email as shpy_email
                ,sso.taxes_included as shpy_taxes_included
                ,sso.currency as shpy_currency
                ,sso.subtotal_price as shpy_subtotal_price
                ,sso.total_tax as shpy_total_tax
                ,sso.total_price as shpy_total_price
                ,cast(sso.created_at as timestamp) as shpy_created_at
                ,cast(sso.updated_at as timestamp) as shpy_updated_at
                ,sso.name as shpy_name
                ,sso.shipping_address_name as shpy_shipping_address_name
                ,sso.shipping_address_first_name as shpy_shipping_address_first_name
                ,sso.shipping_address_last_name as shpy_shipping_address_last_name
                ,sso.shipping_address_company as shpy_shipping_address_company
                ,sso.shipping_address_phone as shpy_shipping_address_phone
                ,sso.shipping_address_address_1 as shpy_shipping_address_address_1
                ,sso.shipping_address_address_2 as shpy_shipping_address_address_2
                ,sso.shipping_address_city as shpy_shipping_address_city
                ,sso.shipping_address_country as shpy_shipping_address_country
                ,sso.shipping_address_country_code as shpy_shipping_address_country_code
                ,sso.shipping_address_province as shpy_shipping_address_province
                ,sso.shipping_address_province_code as shpy_shipping_address_province_code
                ,sso.shipping_address_zip as shpy_shipping_address_zip
                ,sso.shipping_address_latitude as shpy_shipping_address_latitude
                ,sso.shipping_address_longitude as shpy_shipping_address_longitude
                ,sso.billing_address_name as shpy_billing_address_name
                ,sso.billing_address_first_name as shpy_billing_address_first_name
                ,sso.billing_address_last_name as shpy_billing_address_last_name
                ,sso.billing_address_company as shpy_billing_address_company
                ,sso.billing_address_phone as shpy_billing_address_phone
                ,sso.billing_address_address_1 as shpy_billing_address_address_1
                ,sso.billing_address_address_2 as shpy_billing_address_address_2
                ,sso.billing_address_city as shpy_billing_address_city
                ,sso.billing_address_country as shpy_billing_address_country
                ,sso.billing_address_country_code as shpy_billing_address_country_code
                ,sso.billing_address_province as shpy_billing_address_province
                ,sso.billing_address_province_code as shpy_billing_address_province_code
                ,sso.billing_address_zip as shpy_billing_address_zip
                ,sso.billing_address_latitude as shpy_billing_address_latitude
                ,sso.billing_address_longitude as shpy_billing_address_longitude
                ,sso.customer_id as shpy_customer_id
                ,sso.location_id as shpy_location_id
                ,sso.user_id as shpy_user_id
                ,sso.company_id as shpy_company_id
                ,sso.company_location_id as shpy_company_location_id
                ,sso.app_id as shpy_app_id
                ,sso.financial_status as shpy_financial_status
                ,sso.fulfillment_status as shpy_fulfillment_status
                ,cast(sso.processed_at as timestamp) as shpy_processed_at
                ,sso.referring_site as shpy_referring_site
                ,sso.cancel_reason as shpy_cancel_reason
                ,sso.cancelled_at as shpy_cancelled_at
                ,cast(sso.closed_at as timestamp) as shpy_closed_at
                ,sso.total_discounts as shpy_total_discounts
                ,sso.current_total_price as shpy_current_total_price
                ,sso.current_total_discounts as shpy_current_total_discounts
                ,sso.current_subtotal_price as shpy_current_subtotal_price
                ,sso.current_total_tax as shpy_current_total_tax
                ,sso.total_line_items_price as shpy_total_line_items_price
                ,sso.total_weight * 0.0022046226 as shpy_total_weight --conversion to pounds
                ,sso.source_name as shpy_source_name
                ,sso.browser_ip as shpy_browser_ip
                ,sso.buyer_accepts_marketing as shpy_buyer_accepts_marketing
                ,sso.token as shpy_token
                ,sso.cart_token as shpy_cart_token
                ,sso.checkout_token as shpy_checkout_token
                ,sso.checkout_id as shpy_checkout_id
                ,sso.customer_locale as shpy_customer_locale
                ,sso.landing_site_ref as shpy_landing_site_ref
                ,sso.presentment_currency as shpy_presentment_currency
                ,sso.order_status_url as shpy_order_status_url
                ,sso.payment_gateway_names as shpy_payment_gateway_names
                ,sso.client_details_user_agent as shpy_client_details_user_agent
                ,sso.landing_site_base_url as shpy_landing_site_base_url
        FROM bronze_acumatica_soorder aso
        LEFT JOIN st_sales_order_union stso ON stso.order_number = aso.OrderNbr
        LEFT JOIN bronze_shopify_order sso ON sso.name = aso.CustomerOrderNbr
    """)
    return df