# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "AvantisSourceSilver",
# META       "known_lakehouses": [
# META         {"id": "d04b2b50-92dd-41e9-80c6-e04068075313"},
# META         {"id": "adfbc86c-1b58-4463-918e-6cce10ada007"}
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver_Transform_Procurement
# 
# Builds the procurement / inventory-catalog silver layer for the supply-chain team.
# Sourced from `AvantisSourceBronze.mc`; written to `AvantisSourceSilver`.
# 
# **Source tables** (verified present in bronze on 2026-05-07):
# `mc.POSUM`, `mc.POLINE`, `mc.PCITEM`, `mc.VENDOR`, `mc.VENDORPART`, `mc.MANUFACTURE`
# 
# **Output silver tables**:
# `dim_vendor`, `dim_manufacturer`, `dim_item`, `dim_vendor_part`, `fact_po_header`, `fact_po_line`

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

BRONZE = "AvantisSourceBronze"
SILVER = "AvantisSourceSilver"

def write_silver(df, table_name):
    target = f"{SILVER}.{table_name}"
    print(f"--> writing {target} ({df.count():,} rows)")
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. dim_vendor
vendor = spark.table(f"{BRONZE}.mc.VENDOR")
dim_vendor = (vendor
    .select(
        col("VENDOI").cast("int").alias("vendor_key"),
        col("ID").alias("vendor_id"),
        col("AENM").alias("vendor_name"),
        col("APPTEXT").alias("vendor_description"),
        col("CUR").alias("currency"),
        col("VENTYP_OI").cast("int").alias("vendor_type_key"),
        col("PAYTRM_OI").cast("int").alias("payment_terms_key"),
        col("CNTCTYP_OI").cast("int").alias("contact_key"),
        col("LANGUAGE_ID").alias("language_id"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["vendor_key"]))
write_silver(dim_vendor, "dim_vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. dim_manufacturer
mfg = spark.table(f"{BRONZE}.mc.MANUFACTURE")
dim_manufacturer = (mfg
    .select(
        col("MANFOI").cast("int").alias("manufacturer_key"),
        col("MANFID").alias("manufacturer_id"),
        col("AENM").alias("manufacturer_name"),
        col("ABRV").alias("manufacturer_abbrev"),
        col("STNUM").alias("dnb_number"),
        col("APPTEXT").alias("manufacturer_description"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["manufacturer_key"]))
write_silver(dim_manufacturer, "dim_manufacturer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. dim_item (catalog grain - PCITEM)
pcitem = spark.table(f"{BRONZE}.mc.PCITEM")
dim_item = (pcitem
    .select(
        col("PCITOI").cast("int").alias("item_key"),
        col("ITID").alias("item_id"),
        col("AENM").alias("item_name"),
        col("APPTEXT").alias("item_description"),
        col("UOM").alias("uom"),
        col("ALT_UOM").alias("alt_uom"),
        col("ITEMTYP_OI").cast("int").alias("item_type_key"),
        col("BUY_OI").cast("int").alias("buyer_key"),
        col("ECATNUM").alias("ecatalog_number"),
        col("ITWT_AMT").cast("double").alias("weight_amount"),
        col("ITWT_UOM").alias("weight_uom"),
        col("LASTQUOTED_DTTM").alias("last_quoted_dttm"),
        col("NOCONTRACT").cast("int").alias("is_no_contract"),
        col("DIRPUR").cast("int").alias("allow_direct_purchase"),
        col("TMPLT").cast("int").alias("is_template"),
        F.when(col("SUSP_OI").isNotNull(), lit(1)).otherwise(lit(0)).alias("is_suspended"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["item_key"]))
write_silver(dim_item, "dim_item")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. dim_vendor_part (PCItem x Vendor - carries MFG PN + Vendor PN)
vendorpart = spark.table(f"{BRONZE}.mc.VENDORPART")
dim_vendor_part = (vendorpart
    .select(
        col("VDITOI").cast("int").alias("vendor_part_key"),
        col("AKA").alias("vendor_part_aka"),
        col("VND_OI").cast("int").alias("vendor_key"),
        col("PCITEM_OI").cast("int").alias("item_key"),
        col("MANUF_OI").cast("int").alias("manufacturer_key"),
        col("MPARTNUM").alias("mfg_part_number"),
        col("VDPT").alias("vendor_part_number"),
        col("EVENDPART").alias("e_vendor_part_number"),
        col("AENM").alias("vendor_part_name"),
        col("APPTEXT").alias("vendor_part_description"),
        col("UOM").alias("uom"),
        col("LEADTIME_AMT").cast("double").alias("lead_time_amount"),
        col("LEADTIME_UOM").alias("lead_time_uom"),
        col("MINQTY_AMT").cast("double").alias("min_qty_amount"),
        col("MINQTY_UOM").alias("min_qty_uom"),
        col("ORDERQTY_AMT").cast("double").alias("order_qty_amount"),
        col("ORDERQTY_UOM").alias("order_qty_uom"),
        col("PRVD").cast("int").alias("is_prime_vendor"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["vendor_part_key"]))
write_silver(dim_vendor_part, "dim_vendor_part")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. fact_po_header (1 row per PO - all sites, all history)
posum = spark.table(f"{BRONZE}.mc.POSUM")
fact_po_header = (posum
    .select(
        col("POSUMOI").cast("int").alias("po_key"),
        col("ID").alias("po_number"),
        col("BEPARN_OI").cast("int").alias("site_key"),
        col("VND_OI").cast("int").alias("vendor_key"),
        col("BUY_OI").cast("int").alias("buyer_key"),
        col("POAGENT_OI").cast("int").alias("po_agent_key"),
        col("ORDTYP_OI").cast("int").alias("order_type_key"),
        col("CONTREF_OI").cast("int").alias("contract_key"),
        col("REGION_OI").cast("int").alias("region_key"),
        col("PODT_DTTM").alias("po_date"),
        col("POSTAT").cast("int").alias("po_status"),
        col("CURREVNO").cast("int").alias("current_revision"),
        col("CONFIRMORD").cast("int").alias("is_confirmation_order"),
        col("WAITTRANSMIT").cast("int").alias("is_awaiting_transmit"),
        col("POCUR_CUR").alias("po_currency"),
        col("POBCUR_CUR").alias("po_base_currency"),
        col("TOAMT_AMT").cast("double").alias("total_amount"),
        col("TOAMT_CUR").alias("total_amount_currency"),
        col("PRNTED").cast("int").alias("print_count"),
        col("LASTPR_DTTM").alias("last_printed_dttm"),
        col("CLOSINF_CLOS").cast("int").alias("is_closed"),
        col("CLOSINF_CLSDT_DATE").alias("closed_date"),
        col("CI_CNCL_OI").cast("int").alias("cancel_code_key"),
        col("CI_CNC_DATE").alias("cancelled_date"),
        col("HLD_HLD_OI").cast("int").alias("hold_code_key"),
        col("HLD_HELD_DATE").alias("hold_date"),
        col("APRI_STATUS").cast("int").alias("approval_status"),
        col("APRI_STATUS_DTTM").alias("approval_status_dttm"),
        col("RELEASENO").cast("int").alias("contract_release_number"),
        col("AENM").alias("po_title"),
        col("APPTEXT").alias("po_description"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["po_key"]))
write_silver(fact_po_header, "fact_po_header")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. fact_po_line (1 row per PO line - what was ordered)
poline = spark.table(f"{BRONZE}.mc.POLINE")
fact_po_line = (poline
    .select(
        col("POLNOI").cast("int").alias("po_line_key"),
        col("PO_OI").cast("int").alias("po_key"),
        col("ID").cast("int").alias("po_line_number"),
        col("PCITEM_OI").cast("int").alias("item_key"),
        col("VNDITM_OI").cast("int").alias("vendor_part_key"),
        col("CONTREF_OI").cast("int").alias("contract_key"),
        col("QTYORD_AMT").cast("double").alias("qty_ordered"),
        col("QTYORD_UOM").alias("qty_ordered_uom"),
        col("TQORDER_AMT").cast("double").alias("total_qty_ordered"),
        col("TQORDER_UOM").alias("total_qty_ordered_uom"),
        col("TQREC_AMT").cast("double").alias("total_qty_received"),
        col("TQREC_UOM").alias("total_qty_received_uom"),
        col("RECDFULL").cast("int").alias("is_received_full"),
        col("TOAMT_AMT").cast("double").alias("line_total_amount"),
        col("TOAMT_CUR").alias("line_total_currency"),
        col("POLINEFRM").cast("int").alias("po_line_type"),
        col("CONSIGN").cast("int").alias("is_consignment"),
        col("DELSUM").cast("int").alias("qty_from_delivery_sum"),
        col("CURREVNO").cast("int").alias("current_revision"),
        col("CLOSINF_CLOS").cast("int").alias("is_closed"),
        col("CLOSINF_CLSDT_DATE").alias("closed_date"),
        col("CI_CNCL_OI").cast("int").alias("cancel_code_key"),
        col("CI_CNC_DATE").alias("cancelled_date"),
        col("HLD_HLD_OI").cast("int").alias("hold_code_key"),
        col("DTADDPO_DTTM").alias("date_added_to_po"),
        col("APRI_STATUS").cast("int").alias("approval_status"),
        col("APRI_STATUS_DTTM").alias("approval_status_dttm"),
        col("AENM").alias("po_line_title"),
        col("APPTEXT").alias("po_line_description"),
        col("AUDT_CREATED_DTTM").alias("created_dttm"),
        col("AUDT_UPDTED_DTTM").alias("updated_dttm"),
        col("ITKEXTRACT_DTTM").alias("itkextract_dttm"),
    )
    .dropDuplicates(["po_line_key"]))
write_silver(fact_po_line, "fact_po_line")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Smoke check - print row counts for each output
for t in ["dim_vendor", "dim_manufacturer", "dim_item", "dim_vendor_part",
          "fact_po_header", "fact_po_line"]:
    n = spark.table(f"{SILVER}.{t}").count()
    print(f"{t:<22}  {n:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
