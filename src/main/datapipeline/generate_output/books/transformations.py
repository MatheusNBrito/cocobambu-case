from pyspark.sql.functions import col, explode, lit, year, month, dayofmonth, sum
from main.datapipeline.generate_output.books.constants import *

# Função para explodir guestChecks e incluir locRef no contexto
def expand_guest_checks(df):
    return df.select(
        col(LOC_REF).alias(STORE_ID),  # Incluir locRef
        explode(col(GUEST_CHECKS)).alias("guestCheck")
    ).select(
        col(STORE_ID),  # Mantém locRef no contexto
        col("guestCheck.*")
    )

# Função para criar a Tabela Fato: Sales
def create_fact_sales(guest_checks):
    fact_sales = guest_checks.select(
        col(GUEST_CHECK_ID),
        col(STORE_ID),  # locRef já mapeado como storeId
        col(OPEN_BUS_DT).alias("busDt"),
        col(SUB_TOTAL).alias("subTotal"),
        col(DISCOUNT_TOTAL).alias("discountTotal"),
        col(CHECK_TOTAL).alias("checkTotal"),
        col(PAID_TOTAL).alias("paidTotal")
    )

    # Adicionar somatório de taxes.taxCollTtl
    taxes = guest_checks.select(
        col(GUEST_CHECK_ID),
        explode(col(TAXES)).alias("tax")
    ).select(
        col(GUEST_CHECK_ID),
        col("tax." + TAX_COLL_TTL).alias("taxCollected")
    )

    return fact_sales.join(
        taxes.groupBy(GUEST_CHECK_ID).agg(sum("taxCollected").alias("taxCollected")),
        on=GUEST_CHECK_ID,
        how="left"
    )

# Função para criar a Dimensão Date
def create_dim_date(guest_checks):
    return guest_checks.select(
        col(OPEN_BUS_DT).alias("date"),
        year(col(OPEN_BUS_DT)).alias("year"),
        month(col(OPEN_BUS_DT)).alias("month"),
        dayofmonth(col(OPEN_BUS_DT)).alias("day")
    ).distinct()

# Função para criar a Dimensão Store
def create_dim_store(df):
    return df.select(
        col(LOC_REF).alias(STORE_ID),
        lit("Coco Bambu").alias("storeLocation")  # Pode ser ajustado conforme necessário
    ).distinct()

# Função para criar a Dimensão MenuItem
def create_dim_menu_item(guest_checks):
    detail_lines = guest_checks.select(
        col(GUEST_CHECK_ID),
        explode(col(DETAIL_LINES)).alias("detailLine")
    )
    
    return detail_lines.select(
        col("detailLine.menuItem.miNum").alias("menuItemId"),
        col("detailLine.menuItem.prcLvl").alias("priceLevel"),
        col("detailLine.menuItem.activeTaxes").alias("activeTaxes")
    ).distinct()

# Função para criar a Dimensão Taxes
def create_dim_taxes(guest_checks):
    return guest_checks.select(
        col(GUEST_CHECK_ID),
        explode(col(TAXES)).alias("tax")
    ).select(
        col("tax." + TAX_NUM).alias("taxId"),
        col("tax." + TAX_COLL_TTL).alias("taxAmount"),
        col("tax." + TAX_RATE).alias("taxRate")
    ).distinct()
