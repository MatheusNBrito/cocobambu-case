from pyspark.sql.functions import col, explode, lit, year, month, dayofmonth, sum, date_format, to_date
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
    # Seleciona colunas principais para fact_sales
    fact_sales = guest_checks.select(
        col(GUEST_CHECK_ID).alias("sales_id"),
        col(STORE_ID).alias("store_id"),
        col(OPEN_BUS_DT).alias("bus_dt"),
        col(SUB_TOTAL).alias("subtotal"),
        col(DISCOUNT_TOTAL).alias("discount_total"),
        col(CHECK_TOTAL).alias("check_total"),
        col(PAID_TOTAL).alias("paid_total")
    )

    # Verifica o esquema inicial
    print("Schema de fact_sales:")
    fact_sales.printSchema()

    # Extrai e agrega as taxas
    taxes = guest_checks.select(
        col(GUEST_CHECK_ID).alias("sales_id"),
        explode(col(TAXES)).alias("tax")
    ).select(
        col("sales_id"),
        col("tax." + TAX_COLL_TTL).alias("tax_collected")
    )

    print("Schema de taxes:")
    taxes.printSchema()

    # Realiza o join entre fact_sales e as taxas agregadas
    try:
        fact_sales = fact_sales.join(
            taxes.groupBy("sales_id").agg(sum("tax_collected").alias("tax_collected")),
            on="sales_id",
            how="left"
        )
        print("Schema de fact_sales após o join:")
        fact_sales.printSchema()
    except Exception as e:
        print(f"Erro ao realizar o join em fact_sales: {e}")
        return None

    # Adiciona colunas ausentes
    try:
        fact_sales = fact_sales.withColumn("guest_count", lit(None).cast("int")) \
            .withColumn("table_name", lit(None).cast("string")) \
            .withColumn("server_id", lit(None).cast("long")) \
            .withColumn("employee_id", lit(None).cast("long")) \
            .withColumn("opened_date", lit(None).cast("timestamp"))
    except Exception as e:
        print(f"Erro ao adicionar colunas em fact_sales: {e}")
        return None

    return fact_sales

# Função para criar a Dimensão Date
def create_dim_date(guest_checks):
    return guest_checks.select(
        col(OPEN_BUS_DT).alias("date"),
        year(col(OPEN_BUS_DT)).alias("year"),
        month(col(OPEN_BUS_DT)).alias("month"),
        dayofmonth(col(OPEN_BUS_DT)).alias("day"),
        date_format(col(OPEN_BUS_DT), "EEEE").alias("weekday")
    ).distinct()

# Função para criar a Dimensão Store
def create_dim_store(df):
    return df.select(
        col(LOC_REF).alias("storeId")
    ).distinct().withColumn("storeLocation", lit("Default Location"))

# Função para criar a Dimensão MenuItem
def create_dim_menu_item(guest_checks):
    detail_lines = guest_checks.select(
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
        col(GUEST_CHECK_ID).alias("guestCheckId"),
        explode(col(TAXES)).alias("tax")
    ).select(
        col(f"tax.{TAX_NUM}").alias("taxId"),
        col(f"tax.{TAX_COLL_TTL}").alias("taxAmount"),
        col(f"tax.{TAX_RATE}").alias("taxRate")
    ).distinct()

# Função para salvar no banco de dados
def save_to_database(dataframe, table_name, db_url, db_properties):
    # Renomear colunas do DataFrame para corresponder ao esquema do banco de dados
    column_mapping = {
        "sales_id": "guestcheckid",
        "store_id": "storeid",
        "bus_dt": "busdt",
        "subtotal": "subtotal",
        "discount_total": "discounttotal",
        "check_total": "checktotal",
        "paid_total": "paidtotal",
        "tax_collected": "taxcollected",
        "guest_count": "guestcount",
        "table_name": "tablename",
        "server_id": "serverid",
        "employee_id": "employeeid",
        "opened_date": "openeddate"
    }

    # Aplicar renomeação
    for spark_col, db_col in column_mapping.items():
        if spark_col in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(spark_col, db_col)

    # Salvar no banco
    dataframe.write.jdbc(
        url=db_url,
        table=table_name,
        mode="append",
        properties=db_properties
    )
