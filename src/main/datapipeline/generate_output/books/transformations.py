from pyspark.sql.functions import col, explode, lit, year, month, dayofmonth, sum, date_format, to_date
from main.datapipeline.generate_output.books.constants import *

# Função para explodir guestChecks e incluir locRef no contexto
def expand_guest_checks(df):
    return df.select(
        col(LOC_REF).alias(STORE_ID),  # Usando constante LOC_REF
        explode(col(GUEST_CHECKS)).alias("guestCheck")  # Usando constante GUEST_CHECKS
    ).select(
        col(STORE_ID),  # Usando constante STORE_ID
        col("guestCheck.*")
    )

# Função para criar a Tabela Fato: Sales
def create_fact_sales(guest_checks):
    fact_sales = guest_checks.select(
        col(GUEST_CHECK_ID).alias("sales_id"),
        col(STORE_ID).alias("store_id"),
        col(OPEN_BUS_DT).alias("bus_dt"),
        col(SUB_TOTAL).alias("subtotal"),
        col(DISCOUNT_TOTAL).alias("discount_total"),
        col(CHECK_TOTAL).alias("check_total"),
        col(PAID_TOTAL).alias("paid_total")
    )

    # Extrai e agrega as taxas
    taxes = guest_checks.select(
        col(GUEST_CHECK_ID).alias("sales_id"),
        explode(col(TAXES)).alias("tax")  # Usando constante TAXES
    ).select(
        col("sales_id"),
        col("tax." + TAX_COLL_TTL).alias("tax_collected")  # Usando constante TAX_COLL_TTL
    )

    # Realiza o join entre fact_sales e as taxas agregadas
    try:
        fact_sales = fact_sales.join(
            taxes.groupBy("sales_id").agg(sum("tax_collected").alias("tax_collected")),
            on="sales_id",
            how="left"
        )
    except Exception as e:
        print(f"Erro ao realizar o join em fact_sales: {e}")
        return None

    # Adiciona colunas ausentes com valores default
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
def create_dim_store(df):
    # Adiciona 'store_id' ao DataFrame
    df = df.withColumn("store_id", col("locRef"))  # Renomeia 'locRef' para 'store_id'
    
    return df.select(
        col("store_id"),  # Usando o nome consistente 'store_id'
        lit("Default Location").alias("storeLocation")
    ).distinct()

# Função para criar a Dimensão Date
def create_dim_date(guest_checks):
    return guest_checks.select(
        col(OPEN_BUS_DT).alias("date"),
        year(col(OPEN_BUS_DT)).alias("year"),
        month(col(OPEN_BUS_DT)).alias("month"),
        dayofmonth(col(OPEN_BUS_DT)).alias("day"),
        date_format(col(OPEN_BUS_DT), "EEEE").alias("weekday")
    ).distinct()

# Função para criar a Dimensão MenuItem
def create_dim_menu_item(guest_checks):
    detail_lines = guest_checks.select(
        explode(col(DETAIL_LINES)).alias("detailLine")  # Usando constante DETAIL_LINES
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
        explode(col(TAXES)).alias("tax")  # Usando constante TAXES
    ).select(
        col(f"tax.{TAX_NUM}").alias("taxId"),  # Usando constante TAX_NUM
        col(f"tax.{TAX_COLL_TTL}").alias("taxAmount"),  # Usando constante TAX_COLL_TTL
        col(f"tax.{TAX_RATE}").alias("taxRate")  # Usando constante TAX_RATE
    ).distinct()

def add_partition_columns(dataframe, date_column, store_column=None):
    dataframe = dataframe.withColumn("year", year(col(date_column))) \
                         .withColumn("month", month(col(date_column)))
    if store_column:
        if store_column not in dataframe.columns:
            raise ValueError(f"A coluna {store_column} não existe no DataFrame. Colunas disponíveis: {dataframe.columns}")
        dataframe = dataframe.withColumn(store_column, col(store_column))
    return dataframe

def save_to_data_lake(dataframe, base_path, store_column=None, date_column=None):
    try:
        # Adicionar colunas de particionamento apenas se especificadas
        if date_column:
            dataframe = dataframe.withColumn("year", year(col(date_column))) \
                                 .withColumn("month", month(col(date_column)))

        if store_column:
            if store_column not in dataframe.columns:
                raise ValueError(f"A coluna {store_column} não existe no DataFrame. Colunas disponíveis: {dataframe.columns}")
            dataframe = dataframe.withColumn(store_column, col(store_column))
        
        # Configurar particionamento dinamicamente
        partition_cols = []
        if store_column:
            partition_cols.append(store_column)
        if date_column:
            partition_cols.extend(["year", "month"])

        writer = dataframe.write.mode("overwrite").format("parquet")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(base_path)
        print(f"Dados salvos no Data Lake em {base_path}")
    except Exception as e:
        print(f"Erro ao salvar dados no Data Lake: {e}")

# Função para salvar a tabela fato Sales no Data Lake
def save_fact_sales(fact_sales, lake_base_path):
    save_to_data_lake(
        fact_sales,
        f"{lake_base_path}/fact_sales",
        store_column="store_id",
        date_column="bus_dt"
    )

# Função para salvar a dimensão Date no Data Lake
def save_dim_date(dim_date, lake_base_path):
    save_to_data_lake(
        dim_date,
        f"{lake_base_path}/dim_date",
        store_column=None,  # Não particiona por loja
        date_column="date"  # Particiona por ano e mês
    )

# Função para salvar a dimensão Store no Data Lake
def save_dim_store(dim_store, lake_base_path):
    save_to_data_lake(
        dim_store,
        f"{lake_base_path}/dim_store",
        store_column=None,  # Sem particionamento por loja
        date_column=None    # Sem particionamento por data
    )

# Função para salvar a dimensão MenuItem no Data Lake
def save_dim_menu_item(dim_menu_item, lake_base_path):
    save_to_data_lake(
        dim_menu_item,
        f"{lake_base_path}/dim_menu_item",
        store_column=None,  # Sem particionamento por loja
        date_column=None    # Sem particionamento por data
    )

# Função para salvar a dimensão Taxes no Data Lake
def save_dim_taxes(dim_taxes, lake_base_path):
    save_to_data_lake(
        dim_taxes,
        f"{lake_base_path}/dim_taxes",
        store_column=None,  # Sem particionamento por loja
        date_column=None    # Sem particionamento por data
    )

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