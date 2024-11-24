from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, sum, year, month, dayofmonth
from pyspark.sql.types import *
import sys
sys.path.append("c:/Projetos/cocobambu-case")

from main.datapipeline.generate_output.books.config import load_config
from main.datapipeline.generate_output.books import constants
import json

# Carregar configurações
config = load_config()
input_path = config["input_paths"]["ERP_PATH"]
output_path = config["output_paths"]["OUTPUT_PATH"]

# Inicializar SparkSession
spark = SparkSession.builder.appName("starSchemaTables").getOrCreate()

# Verificar se o JSON é válido
with open(input_path, "r") as f:
    try:
        data = json.load(f)
        print("JSON carregado com sucesso!")
    except json.JSONDecodeError as e:
        print("Erro ao carregar JSON:", e)

# Carregar o arquivo JSON como DataFrame do Spark
erp_raw_df = spark.read \
    .option("multiLine", "true") \
    .json(input_path)

# Explodir guestChecks e incluir locRef no contexto
guest_checks = erp_raw_df.select(
    col(constants.LOC_REF).alias(constants.STORE_ID),  # Incluir locRef
    explode(col(constants.GUEST_CHECKS)).alias("guestCheck")
)

# Expandir guestChecks mantendo locRef
guest_checks = guest_checks.select(
    col(constants.STORE_ID),  # Mantém locRef no contexto
    col("guestCheck.*")
)

# Criar Tabela Fato: Sales
fact_sales = guest_checks.select(
    col(constants.GUEST_CHECK_ID),
    col(constants.STORE_ID),  # locRef já mapeado como storeId
    col(constants.OPEN_BUS_DT).alias("busDt"),
    col(constants.SUB_TOTAL).alias("subTotal"),
    col(constants.DISCOUNT_TOTAL).alias("discountTotal"),
    col(constants.CHECK_TOTAL).alias("checkTotal"),
    col(constants.PAID_TOTAL).alias("paidTotal")
)

# Adicionar somatório de taxes.taxCollTtl
taxes = guest_checks.select(
    col(constants.GUEST_CHECK_ID),
    explode(col(constants.TAXES)).alias("tax")
).select(
    col(constants.GUEST_CHECK_ID),
    col("tax." + constants.TAX_COLL_TTL).alias("taxCollected")
)

fact_sales = fact_sales.join(
    taxes.groupBy(constants.GUEST_CHECK_ID).agg(sum("taxCollected").alias("taxCollected")),
    on=constants.GUEST_CHECK_ID,
    how="left"
)

# Criar Dimensão Date
dim_date = guest_checks.select(
    col(constants.OPEN_BUS_DT).alias("date"),
    year(col(constants.OPEN_BUS_DT)).alias("year"),
    month(col(constants.OPEN_BUS_DT)).alias("month"),
    dayofmonth(col(constants.OPEN_BUS_DT)).alias("day")
).distinct()

# Criar Dimensão Store
dim_store = erp_raw_df.select(
    col(constants.LOC_REF).alias(constants.STORE_ID),
    lit("Coco Bambu").alias("storeLocation")  # Pode ser ajustado conforme necessário
).distinct()

# Criar Dimensão MenuItem
detail_lines = guest_checks.select(
    col(constants.GUEST_CHECK_ID),
    explode(col(constants.DETAIL_LINES)).alias("detailLine")
)

dim_menu_item = detail_lines.select(
    col("detailLine.menuItem.miNum").alias("menuItemId"),
    col("detailLine.menuItem.prcLvl").alias("priceLevel"),
    col("detailLine.menuItem.activeTaxes").alias("activeTaxes")
).distinct()

# Criar Dimensão Taxes
dim_taxes = guest_checks.select(
    col(constants.GUEST_CHECK_ID),
    explode(col(constants.TAXES)).alias("tax")
).select(
    col("tax." + constants.TAX_NUM).alias("taxId"),
    col("tax." + constants.TAX_COLL_TTL).alias("taxAmount"),
    col("tax." + constants.TAX_RATE).alias("taxRate")
).distinct()

# Salvar os resultados em Parquet
fact_sales.write.parquet(f"{constants.OUTPUT_PATH}/FactSales", mode="overwrite")
dim_date.write.parquet(f"{constants.OUTPUT_PATH}/DimDate", mode="overwrite")
dim_store.write.parquet(f"{constants.OUTPUT_PATH}/DimStore", mode="overwrite")
dim_menu_item.write.parquet(f"{constants.OUTPUT_PATH}/DimMenuItem", mode="overwrite")
dim_taxes.write.parquet(f"{constants.OUTPUT_PATH}/DimTaxes", mode="overwrite")

print("Pipeline concluído com sucesso!")