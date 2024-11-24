from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, sum, year, month, dayofmonth
from pyspark.sql.types import *

import sys
sys.path.append("c:/Projetos/cocobambu-case")

from main.datapipeline.generate_output.books.config import load_config
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
    col("locRef").alias("storeId"),  # Incluir locRef
    explode(col("guestChecks")).alias("guestCheck")
)

# Expandir guestChecks mantendo locRef
guest_checks = guest_checks.select(
    col("storeId"),  # Mantém locRef no contexto
    col("guestCheck.*")
)

# Criar Tabela Fato: Sales
fact_sales = guest_checks.select(
    col("guestCheckId"),
    col("storeId"),  # locRef já mapeado como storeId
    col("opnBusDt").alias("busDt"),
    col("subTtl").alias("subTotal"),
    col("dscTtl").alias("discountTotal"),
    col("chkTtl").alias("checkTotal"),
    col("payTtl").alias("paidTotal")
)

# Adicionar somatório de taxes.taxCollTtl
taxes = guest_checks.select(
    col("guestCheckId"),
    explode(col("taxes")).alias("tax")
).select(
    col("guestCheckId"),
    col("tax.taxCollTtl").alias("taxCollected")
)

fact_sales = fact_sales.join(
    taxes.groupBy("guestCheckId").agg(sum("taxCollected").alias("taxCollected")),
    on="guestCheckId",
    how="left"
)

# Criar Dimensão Date
dim_date = guest_checks.select(
    col("opnBusDt").alias("date"),
    year(col("opnBusDt")).alias("year"),
    month(col("opnBusDt")).alias("month"),
    dayofmonth(col("opnBusDt")).alias("day")
).distinct()

# Criar Dimensão Store
dim_store = erp_raw_df.select(
    col("locRef").alias("storeId"),
    lit("Coco Bambu").alias("storeLocation")  # Pode ser ajustado conforme necessário
).distinct()

# Criar Dimensão MenuItem
detail_lines = guest_checks.select(
    col("guestCheckId"),
    explode(col("detailLines")).alias("detailLine")
)

dim_menu_item = detail_lines.select(
    col("detailLine.menuItem.miNum").alias("menuItemId"),
    col("detailLine.menuItem.prcLvl").alias("priceLevel"),
    col("detailLine.menuItem.activeTaxes").alias("activeTaxes")
).distinct()

# Criar Dimensão Taxes
dim_taxes = guest_checks.select(
    col("guestCheckId"),
    explode(col("taxes")).alias("tax")
).select(
    col("tax.taxNum").alias("taxId"),
    col("tax.taxCollTtl").alias("taxAmount"),
    col("tax.taxRate").alias("taxRate")
).distinct()


fact_sales.write.parquet(f"{output_path}/FactSales", mode="overwrite")
dim_date.write.parquet(f"{output_path}/DimDate", mode="overwrite")
dim_store.write.parquet(f"{output_path}/DimStore", mode="overwrite")
dim_menu_item.write.parquet(f"{output_path}/DimMenuItem", mode="overwrite")
dim_taxes.write.parquet(f"{output_path}/DimTaxes", mode="overwrite")

print("Pipeline concluído com sucesso!")
