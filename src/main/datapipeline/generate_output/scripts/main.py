from pyspark.sql import SparkSession
from pyspark.sql.types import *
from main.datapipeline.generate_output.books.config import load_config
from main.datapipeline.generate_output.books import constants
from main.datapipeline.generate_output.books.data_loanding import *
from main.datapipeline.generate_output.books.transformations import expand_guest_checks, create_fact_sales, create_dim_date, create_dim_store, create_dim_menu_item, create_dim_taxes

# Carregar configurações
config = load_config()
input_path = config["input_paths"]["ERP_PATH"]
output_path = config["output_paths"]["OUTPUT_PATH"]

# Inicializar SparkSession
spark = SparkSession.builder.appName("starSchemaTables").getOrCreate()

# Carregar os dados JSON como DataFrame
erp_raw_df = load_json_as_dataframe(spark, input_path)

# Explodir guestChecks e incluir locRef no contexto
guest_checks = expand_guest_checks(erp_raw_df)

# Criar Tabela Fato: Sales
fact_sales = create_fact_sales(guest_checks)
# Expandir guestChecks mantendo locRef

# Criar Dimensões
dim_date = create_dim_date(guest_checks)
dim_store = create_dim_store(erp_raw_df)
dim_menu_item = create_dim_menu_item(guest_checks)
dim_taxes = create_dim_taxes(guest_checks)

# Salvar os resultados em Parquet
fact_sales.write.parquet(f"{constants.OUTPUT_PATH}/FactSales", mode="overwrite")
dim_date.write.parquet(f"{constants.OUTPUT_PATH}/DimDate", mode="overwrite")
dim_store.write.parquet(f"{constants.OUTPUT_PATH}/DimStore", mode="overwrite")
dim_menu_item.write.parquet(f"{constants.OUTPUT_PATH}/DimMenuItem", mode="overwrite")
dim_taxes.write.parquet(f"{constants.OUTPUT_PATH}/DimTaxes", mode="overwrite")

print("Pipeline concluído com sucesso!")