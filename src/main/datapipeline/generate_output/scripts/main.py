from pyspark.sql import SparkSession
from pyspark.sql.types import *
from main.datapipeline.generate_output.books.config import load_config
from main.datapipeline.generate_output.books import constants
from main.datapipeline.generate_output.books.data_loanding import *
from main.datapipeline.generate_output.books.transformations import (
    expand_guest_checks,
    create_fact_sales,
    create_dim_date,
    create_dim_store,
    create_dim_menu_item,
    create_dim_taxes,
    save_to_database,
)

# Carregar configurações
config = load_config()
input_path = config["input_paths"]["ERP_PATH"]

# Configurações do banco de dados PostgreSQL
db_url = "jdbc:postgresql://localhost:5432/cocobambu_case" 
db_properties = {
    "user": "postgres",  
    "password": "1475963",  
    "driver": "org.postgresql.Driver",
    "cascadeTruncate": "true" 
}

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("starSchemaTables") \
    .config("spark.jars", "C:\Projetos\postgresql-42.7.4.jar") \
    .getOrCreate()

# Carregar os dados JSON como DataFrame
erp_raw_df = load_json_as_dataframe(spark, input_path)

# Explodir guestChecks e incluir locRef no contexto
guest_checks = expand_guest_checks(erp_raw_df)
guest_checks.show(truncate=False)
guest_checks.printSchema()
# Criar Tabela Fato: Sales
fact_sales = create_fact_sales(guest_checks)
if fact_sales is None:
    raise ValueError("Erro: 'fact_sales' retornou None.")
fact_sales.show()
print("schema fact_sales:")
fact_sales.printSchema()

# Criar Dimensões
dim_date = create_dim_date(guest_checks)
if dim_date is None:
    raise ValueError("Erro: 'dim_date' retornou None.")
dim_date.show()
print("schema dim_date:")
dim_date.printSchema()


dim_store = create_dim_store(erp_raw_df)
if dim_store is None:
    raise ValueError("Erro: 'dim_store' retornou None.")
dim_store.show()
print("schema dim_store:")
dim_store.printSchema()

dim_menu_item = create_dim_menu_item(guest_checks)
if dim_menu_item is None:
    raise ValueError("Erro: 'dim_menu_item' retornou None.")
dim_menu_item.show()
print("schema dim_menu_item:")
dim_menu_item.printSchema()

dim_taxes = create_dim_taxes(guest_checks)
if dim_taxes is None:
    raise ValueError("Erro: 'dim_taxes' retornou None.")
dim_taxes.show()
print("schema dim_taxes:")
dim_taxes.printSchema()



try:
    save_to_database(fact_sales, "FactSales", db_url, db_properties)
    save_to_database(dim_date, "DimDate", db_url, db_properties)
    save_to_database(dim_store, "DimStore", db_url, db_properties)
    save_to_database(dim_menu_item, "DimMenuItem", db_url, db_properties)
    save_to_database(dim_taxes, "DimTaxes", db_url, db_properties)
    print("Tabelas salvas no banco de dados com sucesso!")
except Exception as e:
    print(f"Erro ao salvar dados no banco de dados: {e}")


print("Pipeline concluído com sucesso! Tabelas salvas no banco de dados.")