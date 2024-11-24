from main.datapipeline.generate_output.books.config import load_config

# Carregar configurações do application.conf
config = load_config("C:/Projetos/cocobambu-case/src/main/datapipeline/generate_output/resources/application.conf")

# Definir as constantes para os caminhos de input e output a partir do arquivo de configuração
INPUT_PATH = config["input_paths"]["ERP_PATH"]
OUTPUT_PATH = config["output_paths"]["OUTPUT_PATH"]

# Definir os nomes das colunas como constantes
LOC_REF = "locRef"
GUEST_CHECKS = "guestChecks"
STORE_ID = "storeId"
GUEST_CHECK_ID = "guestCheckId"
OPEN_BUS_DT = "opnBusDt"
SUB_TOTAL = "subTtl"
DISCOUNT_TOTAL = "dscTtl"
CHECK_TOTAL = "chkTtl"
PAID_TOTAL = "payTtl"
TAXES = "taxes"
TAX_COLL_TTL = "taxCollTtl"
TAX_NUM = "taxNum"
TAX_RATE = "taxRate"
DETAIL_LINES = "detailLines"
MENU_ITEM = "menuItem"
