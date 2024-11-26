import configparser

def load_config(config_path="C:/Projetos/cocobambu-case/src/main/datapipeline/generate_output/resources/application.conf"):
    config = configparser.ConfigParser()
    config.read(config_path)

    if not config.sections():
        raise Exception(f"Erro ao carregar o arquivo de configuração: {config_path}")

    # Recuperar as configurações
    input_paths = config["input_paths"]
    output_paths = config["output_paths"]
    database = config["database"]

    return {
        "input_paths": {
            "ERP_PATH": input_paths.get("raw_tables.ERP_PATH")
        },
        "output_paths": {
            "LAKE_BASE_PATH": output_paths.get("lake_base_path")
        },
        "database": {
            "url": database.get("url"),
            "user": database.get("user"),
            "password": database.get("password"),
            "driver": database.get("driver"),
            "cascade_truncate": database.getboolean("cascade_truncate")
        }
    }
