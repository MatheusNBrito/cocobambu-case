import configparser
import os

def load_config(config_path="C:/Projetos/cocobambu-case/src/main/datapipeline/generate_output/resources/application.conf"):
    config = configparser.ConfigParser()
    config.read(config_path)

    # Verificar se as seções estão corretamente carregadas
    if not config.sections():
        raise Exception(f"Erro ao carregar o arquivo de configuração: {config_path}")

    # Debug: Mostrar as seções carregadas
    print("Seções encontradas no arquivo de configuração:", config.sections())

    # Acessando as seções específicas
    if "input_paths" not in config:
        raise KeyError("A seção 'input_paths' não foi encontrada no arquivo de configuração.")
    if "output_paths" not in config:
        raise KeyError("A seção 'output_paths' não foi encontrada no arquivo de configuração.")

    input_paths = config["input_paths"]
    output_paths = config["output_paths"]

    print("Caminho ERP:", input_paths["raw_tables.ERP_PATH"])
    print("Caminho de saída:", output_paths["output_path"])

    return {
        "input_paths": {
            "ERP_PATH": input_paths["raw_tables.ERP_PATH"]
        },
        "output_paths": {
            "OUTPUT_PATH": output_paths["output_path"]
        }
    }


config = load_config()
