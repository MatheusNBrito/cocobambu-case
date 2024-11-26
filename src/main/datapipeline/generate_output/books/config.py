import configparser

def load_config(config_path="C:/Projetos/cocobambu-case/src/main/datapipeline/generate_output/resources/application.conf"):
    config = configparser.ConfigParser()
    config.read(config_path)

    # Verificar se as seções estão corretamente carregadas
    if not config.sections():
        raise Exception(f"Erro ao carregar o arquivo de configuração: {config_path}")

    # Debug: Mostrar as seções carregadas
    print("Seções encontradas no arquivo de configuração:", config.sections())

    # Verificar a existência das seções necessárias
    if "input_paths" not in config:
        raise KeyError("A seção 'input_paths' não foi encontrada no arquivo de configuração.")
    if "output_paths" not in config:
        raise KeyError("A seção 'output_paths' não foi encontrada no arquivo de configuração.")

    # Recuperar as seções
    input_paths = config["input_paths"]
    output_paths = config["output_paths"]

    # Verificar a existência das chaves necessárias
    if "raw_tables.ERP_PATH" not in input_paths:
        raise KeyError("A chave 'raw_tables.ERP_PATH' não foi encontrada na seção 'input_paths'.")
    if "lake_base_path" not in output_paths:
        raise KeyError("A chave 'lake_base_path' não foi encontrada na seção 'output_paths'.")

    # Debug: Mostrar os caminhos carregados
    print("Caminho ERP:", input_paths["raw_tables.ERP_PATH"])
    print("Caminho do Data Lake:", output_paths["lake_base_path"])

    # Retornar as configurações carregadas
    return {
        "input_paths": {
            "ERP_PATH": input_paths["raw_tables.ERP_PATH"]
        },
        "output_paths": {
            "LAKE_BASE_PATH": output_paths["lake_base_path"]
        }
    }

# Testar o carregamento de configuração
if __name__ == "__main__":
    config = load_config()
    print("Configuração carregada com sucesso:", config)
