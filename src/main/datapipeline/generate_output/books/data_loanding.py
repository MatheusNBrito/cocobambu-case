import json

# Função para verificar se o JSON é válido
def validate_json(input_path):
    with open(input_path, "r") as f:
        try:
            data = json.load(f)
            print("JSON carregado com sucesso!")
            return True
        except json.JSONDecodeError as e:
            print("Erro ao carregar JSON:", e)
            return False

# Função para carregar o arquivo JSON como DataFrame do Spark
def load_json_as_dataframe(spark, input_path):
    if validate_json(input_path):
        return spark.read.option("multiLine", "true").json(input_path)
    else:
        raise ValueError("Falha ao validar o arquivo JSON.")
