from pyspark.sql import SparkSession

class SparkSessionWrapper:
    def __init__(self, app_name="cocobambu-pipeline", master="local[*]", jar_path=r"C:\Projetos\postgresql-42.7.4.jar"):
        # Inicializa o SparkSession com o caminho para o jar do PostgreSQL
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars", jar_path) \
            .master(master) \
            .getOrCreate()
        
    def get_spark_session(self):
        """Retorna a instância ativa do SparkSession."""
        return self.spark
        
    def stop(self):
        """Parar a instância do SparkSession."""
        self.spark.stop()