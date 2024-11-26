# Escolhendo uma imagem base com Python
FROM python:3.12-slim

# Definindo o diretório de trabalho dentro do container
WORKDIR /app

# Copiando o requirements.txt para o container
COPY requirements.txt .

# Atualizar o sistema e instalar dependências
RUN apt-get update && apt-get install -y curl build-essential

# Instalar o Rust e o Cargo
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Adicionar o Cargo ao PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Instalando as dependências do requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Instalando Java (necessário para o PySpark)
RUN apt-get update && apt-get install -y openjdk-17-jre

# Definindo variáveis de ambiente para Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Instalando o Spark (ajuste o caminho conforme necessário)
RUN curl -sL https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz | tar -xz -C /opt/
ENV SPARK_HOME=/opt/spark-3.4.3-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Instalando o Hadoop (ajuste o caminho conforme necessário)
RUN curl -sL https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz | tar -xz -C /opt/
ENV HADOOP_HOME=/opt/hadoop-3.4.0
ENV PATH=$HADOOP_HOME/bin:$PATH

# Configurando o Airflow
ENV AIRFLOW_HOME=/app/airflow
RUN pip install apache-airflow[postgres,google,azure,elasticsearch]

# Copiando os arquivos do seu projeto para o container
COPY . .

# Expondo a porta do Airflow Web Server
EXPOSE 8080

# Inicializando o Airflow (ajuste o comando conforme a sua necessidade)
CMD ["airflow", "webserver"]
