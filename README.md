README - Desafio Engenharia de Dados Coco Bambu
Este repositório contém a solução para o Desafio de Engenharia de Dados Coco Bambu, focado no processamento de dados de restaurantes. O objetivo foi transformar e armazenar dados de um arquivo JSON de ERP e de múltiplos endpoints de API em um Data Lake e banco de dados PostgreSQL, utilizando um modelo de dados Star Schema.

Estrutura do Projeto
main.py: Controla o fluxo principal do pipeline, carregando dados, realizando transformações e salvando no banco de dados e Data Lake.
transformations.py: Define funções para criar a tabela fato Sales e dimensões como Date, Store, MenuItem e Taxes.
data_loading.py: Contém funções para carregar e validar dados JSON.
config.py: Carrega as configurações do arquivo application.conf (entradas, saídas e banco de dados).
constants.py: Define constantes como nomes de colunas e caminhos usados no projeto.
Escolha do Modelo Star Schema
Optou-se pelo Star Schema devido à sua simplicidade e eficiência para consultas analíticas. Nesse modelo, temos:

Tabela Fato (Sales): Contém dados transacionais (ex: subtotal, tax_collected, check_total).
Tabelas de Dimensões: Representam entidades como data, loja, itens de menu e impostos.
A tabela fato se conecta diretamente às dimensões por chaves estrangeiras (ex: store_id, date_id).
Facilita consultas rápidas e agregações de dados, ideais para análise de receitas e métricas de desempenho.

Como Executar
Instalar dependências:

bash
pip install pyspark

Executar o pipeline:

bash
python -m main.datapipeline.generate_output.books.main

O pipeline processa os dados, cria as tabelas de fato e dimensões e as salva tanto no banco de dados PostgreSQL quanto no Data Lake.