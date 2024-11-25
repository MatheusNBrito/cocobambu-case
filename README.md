Desafio de Engenharia de Dados - Coco Bambu

Descrição
A tarefa consistiu em processar o arquivo ERP.json, que contém informações sobre pedidos feitos no restaurante, e criar uma série de tabelas no modelo dimensional Star Schema. A solução gerou as seguintes tabelas:

Tabela de Fato: Sales
Contém as métricas principais dos pedidos, como total de vendas, descontos, impostos e valores pagos.
Tabelas Dimensionais
DimDate: Informações sobre as datas dos pedidos.
DimStore: Dados sobre a loja em que os pedidos foram feitos.
DimMenuItem: Detalhes sobre os itens do menu pedidos.
DimTaxes: Informações detalhadas sobre os impostos aplicados.

Como Executar
Pré-requisitos
Para executar o código, é necessário ter o Apache Spark e o PySpark instalados. É possível instalar o PySpark via pip:

bash
Copiar código
pip install pyspark

Definição do input_path
No início do processo, o arquivo de entrada ERP.json foi carregado através da variável input_path. O input_path é a definição do caminho do arquivo que contém os dados originais, que pode ser local ou em um diretório específico no ambiente em que o código for executado.

O valor do input_path foi definido da seguinte forma:

python
Copiar código
input_path = "data/ERP.json"
Isso indica que o arquivo ERP.json está localizado no diretório /data do projeto. Se necessário, o caminho pode ser alterado para apontar para um arquivo diferente ou uma URL de um arquivo hospedado remotamente.

Execução do Pipeline
Para rodar o pipeline e gerar as tabelas, basta executar o script principal. Ele irá processar o arquivo ERP.json, gerar as tabelas e salvar os resultados no diretório /output como arquivos Parquet.

Após a execução, os arquivos Parquet estarão disponíveis para consulta.

Transformações Realizadas
Durante o processamento do arquivo, apliquei uma série de transformações para transformar os dados em um formato adequado para o modelo Star Schema. Aqui estão as principais transformações realizadas:

Explodir a Estrutura de guestChecks: O arquivo JSON contém um campo chamado guestChecks, que é uma lista de objetos representando pedidos realizados no restaurante. Cada pedido pode ter múltiplas linhas de detalhes, como itens do menu, valores, impostos, etc. Utilizei o comando explode do PySpark para "desempacotar" cada item da lista guestChecks em registros individuais, mantendo os dados estruturados.

Criação das Tabelas Dimensionais: Com os dados descompactados, criei as tabelas dimensionais, que representam as entidades do modelo Star Schema:

DimDate: Contém as datas associadas a cada pedido. Extraí o ano, mês e dia do campo opnBusDt (data de abertura do pedido).
DimStore: Armazena informações sobre a loja. Extraí o campo locRef (referência da loja) do JSON.
DimMenuItem: Contém informações sobre os itens do menu solicitados nos pedidos. Utilizei o campo menuItem dentro de cada detailLines para preencher essa tabela.
DimTaxes: Armazena informações sobre os impostos aplicados a cada pedido. Cada item de taxes foi extraído e mapeado para a tabela de impostos.
Criação da Tabela de Fato (Sales): A tabela de fato foi criada para centralizar as métricas dos pedidos, como o total do pedido, descontos, impostos e valores pagos. A partir dos dados de guestChecks, extraí o total de cada pedido, o total de descontos e impostos, e os valores pagos.

Armazenamento dos Resultados: Todas as tabelas (tanto as dimensionais quanto a tabela de fato) foram salvas no formato Parquet, um formato eficiente para o armazenamento de grandes volumes de dados. Essas tabelas agora podem ser carregadas facilmente em um ambiente de BI para análise posterior.

Resultados Gerados
O pipeline cria as seguintes tabelas:

Sales (Fato): Contém os totais dos pedidos, incluindo subtotais, descontos e impostos.
DimDate: Fornece informações sobre a data de cada pedido.
DimStore: Contém os dados da loja associada aos pedidos.
DimMenuItem: Detalha os itens do cardápio relacionados aos pedidos.
DimTaxes: Fornece informações detalhadas sobre os impostos aplicados.
Essas tabelas foram organizadas em formato Parquet no diretório output/, prontas para serem utilizadas em análises.

Decisões de Design
O modelo Star Schema foi escolhido para organizar os dados de forma eficiente para consultas de BI. A tabela de fato centraliza as métricas dos pedidos, enquanto as tabelas dimensionais fornecem informações adicionais necessárias para análise detalhada, como dados de data, loja, itens de menu e impostos.