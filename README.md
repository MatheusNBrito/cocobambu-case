#  Desafio Engenharia de Dados - Coco Bambu  

Este repositório apresenta a solução para o **Desafio de Engenharia de Dados** da Coco Bambu, voltado para o processamento e modelagem de dados do setor de restaurantes.  

##  Objetivo  
Transformar e armazenar dados provenientes de:
- Um arquivo JSON do sistema ERP.

Os dados serão processados e organizados no formato **Star Schema**, com tabelas de fato e dimensões.  

##  Solução  
O pipeline executará as seguintes etapas:  
1. **Processamento de Dados:**  
   - Extração dos dados JSON 
   - Transformações para atender ao modelo de dados Star Schema.  

2. **Armazenamento:**  
   - Tabelas **de fato e dimensões** salvas no PostgreSQL.  
   - Dados em formato **Parquet**, particionados por `store_id` e `date`, armazenados no Data Lake.  

## 📑 Documentação Completa  
Para mais detalhes, confira a documentação completa do projeto:  
[Documentação do Desafio de Engenharia de Dados](https://pointed-growth-de1.notion.site/Documenta-o-Completa-do-Projeto-Desafio-Engenharia-de-Dados-Coco-Bambu-14a325ce837280a0b500c40515bfc4fd)  