# Case Técnico Data Architect - iFood Challenge

Este repositório contém a solução para o desafio técnico de Arquitetura de Dados proposto pelo iFood. O objetivo é demonstrar habilidades em engenharia, análise e modelagem de dados através da criação de um pipeline para ingestão e processamento de dados de corridas de táxi de Nova York.

## Arquitetura da Solução

A solução foi desenvolvida utilizando **Databricks** e segue uma arquitetura **Medalhão (Bronze, Silver, Gold)**, que promove a governança e a qualidade dos dados em camadas.

1.  **Landing Zone (Amazon S3):** Os arquivos brutos `.parquet` dos meses de Janeiro a Maio de 2023 são baixados e armazenados no caminho `s3://datalake-ifood-case/landing/`.
2.  **Camada Bronze (Tabela Delta no S3):** Os dados brutos são ingeridos da Landing Zone para a tabela Delta `taxi_bronze`, localizada em `s3://datalake-ifood-case/bronze/`. Esta camada serve como um backup versionado e auditável dos dados originais.

3.  **Camada Silver (Tabela Delta no S3):** A partir da camada Bronze, os dados são limpos, transformados e enriquecidos, resultando na tabela `taxi_silver`, localizada em `s3://datalake-ifood-case/silver/`. Esta é a camada principal para consumo e análises. As transformações incluem:
    *   Seleção das colunas de interesse: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `total_amount`.
    *   Conversão de tipos de dados para garantir consistência.
    *   Aplicação de regras de qualidade de dados (ex: `total_amount > 0`) e separação de registros inválidos para uma camada de **Quarentena**.
    *   Criação de colunas derivadas (`pickup_year`, `pickup_month`) para otimizar consultas.
    *   A tabela é particionada por ano e mês para melhorar a performance das queries.

4.  **Quarentena (Tabela Delta no S3):** Registros que falham nas validações de qualidade da camada Silver são movidos para a tabela `taxi_quarantine` em `s3://datalake-ifood-case/quarantine/` para análise posterior.

4.  **Camada Gold (Views/Análises):** As análises de negócio são realizadas diretamente sobre a tabela Silver através de queries SQL, que podem ser salvas como `Views` para facilitar o acesso por usuários finais.

### Justificativas Técnicas

*   **PySpark:** Utilizado como principal ferramenta de ETL por sua capacidade de processamento distribuído e escalável, ideal para grandes volumes de dados.
*   **Databricks:** Escolhido por ser uma plataforma unificada que integra perfeitamente Spark, notebooks, gerenciamento de jobs e um metastore, simplificando o desenvolvimento e a operação do pipeline.
*   **Delta Lake:** Adotado como formato de armazenamento por garantir transações ACID, versionamento de dados (time travel) e por otimizar o desempenho de queries em um Data Lake.
*   **SQL para Análise:** A disponibilização dos dados via tabelas Delta no metastore do Databricks permite que usuários finais (analistas, cientistas de dados) consultem os dados de forma simples e eficiente usando a linguagem SQL padrão, com a qual já estão familiarizados.

## Estrutura do Repositório

O código foi modularizado para promover a reutilização e a clareza, seguindo as melhores práticas de engenharia de software.

```
ifood-case/ 
├─ src/
│ ├─ common/
│ │ └─ spark.py             # Função para criar e configurar a SparkSession
│ ├─ jobs/
│ │ └─ taxi_ingestion.py    # Lógica principal do pipeline de ingestão (Bronze -> Silver)
│ ├─ utils/
│ │ ├─ data_loader.py       # Função para download automático dos dados
│ │ └─ data_quality.py      # Funções para aplicar as regras de qualidade
│ └─ main.py                # Ponto de entrada que orquestra a execução do pipeline 
├─ analysis/
│ └─ queries.sql            # Queries SQL para responder às perguntas de negócio
├─ README.md                # Este arquivo 
└─ requirements.txt         # Dependências do projeto
```

## Instruções de Execução

Siga os passos abaixo para executar a solução no ambiente **Databricks** conectado à sua conta **AWS**.

### 1. Pré-requisitos (Configuração na AWS)

1.  **Conta AWS:** Você precisa de uma conta na AWS com permissões para criar buckets S3 e roles no IAM.
2.  **Bucket S3:** Crie um bucket S3 que servirá como seu Data Lake. Para este projeto, o nome utilizado é `datalake-ifood-case`.
3.  **IAM Role e Instance Profile:** Para que o Databricks possa acessar o S3 de forma segura, crie um **Instance Profile**:
    *   No console da AWS, vá para o **IAM** e crie uma nova **Role**.
    *   Selecione `AWS service` como tipo de entidade confiável e `EC2` como caso de uso.
    *   Anexe a política `AmazonS3FullAccess` (para simplificar) ou uma política customizada que dê permissão de leitura e escrita apenas no bucket `datalake-ifood-case`.
    *   Dê um nome à role (ex: `databricks-s3-access-role`) e crie-a.
    *   O **Instance Profile** é criado automaticamente com o mesmo nome da role.

### 2. Configuração do Ambiente Databricks

1.  **Crie uma conta** no Databricks (pode ser a versão Trial para ter acesso à integração com AWS).
2.  **Crie um Cluster:**
    *   No menu lateral, vá em `Compute`, clique em `Create Cluster`.
    *   Em `Advanced Options` -> `Instance Profile`, selecione o Instance Profile que você criou no passo anterior (`databricks-s3-access-role`).
    *   Finalize a criação do cluster e aguarde ele iniciar.

### 3. Upload do Código

1.  **Importe o repositório:**
    *   No menu lateral do Databricks, vá em `Workspace`.
    *   `Create` -> `Git Folder`
    *   E insira os dados do repositório:
        - https://github.com/brunobta/ifood-case
    *   `Create Git Folder`

### 4. Instalação das Dependências

O Databricks Runtime já inclui as bibliotecas `pyspark` e `delta-spark`. Você só precisa instalar as dependências adicionais listadas no `requirements.txt` (neste caso, `requests`).

1.  No menu lateral, vá em `Compute` e clique no seu cluster.
2.  Vá para a aba **`Libraries`**.
3.  Clique em **`Install new`**.
4.  Para `Library Source`, selecione **`Workspace`**.
5.  No campo `Workspace File Path`, navegue e selecione o arquivo `requirements.txt` que você subiu no passo anterior.
6.  Clique em **`Install`**. O cluster irá instalar as bibliotecas e reiniciar.

### 5. Execução do Pipeline de Ingestão

O pipeline completo, incluindo o download dos dados e a ingestão, é executado a partir de um único ponto de entrada.

1.  No `Workspace`, crie um novo notebook chamado `pipeline_runner`.
2.  Anexe o cluster ativo ao notebook.
3.  Em uma célula do notebook, insira o seguinte comando mágico para executar o script principal:

    ```python
    exec(
    open(
        './src/main.py'
    ).read()
    )
    ```

4.  Execute a célula. O script irá:
    *   Baixar automaticamente os arquivos `.parquet` necessários para a `landing_zone`.
    *   Executar o pipeline de ingestão, criando as tabelas `taxi_bronze` e `taxi_silver`.
    *   Registrar as tabelas no Metastore do Databricks para que possam ser consultadas via SQL.

### 6. Execução das Análises

1.  Crie um novo notebook SQL chamado `analysis_notebook`.
2.  Copie o conteúdo do arquivo `analysis/queries.sql` para células separadas no notebook.
3.  Execute cada célula para obter as respostas para as perguntas do desafio. Os resultados serão exibidos em formato de tabela.

### 6. Criando um Dashboard de Análise Visual

Para apresentar os resultados de forma interativa, você pode criar um dashboard no ambiente **Databricks SQL**. Siga os passos abaixo:

1.  **Mude para o ambiente SQL:**
    *   No menu lateral esquerdo do Databricks, mude a persona de `Data Science & Engineering` para **`SQL`**.

2.  **Crie as Visualizações:**
    *   No menu lateral, vá em **`SQL Editor`**.
    *   **Gráfico 1: Média de Faturamento Mensal:**
        *   Cole a primeira query do arquivo `analysis/queries.sql`. Para uma melhor visualização, podemos formatar a data:
          ```sql
          SELECT
              MAKE_DATE(pickup_year, pickup_month, 1) AS ride_month,
              AVG(total_amount) AS average_total_amount
          FROM taxi_silver
          GROUP BY ride_month
          ORDER BY ride_month;
          ```
        *   Execute a query. Abaixo da tabela de resultados, clique em **`+ Add Visualization`**.
        *   Escolha `Bar` (Barras) ou `Line` (Linha) como tipo de visualização, configure o eixo X para `ride_month` e o eixo Y para `average_total_amount`. Salve a visualização.
    *   **Gráfico 2: Média de Passageiros por Hora:**
        *   Em uma nova aba, cole a segunda query do arquivo `analysis/queries.sql`.
        *   Execute-a e crie uma nova visualização do tipo `Bar`, com o eixo X sendo `pickup_hour` e o eixo Y `average_passenger_count`. Salve a visualização.

3.  **Monte o Dashboard:**
    *   No menu lateral, vá em **`Dashboards`**.
    *   Clique em **`Create Dashboard`**, dê um nome (ex: "Análise de Corridas de Táxi - NY") e crie.
    *   Dentro do dashboard, clique em **`Add`** -> **`Visualization`** e adicione os dois gráficos que você salvou.

4.  **Organize e Publique:**
    *   Arraste e redimensione os gráficos como preferir.
    *   Quando terminar, clique em **`Done Editing`**.
    *   Você pode usar o botão **`Share`** para compartilhar o link do seu dashboard.