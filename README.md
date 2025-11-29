# Desafio Técnico - Data Engineering

Projeto com 3 questões práticas de engenharia de dados envolvendo ETL, orquestração com Airflow e validação de dados no BigQuery.

## Estrutura do Projeto

```
.
├── questao_1/
│   ├── etl_users.py
│   ├── indicadores.sql
│   └── requirements.txt
├── questao_2/
│   └── dag_regua_cobranca.py
├── questao_3/
│   ├── validacao_aplicacoes.sql
│   ├── validacao_local.py
│   ├── requirements.txt
│   └── dados/
│       ├── application_record_local.csv
│       └── application_record_gcp.csv
├── .gitignore
└── README.md
```

---

## Questão 1 - ETL de Usuários

### O que faz

Script Python que consome a API pública DummyJSON, extrai dados de usuários e carrega em um banco SQLite local com estrutura relacional normalizada.

Após a carga, um script SQL calcula 4 indicadores de negócio baseados nos dados importados.

### Estrutura de dados

**Tabelas criadas:**
- `usuarios`: id, nome, email, idade, genero, telefone
- `empresas`: id, usuario_id (FK), nome_empresa, cargo, departamento
- `indicadores`: nm_indicador, vlr_indicador

**Indicadores calculados:**
- Percentual de profissionais de TI com menos de 40 anos
- Média de idade dos usuários
- Total de departamentos únicos
- Percentual de mulheres na base

### Como executar

```bash
cd questao_1

# Instalar dependências
pip install -r requirements.txt

# Executar ETL
python etl_users.py

# Executar cálculo de indicadores (após o ETL)
sqlite3 users.db < indicadores.sql
```

O arquivo `users.db` será criado no diretório atual (já está no .gitignore).

---

## Questão 2 - DAG Airflow de Régua de Cobrança

### O que faz

DAG do Apache Airflow que implementa um pipeline de processamento de pagamentos com lógica condicional baseada no dia da semana.

**Fluxo:**
1. FileSensor aguarda chegada do arquivo `pagamentos_d-1.csv`
2. Quando o arquivo chega, verifica o dia da semana
3. **Dias úteis (seg-sex)**: processa e carrega no banco de produção
4. **Finais de semana (sáb-dom)**: apenas arquiva o arquivo, sem processar

**Configurações:**
- Verificação a cada 5 minutos (poke_interval=300)
- 3 tentativas de retry com intervalo de 5 minutos
- Schedule diário (@daily)

### Como executar

```bash
# Copiar DAG para pasta do Airflow
cp questao_2/dag_regua_cobranca.py $AIRFLOW_HOME/dags/

# Reiniciar Airflow (se necessário)
airflow dags list | grep dag_regua_cobranca

# Testar a DAG
airflow dags test dag_regua_cobranca 2024-01-15
```

**Nota:** Esta questão não requer ambiente Airflow funcionando, apenas demonstra o código da DAG.

---

## Questão 3 - Validação de Dados no BigQuery

### O que faz

Procedure SQL no BigQuery que compara duas tabelas (local vs GCP) e identifica inconsistências entre elas.

**Tipos de inconsistências detectadas:**
- **AUSENTE_GCP**: Registros que existem no local mas não no GCP
- **AUSENTE_LOCAL**: Registros que existem no GCP mas não no local
- **DIVERGENCIA_VALORES**: Registros com valores diferentes entre as bases

**Tabela de saída:** `tb_inconsistencias_aplicacoes`

A procedure gera um relatório consolidado mostrando a quantidade de cada tipo de inconsistência encontrada.

### Pré-requisitos

Antes de executar a procedure, é necessário preparar o ambiente no BigQuery.

**1. Criar o dataset:**

```sql
CREATE SCHEMA IF NOT EXISTS `elviscsprot-desafio.engenharia_dados`;
```

**2. Carregar as tabelas de dados:**

Os arquivos CSV estão em `questao_3/dados/` e precisam ser carregados como tabelas no BigQuery.

**Opção A - Via Console do BigQuery:**

1. Acesse o BigQuery Console
2. Selecione o dataset `elviscsprot-desafio.engenharia_dados`
3. Clique em "Criar tabela"
4. Em "Criar tabela de": selecione "Upload"
5. Faça upload de `application_record_local.csv`
6. Nome da tabela: `application_record_local`
7. Marque "Detectar esquema automaticamente"
8. Clique em "Criar tabela"
9. Repita os passos para `application_record_gcp.csv`

**Opção B - Via bq CLI:**

```bash
# Carregar tabela local
bq load \
  --source_format=CSV \
  --autodetect \
  elviscsprot-desafio:engenharia_dados.application_record_local \
  questao_3/dados/application_record_local.csv

# Carregar tabela GCP
bq load \
  --source_format=CSV \
  --autodetect \
  elviscsprot-desafio:engenharia_dados.application_record_gcp \
  questao_3/dados/application_record_gcp.csv
```

### Como executar

**Opção 1 - Execução Local (SQLite + Python):**

```bash
cd questao_3

# Instalar dependências
pip install -r requirements.txt

# Executar validação local
python validacao_local.py
```

O script irá:
1. Carregar os CSVs (local e GCP)
2. Inserir no banco SQLite
3. Comparar as bases e identificar inconsistências
4. Gerar arquivo `validacao.db` com os resultados

**Opção 2 - BigQuery (Cloud):**

```bash
# Via bq CLI
bq query --use_legacy_sql=false < questao_3/validacao_aplicacoes.sql

# Executar a procedure (após criada)
CALL `elviscsprot-desafio.engenharia_dados.prc_load_validacao_aplicacoes`(5008804, 5008900);
```

**Parâmetros:**
- `id_inicio` (INT64) - ID inicial da faixa de registros a validar
- `id_fim` (INT64) - ID final da faixa de registros a validar

### Consultar resultados

Após executar a procedure, é possível consultar as inconsistências encontradas:

```sql
-- Visualizar todas as inconsistências
SELECT *
FROM `elviscsprot-desafio.engenharia_dados.tb_inconsistencias_aplicacoes`
ORDER BY tipo_inconsistencia, id_registro;

-- Resumo por tipo de inconsistência
SELECT 
    tipo_inconsistencia,
    COUNT(*) AS total_inconsistencias
FROM `elviscsprot-desafio.engenharia_dados.tb_inconsistencias_aplicacoes`
GROUP BY tipo_inconsistencia
ORDER BY total_inconsistencias DESC;

-- Filtrar apenas divergências de valores
SELECT *
FROM `elviscsprot-desafio.engenharia_dados.tb_inconsistencias_aplicacoes`
WHERE tipo_inconsistencia = 'DIVERGENCIA_VALORES';
```

---

## Tecnologias Utilizadas

- **Python 3.x**: ETL e processamento de dados
- **SQLite**: Banco de dados local
- **Apache Airflow**: Orquestração de pipelines
- **Google BigQuery**: Data warehouse e validação de dados
- **SQL**: Transformação e análise de dados

---

## Observações

- O arquivo `.gitignore` está configurado para não versionar bancos de dados (*.db)
- Todos os scripts são independentes e podem ser executados separadamente
- Códigos seguem boas práticas de engenharia de dados com foco em legibilidade e manutenibilidade

