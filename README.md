# MPCS-53001 Final Project

## Directory Structure
```text
MPCS-53001-Final-Project/
├── graph/
│   └── to_neo4j_script.py          # MySQL/Mongo -> Neo4j ETL
├── neo4j/
│   └── etl.py                      # Additional Neo4j utilities (if used)
├── prompts/
│   ├── data-generation-prompt.md
│   ├── data-generation-prompt-v2.md
│   └── relational_schema.txt       # ERD source text
├── queries/
│   ├── relational_queries.sql      # SQL answers to project questions
│   ├── mongo_queries.js            # MongoDB queries
│   └── graph_queries.cyp           # Cypher queries
├── scripts/
│   ├── data_generation.py          # Main synthetic data generator
│   └── test_script.py
├── sql/
│   ├── schema.sql                  # Relational schema
│   ├── setup.py                    # DB reset/setup script
│   └── data_gen_eqa.sql            # Data quality / integrity checks
├── project.txt                     # Assignment description
└── README.md
```

## Prerequisites

Required packages:
- `mysql-connector-python`
- `pymysql`
- `pymongo`
- `redis`
- `neo4j`
- `faker`

## Instrictions

### 1) Reset / Initialize relational DB
```bash
python3 sql/setup.py
```

### 2) Test data generation script
```bash
python3 scripts/data_generation.py \
  --users 100 \
  --products 500 \
  --orders 2000 \
  --events 10000
```

### 3) Full data-generation run 
```bash
python3 sql/setup.py
python3 scripts/data_generation.py \
  --users 1000 \
  --products 5000 \
  --orders 100000 \
  --events 500000
```

### 4) Neo4j ETL 
```bash
python3 graph/to_neo4j_script.py
```
