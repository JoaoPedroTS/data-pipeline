# Data Pipeline – Sistema de Franquias de Café

## 1. Visão Geral

Este projeto implementa uma **pipeline de dados ponta a ponta** simulando um sistema de franquias de café.

A solução:

* Consome dados de vendas via API (JSON)
* Realiza transformações com **PySpark**
* Persiste os dados em **PostgreSQL**
* É orquestrada com **Apache Airflow**
* Executa em ambiente containerizado com **Docker**

---

## 2. Arquitetura da Solução

### Fluxo da Pipeline

```
API → Extract → JSON local
      ↓
Transform (Spark)
      ↓
Load (PostgreSQL)
```

### Estrutura de Camadas

| Camada       | Responsabilidade                                     |
| ------------ | ---------------------------------------------------- |
| Extract      | Consumir API e salvar JSON bruto                     |
| Transform    | Limpeza, tipagem, enriquecimento e regras de negócio |
| Load         | Persistência via JDBC no PostgreSQL                  |
| Orquestração | Execução, retry, dependências e agendamento          |

---

## 3. Fonte de Dados – API de Pedidos

A API retorna dados no seguinte formato:

```json
{
  "items": [...],
  "order": {...}
}
```

### Entidades principais:

#### Order

* id
* establishment_id
* created_at
* payment_method
* total_amount

#### Order Items

* id
* menu_item_id
* order_id
* quantity
* unit_price

---

## 4. Transformações Implementadas

### 4.1 Tabela Cafés

Arquivo: `cafes.csv`

Transformações:

* Normalização da coluna `seats`
* Criação de:

  * `min_seats`
  * `max_seats`
  * `avg_seats`
* Conversão de colunas booleanas
* Remoção de espaços
* Criação da coluna categórica `establishment_type`

Regras de categorização:

* < 10 lugares → Coffee Stand / To Go
* \> 40 lugares + WiFi + tomadas → Co-working Friendly
* \> 40 lugares → Large Cafe
* Caso contrário → Standard Cafe

---

### 4.2 Tabela Menu Items

Transformações:

* Conversão para double (price, cost)
* Cálculo de:
  * `gross_margin`
  * `margin_percentage`
* Classificação de preço:

  * < 10 → Budget
  * 10–20 → Standard
  * \> 20 → Premium

---

### 4.3 Tabela Orders

Transformações temporais:

A partir de `created_at` são geradas:

* `order_date`
* `order_hour`
* `day_of_week`
* `month_name`
* `day_period`

Classificação de período do dia:

* 6–11 → Manhã
* 12–14 → Almoço
* 15–18 → Tarde
* ≥19 → Noite

---

### 4.4 Tabela Orders Items

Transformação:

* Cálculo de `total_price = quantity * unit_price`

---

## 5. Estratégia de Carga

### Tabelas dimensionais

* cafes_table → overwrite (truncate)
* menu_items_table → overwrite (truncate)

### Tabelas fact

* orders_table → append
* orders_items_table → append

Essa estratégia simula um modelo próximo a um **Data Warehouse simplificado**, onde:

* Dimensões são reconstruídas
* Fatos são acumulativos

---

## 6. Orquestração com Airflow

DAG: `cafe_analises`

Características:

* Execução a cada 1 hora
* Retry automático (2 tentativas)
* Delay de 5 minutos entre retries
* Sem catchup

Fluxo:

```
extract
   ↓
[process_cafe, process_menu, process_orders, process_order_items]
```

O extract deve ser concluído antes das demais tasks.

---

## 7. Organização do Projeto

```
src/
 ├── extract.py
 ├── transform.py
 ├── load.py
 └── main_dag.py
data/
config/
```

Separação clara por responsabilidade:

* Extração isolada
* Transformações puras (DataFrame in → DataFrame out)
* Camada de carga desacoplada

---

# 📊 Modelo de Dados Final

### Dimensões:

* cafes_table
* menu_items_table

### Fatos:

* orders_table
* orders_items_table

Relacionamentos principais:

* orders.order_id → orders_items.order_id
* orders_items.menu_item_id → menu_items.id
* orders.establishment_id → cafes.id

---

# ⚙️ Decisões Técnicas Relevantes

### 1. Uso de Spark

Mesmo para volume pequeno, o uso de Spark:

* Demonstra capacidade de escalar
* Simula ambiente real de Big Data
* Permite adicionar lógica distribuída futura

### 2. Uso de Airflow

Permite:

* Observabilidade
* Retry automático
* Dependências explícitas
* Escalabilidade futura

---

# 🔍 Pontos Fortes do Projeto

* Separação clara de responsabilidades
* Transformações orientadas a negócio
* Enriquecimento temporal (boa prática analítica)
* Uso de variáveis de ambiente para credenciais
* Estrutura pronta para escalar
