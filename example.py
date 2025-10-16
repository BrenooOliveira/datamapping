from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when, round

# ========================
# 1. Inicialização
# ========================
spark = SparkSession.builder \
    .appName("ETL_Vendas") \
    .getOrCreate()

# ========================
# 2. Extração - Carregando tabelas
# ========================
clientes_df = spark.read.csv("data/clientes.csv", header=True, inferSchema=True)
enderecos_df = spark.read.csv("data/enderecos.csv", header=True, inferSchema=True)
pedidos_df = spark.read.csv("data/pedidos.csv", header=True, inferSchema=True)
itens_df = spark.read.csv("data/itens_pedido.csv", header=True, inferSchema=True)
produtos_df = spark.read.csv("data/produtos.csv", header=True, inferSchema=True)

# ========================
# 3. Transformação
# ========================

# --- Normalizações e Limpeza ---
clientes_df = clientes_df.withColumn("nome", trim(upper(col("nome"))))
enderecos_df = enderecos_df.withColumn("cidade", upper(trim(col("cidade"))))
produtos_df = produtos_df.withColumn("preco", round(col("preco"), 2))

# --- Filtros simples ---
pedidos_df = pedidos_df.filter(col("status") == "FINALIZADO")
produtos_df = produtos_df.filter(col("ativo") == True)

# --- Enriquecimento (join 1: cliente + endereço) ---
cliente_endereco_df = clientes_df.join(
    enderecos_df,
    clientes_df["id_endereco"] == enderecos_df["id_endereco"],
    "left"
).select(
    clientes_df["id_cliente"],
    clientes_df["nome"].alias("nome_cliente"),
    enderecos_df["cidade"],
    enderecos_df["estado"]
)

# --- Join 2: pedidos com cliente_endereco ---
pedidos_clientes_df = pedidos_df.join(
    cliente_endereco_df,
    "id_cliente"
)

# --- Join 3: itens + produtos ---
itens_produtos_df = itens_df.join(
    produtos_df,
    "id_produto"
).select(
    "id_pedido",
    "id_produto",
    "descricao",
    "quantidade",
    "preco",
    (col("quantidade") * col("preco")).alias("valor_total_item")
)

# --- Join 4: consolidar tudo ---
fato_vendas_df = pedidos_clientes_df.join(
    itens_produtos_df,
    "id_pedido"
).select(
    "id_pedido",
    "data_pedido",
    "nome_cliente",
    "cidade",
    "estado",
    "descricao",
    "quantidade",
    "preco",
    "valor_total_item"
)

# --- Agregações finais (exemplo) ---
vendas_por_cidade = fato_vendas_df.groupBy("cidade").sum("valor_total_item") \
    .withColumnRenamed("sum(valor_total_item)", "valor_total_vendas")

# ========================
# 4. Load - Escrita do resultado
# ========================

# Escreve em Parquet 
# Opcional: salvar no banco
vendas_por_cidade.write \
     .format("jdbc") \
     .option("url", "jdbc:postgresql://localhost:5432/db_vendas") \
     .option("dbtable", "vw_vendas_cidade") \
     .option("user", "user") \
     .option("password", "senha") \
     .save()

spark.stop()

