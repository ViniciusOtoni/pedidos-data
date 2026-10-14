"""Data product do BC Pedidos: pedidos e valor por dia e status.

Lógica de negócio do Gold é do domínio (a plataforma não oferece framework para o Gold).
Executado pelo job pedidos_gold, disparado por table update em <catalogo>.silver.pedido.
"""

import sys

from pyspark.sql import SparkSession

catalog = sys.argv[1]
spark = SparkSession.builder.getOrCreate()

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.gold.pedidos_por_status_diario
    COMMENT 'Pedidos e valor por dia de criação e status atual (data product do BC Pedidos)'
    AS
    SELECT
        CAST(criado_em AS DATE)      AS dia,
        status,
        COUNT(*)                     AS pedidos,
        ROUND(SUM(valor_total), 2)   AS valor_total,
        MAX(_atualizado_em)          AS atualizado_em
    FROM {catalog}.silver.pedido
    GROUP BY ALL
""")
print(spark.table(f"{catalog}.gold.pedidos_por_status_diario").orderBy("dia", "status").toPandas().to_string())
