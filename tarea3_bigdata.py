"""
TAREA 3 - BIG DATA (UNAD)
Procesamiento Batch con PySpark y procesamiento en tiempo real con
Spark Structured Streaming + Apache Kafka.

Autor: [Escribe aquí tu nombre]
Curso: Big Data - UNAD

REQUISITOS
----------
- Java compatible con la versión de Spark instalada.
- Python 3.9 o superior (según la versión de PySpark).
- Apache Spark 3.5.1.
- Apache Kafka en ejecución para el modo streaming.
- Paquetes Python: pyspark==3.5.1

INSTALACIÓN BÁSICA
------------------
    pip install pyspark==3.5.1

EJECUCIÓN BATCH
---------------
Con datos de ejemplo (no necesita CSV):
    spark-submit tarea3_bigdata.py --mode batch

Con un archivo CSV:
    spark-submit tarea3_bigdata.py --mode batch --csv datos_viajes.csv

El CSV debe contener, como mínimo, las columnas:
    categoria, cantidad, valor, fecha

EJECUCIÓN STREAMING
-------------------
Primero, inicia Kafka y crea un topic llamado viajes:

    kafka-topics.sh --create --topic viajes --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1

En Windows, el comando puede ser kafka-topics.bat y debes ejecutarlo
desde la carpeta bin de Kafka.

Inicia el consumidor Spark:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        tarea3_bigdata.py --mode stream --topic viajes \
        --bootstrap localhost:9092

En otra terminal, abre un productor:
    kafka-console-producer.sh --broker-list localhost:9092 --topic viajes

En Windows, puede llamarse kafka-console-producer.bat.

Envía mensajes JSON, uno por línea, por ejemplo:
    {"categoria":"economico","cantidad":2,"valor":15000,"fecha":"2026-10-29T18:30:00"}
    {"categoria":"premium","cantidad":1,"valor":28000,"fecha":"2026-10-29T18:31:00"}

IMPORTANTE
----------
- El nombre del paquete Kafka debe coincidir con las versiones de Spark,
  Scala y el conector Kafka que se estén utilizando.
- El modo streaming espera mensajes JSON con los cuatro campos indicados.
- Para una ejecución real, reemplaza los datos ficticios por datos autorizados.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


# ---------------------------------------------------------------------------
# 1. CONFIGURACIÓN GENERAL Y ESQUEMA DE LOS DATOS
# ---------------------------------------------------------------------------

# Esquema común para el dataset batch y los mensajes JSON de Kafka.
# "fecha" se recibe como texto y después se convierte a timestamp.
ESQUEMA_DATOS = StructType([
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("valor", DoubleType(), True),
    StructField("fecha", StringType(), True),
])


def crear_spark_session(modo):
    """
    Crea la sesión principal de Spark.

    En modo streaming, el conector Kafka se agrega al ejecutar spark-submit
    con --packages. Así se evita descargar dependencias innecesarias en batch.
    """
    return (
        SparkSession.builder
        .appName(f"UNAD_Tarea3_BigData_{modo}")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 2. PROCESAMIENTO BATCH CON PYSPARK
# ---------------------------------------------------------------------------

def crear_dataset_de_ejemplo(spark):
    """
    Construye un pequeño dataset ficticio para que el programa pueda
    ejecutarse incluso cuando el estudiante todavía no tenga un CSV.
    """
    datos = [
        ("economico", 2, 15000.0, "2026-10-29 08:10:00"),
        ("premium", 1, 28000.0, "2026-10-29 08:25:00"),
        ("economico", 3, 12000.0, "2026-10-29 09:00:00"),
        ("estandar", 1, 19000.0, "2026-10-29 09:15:00"),
        ("premium", 2, 30000.0, "2026-10-29 10:00:00"),
        ("estandar", 4, 17500.0, "2026-10-29 10:30:00"),
        ("economico", 1, 14000.0, "2026-10-29 11:00:00"),
        # Esta fila contiene un nulo para demostrar el uso de dropna().
        ("premium", None, 25000.0, "2026-10-29 11:30:00"),
    ]

    return spark.createDataFrame(
        datos,
        schema=ESQUEMA_DATOS
    )


def ejecutar_batch(spark, ruta_csv=None):
    """
    Ejecuta el procesamiento por lotes:
    carga -> limpieza -> transformación -> EDA -> resultados.
    """
    print("\n========== PROCESAMIENTO BATCH ==========")

    # 2.1 Carga de datos desde CSV o dataset ficticio.
    if ruta_csv:
        print(f"Leyendo archivo CSV: {ruta_csv}")
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(ruta_csv)
        )
    else:
        print("No se indicó CSV. Se utilizará el dataset ficticio.")
        df = crear_dataset_de_ejemplo(spark)

    print("\nEsquema original:")
    df.printSchema()

    print("\nDatos originales:")
    df.show(truncate=False)

    # 2.2 Limpieza: eliminar registros que no tengan los campos esenciales.
    # Se conservan únicamente las columnas requeridas para este análisis.
    columnas_requeridas = ["categoria", "cantidad", "valor", "fecha"]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]

    if faltantes:
        raise ValueError(
            "Al CSV le faltan estas columnas obligatorias: "
            + ", ".join(faltantes)
        )

    df_limpio = (
        df.select(*columnas_requeridas)
        .dropna(subset=columnas_requeridas)
    )

    # 2.3 Transformaciones con withColumn:
    # - Normaliza el texto de la categoría.
    # - Convierte fecha de texto a timestamp.
    # - Crea una columna calculada con el valor total del registro.
    df_limpio = (
        df_limpio
        .withColumn("categoria", F.lower(F.trim(F.col("categoria"))))
        .withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("valor", F.col("valor").cast("double"))
        .withColumn("fecha", F.to_timestamp(F.col("fecha")))
        .withColumn(
            "valor_total",
            F.col("cantidad") * F.col("valor")
        )
        .dropna(subset=["categoria", "cantidad", "valor", "fecha"])
    )

    # Evita registros con cantidades o valores negativos.
    df_limpio = df_limpio.filter(
        (F.col("cantidad") >= 0) & (F.col("valor") >= 0)
    )

    print("\nDatos después de la limpieza y transformación:")
    df_limpio.show(truncate=False)

    # 2.4 EDA: cantidad de registros, promedio y suma por categoría.
    print("\nEDA: métricas agrupadas por categoría:")
    resumen = (
        df_limpio
        .groupBy("categoria")
        .agg(
            F.count("*").alias("numero_registros"),
            F.sum("cantidad").alias("cantidad_total"),
            F.avg("valor").alias("valor_promedio"),
            F.sum("valor_total").alias("ingreso_total"),
        )
        .orderBy(F.desc("numero_registros"))
    )
    resumen.show(truncate=False)

    # 2.5 Métricas generales del conjunto de datos.
    print("\nEDA: resumen general:")
    df_limpio.agg(
        F.count("*").alias("total_registros"),
        F.avg("valor").alias("promedio_valor"),
        F.avg("cantidad").alias("promedio_cantidad"),
        F.sum("valor_total").alias("valor_total_general"),
    ).show(truncate=False)

    print("========== FIN DEL PROCESAMIENTO BATCH ==========\n")


# ---------------------------------------------------------------------------
# 3. PROCESAMIENTO EN TIEMPO REAL: SPARK STRUCTURED STREAMING + KAFKA
# ---------------------------------------------------------------------------

def ejecutar_streaming(spark, bootstrap_servers, topic):
    """
    Lee mensajes JSON desde Kafka y calcula métricas por categoría y ventana
    de tiempo usando micro-lotes de Spark Structured Streaming.
    """
    print("\n========== PROCESAMIENTO STREAMING ==========")
    print(f"Servidor Kafka: {bootstrap_servers}")
    print(f"Topic: {topic}")

    # 3.1 Lectura continua del topic de Kafka.
    # Kafka entrega key y value como bytes; value se convierte a texto.
    flujo_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # 3.2 Conversión del valor del mensaje a String y parseo del JSON.
    mensajes_json = flujo_kafka.select(
        F.col("value").cast("string").alias("json")
    )

    datos_parseados = mensajes_json.select(
        F.from_json(F.col("json"), ESQUEMA_DATOS).alias("datos")
    ).select("datos.*")

    # 3.3 Limpieza y conversión de tipos.
    datos_limpios = (
        datos_parseados
        .withColumn("categoria", F.lower(F.trim(F.col("categoria"))))
        .withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("valor", F.col("valor").cast("double"))
        .withColumn("fecha", F.to_timestamp(F.col("fecha")))
        .dropna(subset=["categoria", "cantidad", "valor", "fecha"])
        .filter(
            (F.col("cantidad") >= 0) & (F.col("valor") >= 0)
        )
        .withColumn(
            "valor_total",
            F.col("cantidad") * F.col("valor")
        )
    )

    # 3.4 Agregación por micro-lotes y ventanas de tiempo.
    # watermark permite gestionar datos que llegan con retraso.
    # La ventana agrupa eventos en intervalos de un minuto.
    metricas_tiempo_real = (
        datos_limpios
        .withWatermark("fecha", "2 minutes")
        .groupBy(
            F.window(F.col("fecha"), "1 minute"),
            F.col("categoria")
        )
        .agg(
            F.count("*").alias("numero_eventos"),
            F.sum("cantidad").alias("cantidad_total"),
            F.avg("valor").alias("valor_promedio"),
            F.sum("valor_total").alias("ingreso_total"),
        )
        .select(
            F.col("window.start").alias("inicio_ventana"),
            F.col("window.end").alias("fin_ventana"),
            F.col("categoria"),
            F.col("numero_eventos"),
            F.col("cantidad_total"),
            F.round(F.col("valor_promedio"), 2).alias("valor_promedio"),
            F.round(F.col("ingreso_total"), 2).alias("ingreso_total"),
        )
    )

    # 3.5 Escritura en consola.
    # "update" muestra las filas agregadas que cambian en cada micro-lote.
    consulta = (
        metricas_tiempo_real.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", "./checkpoint_tarea3_kafka")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming activo. Esperando mensajes de Kafka...")
    print("Presiona Ctrl + C para detener la ejecución.")
    consulta.awaitTermination()


# ---------------------------------------------------------------------------
# 4. PUNTO DE ENTRADA DEL PROGRAMA
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Tarea 3 UNAD: PySpark Batch y Spark Streaming con Kafka"
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "stream"],
        default="batch",
        help="Selecciona el modo: batch o stream."
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Ruta del archivo CSV para el procesamiento batch."
    )
    parser.add_argument(
        "--topic",
        default="viajes",
        help="Topic de Kafka para el modo streaming."
    )
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="Dirección del servidor Kafka."
    )

    args = parser.parse_args()
    spark = crear_spark_session(args.mode)

    try:
        if args.mode == "batch":
            ejecutar_batch(spark, args.csv)
        else:
            ejecutar_streaming(
                spark,
                bootstrap_servers=args.bootstrap,
                topic=args.topic
            )
    except KeyboardInterrupt:
        print("\nEjecución detenida por el usuario.")
    except Exception as error:
        print(f"\nSe produjo un error: {error}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
