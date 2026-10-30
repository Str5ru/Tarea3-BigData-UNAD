# Tarea 3 - Big Data | UNAD

Proyecto académico de ejemplo que implementa procesamiento **Batch con PySpark**
y procesamiento de eventos en tiempo real con **Spark Structured Streaming + Apache Kafka**.

## Contenido del repositorio

```text
Tarea3-BigData-UNAD/
├── README.md
├── tarea3_bigdata.py
├── requirements.txt
├── .gitignore
├── data/
│   └── datos_viajes.csv
└── docs/
    └── arquitectura.md
```

## Tecnologías

- Python 3.9 o superior (según la versión instalada de PySpark).
- Apache Spark 3.5.1.
- PySpark 3.5.1.
- Apache Kafka.
- Conector Spark-Kafka `spark-sql-kafka-0-10_2.12:3.5.1`.

> Se recomienda utilizar una versión de Java compatible con Spark 3.5.1 y verificar
> la configuración de `JAVA_HOME`, `SPARK_HOME` y `PATH` en el sistema operativo.

## 1. Preparar el entorno

Instalar la dependencia de Python:

```bash
pip install -r requirements.txt
```

Comprobar que Spark está disponible:

```bash
spark-submit --version
```

## 2. Ejecutar el procesamiento Batch

Desde la carpeta raíz del repositorio:

```bash
spark-submit tarea3_bigdata.py --mode batch
```

El comando anterior utiliza datos ficticios incluidos en el programa.

Para procesar el CSV del repositorio:

```bash
spark-submit tarea3_bigdata.py --mode batch --csv data/datos_viajes.csv
```

El archivo CSV debe contener estas columnas:

- `categoria`
- `cantidad`
- `valor`
- `fecha`

El programa elimina registros incompletos, normaliza la categoría, convierte
los tipos de datos, calcula `valor_total` y genera agregaciones por categoría.

## 3. Ejecutar el procesamiento Streaming con Kafka

Se necesita tener Apache Kafka en ejecución. Los comandos exactos pueden variar
según la instalación y la versión de Kafka.

### Crear el topic

```bash
kafka-topics.sh --create --topic viajes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

En Windows, el ejecutable puede llamarse `kafka-topics.bat`.

### Iniciar Spark Structured Streaming

En una terminal:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 tarea3_bigdata.py --mode stream --topic viajes --bootstrap localhost:9092
```

### Enviar eventos JSON a Kafka

En otra terminal, inicia un productor:

```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic viajes
```

Escribe mensajes JSON, uno por línea:

```json
{"categoria":"economico","cantidad":2,"valor":15000,"fecha":"2026-10-29T18:30:00"}
{"categoria":"premium","cantidad":1,"valor":28000,"fecha":"2026-10-29T18:31:00"}
{"categoria":"estandar","cantidad":3,"valor":19000,"fecha":"2026-10-29T18:32:00"}
```

Spark procesa los eventos en micro-lotes y muestra las métricas agrupadas por
categoría y ventana de un minuto en la consola. Para detener la consulta,
presiona `Ctrl + C`.

## Estructura y funcionamiento

- **Batch:** lectura de datos, limpieza con `dropna`, transformación con
  `withColumn` y análisis exploratorio con `groupBy` y `agg`.
- **Streaming:** lectura del topic Kafka con `readStream`, conversión del
  valor a texto, parseo JSON con `from_json`, agregación por ventana y salida con
  `writeStream`.
- **Kafka:** recibe y distribuye los eventos.
- **Spark Structured Streaming:** procesa los eventos en micro-lotes.

## Consideraciones

- El dataset es ficticio y se utiliza con fines académicos.
- El modo Batch puede ejecutarse sin Kafka.
- El modo Streaming requiere un broker Kafka accesible desde Spark.
- El nombre del paquete del conector Kafka debe corresponder a las versiones
  de Spark, Scala y Kafka Connector utilizadas.
- El checkpoint se crea en `checkpoint_tarea3_kafka/`; no se debe subir al
  repositorio.
- Antes de entregar, reemplaza el campo de autor en el script si corresponde
  y agrega las evidencias de ejecución solicitadas por el docente.

## Referencias

- [Documentación oficial de Apache Spark](https://spark.apache.org/docs/3.5.1/)
- [PySpark API](https://spark.apache.org/docs/3.5.1/api/python/)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/3.5.1/structured-streaming-kafka-integration.html)
- [Documentación de Apache Kafka](https://kafka.apache.org/documentation/)
