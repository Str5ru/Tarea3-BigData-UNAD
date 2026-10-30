# Arquitectura del proyecto

## Procesamiento Batch

1. `SparkSession` inicializa la aplicación Spark.
2. Se leen datos de ejemplo o un CSV.
3. `dropna` elimina filas que no contienen los campos obligatorios.
4. `withColumn` normaliza y convierte campos y calcula `valor_total`.
5. `groupBy` y `agg` calculan métricas por categoría.

## Procesamiento Streaming

1. Los productores publican eventos JSON en el topic `viajes`.
2. Kafka conserva y distribuye los mensajes en el broker.
3. Spark Structured Streaming se conecta mediante `readStream.format("kafka")`.
4. El campo `value` se convierte a texto y `from_json` interpreta el esquema.
5. Se limpian y transforman los eventos.
6. Se calcula una agregación por categoría y ventana temporal.
7. `writeStream` publica los resultados en la consola en modo `update`.

## Flujo resumido

```text
CSV / Datos de ejemplo
        |
        v
   Spark Batch
        |
        v
 Limpieza + EDA

Productor JSON
      |
      v
 Kafka Topic (viajes)
      |
      v
 Spark Structured Streaming
      |
      v
 Parseo + Limpieza + Ventanas
      |
      v
 Consola (writeStream)
```
