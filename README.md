# PRÁCTICA CREATIVA – Predicción de retrasos de vuelo

En esta práctica final de Ingeniería Big Data en la Nube, hemos implementado una arquitectura completa para predecir retrasos de vuelos en tiempo real utilizando diversas tecnologías. Se explica a continuación el enfoque que hemos realizado.


## Objetivos

Desplegar una arquitectura Big Data dockerizada capaz de:

- Recibir peticiones de predicción desde una web en Flask.
- Procesar dichas peticiones con Spark.
- Guardar resultados en Kafka, MongoDB y HDFS.
- Visualizar resultados en tiempo real usando websockets.
- Migrar datos de distancias desde MongoDB a Cassandra para su consulta.


## Tecnologías usadas

| Tecnología  | Descripción |
|-------------|-------------|
| **Docker** | Encapsulación en contenedores y orquestación de la arquitectura completa. |
| **Flask** | Aplicación web para introducir datos de vuelos y mostrar predicciones. |
| **Kafka** | Sistema de mensajería para enviar y recibir peticiones y resultados. |
| **Spark** | Motor de procesamiento. |
| **MongoDB** | Almacenamiento de resultados de predicción. |
| **HDFS** | Almacenamiento adicional. |
| **Cassandra** |Almacenamiento y consulta de distancias entre aeropuertos para el job de predicción. |
| **NiFi** | Lectura periódica de las predicciones desde Kafka y guardado en ficheros .txt. |

---

##  Pasos realizados 
 **1. Dockerización completa**

- Encapsulamos cada servicio en contenedores Docker y definimos docker-compose.yaml para levantar toda la arquitectura con `docker compose up --build` .

 **2. Modificación de la aplicación Flask**

- Implementamos el uso de websockets para capturar la respuesta en tiempo real desde los tópicos de Kafka.
- Mantuvimos todo guardado en MongoDB y añadimos la presentación de la predicción en la interfaz web.

 **3. Kafka como sistema de intercambio**

- Se configuraron productores y consumidores para enviar y recibir predicciones correctamente.

 **4. Procesamiento con Spark**
Spark se utiliza como motor de procesamiento para:
- Leer las peticiones desde Kafka.
- Ejecutar el modelo de predicción.
- Guardar los resultados en MongoDB y en HDFS.

 **5. NiFi para lectura periódica**

- Desplegamos Apache NiFi y creamos un flujo para leer las predicciones en Kafka cada 10 segundos y guardarlas en ficheros `.txt`.

**6. Guardado de predicciones en HDFS**

- Además de MongoDB, configuramos Spark para escribir las predicciones en HDFS como archivos, integrando el ecosistema Hadoop en la arquitectura.

**7. Migración de datos a Cassandra**

- Para obtener las distancias entre aeropuertos, se migraron los datos desde MongoDB a Cassandra utilizando un script de migración.
- Spark consulta directamente en Cassandra durante la ejecución del job de predicción, eliminando la dependencia de MongoDB.


---

**Autores**

Amparo Hinojosa Martínez 

Carmen Hidalgo Jiménez
