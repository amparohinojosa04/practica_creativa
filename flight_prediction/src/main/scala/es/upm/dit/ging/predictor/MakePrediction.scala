package es.upm.dit.ging.predictor

import com.mongodb.spark._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery   
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("spark://spark-master:7077")
      .getOrCreate()
    import spark.implicits._

    // Ruta local en el contenedor donde están los modelos
     //Load the arrival delay bucketizer
    val base_path = "/home/ibdn/practica_creativa"
    val arrivalBucketizerPath = "%s/models/arrival_bucketizer_2.0.bin".format(base_path)
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns = Seq("Carrier", "Origin", "Dest", "Route")

    val stringIndexerModelPath = columns.map(n =>
      ("%s/models/string_indexer_model_".format(base_path) + "%s.bin".format(n)).toSeq)
    val stringIndexerModel = stringIndexerModelPath.map(n => StringIndexerModel.load(n.toString))
    val stringIndexerModelMap = (columns zip stringIndexerModel).toMap

    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath =
      "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)


    //Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))
    flightNestedDf.printSchema()

    // DataFrame for Vectorizing string fields with the corresponding pipeline for that column
    spark.conf.set("spark.cassandra.connection.host", "cassandra")
    
    // Leer distancias desde Cassandra
    val distances = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "flights")
      .option("table", "origin_dest_distances")
      .load()
      
      
    val distancesClean = distances.select($"Origin", $"Dest", $"distance".as("Distance"))  
      
    
    // Flight flatten sin distancia (solo campos base)
    val flightFlattenedDf = flightNestedDf.selectExpr(
       "flight.Origin",
       "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
       "flight.DepDelay","flight.Timestamp","flight.FlightDate",
       "flight.Carrier","flight.UUID"
    )

// Agregamos columna Route y hacemos join con distancias desde Cassandra
    val enrichedDf = flightFlattenedDf
       .withColumn("Route", concat($"Origin", lit("-"), $"Dest"))
       .join(distancesClean, Seq("Origin", "Dest"), "left")
       .na.fill(Map("Distance" -> 0.0))

// Aplicar los string indexers para generar columnas *_index
    val dfWithIndexes = stringIndexerModelMap.foldLeft(enrichedDf) {
       case (df, (col, model)) => model.transform(df)
    }

// Seleccionar solo las columnas necesarias (incluidos los índices)
    val finalDf = dfWithIndexes.select(
      $"Origin",
      $"DayOfWeek",
      $"DayOfYear",
      $"DayOfMonth",
      $"Dest",
      $"DepDelay",
      $"Timestamp",
      $"FlightDate",
      $"Carrier",
      $"UUID",
      $"Distance",
      $"Carrier_index",
      $"Origin_index",
      $"Dest_index",
      $"Route_index"
    )

// Vectorizar las características numéricas y los índices
    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(finalDf)

// El resto del pipeline sigue igual
    val finalVectorizedFeatures = vectorizedFeatures
       .drop("Carrier_index")
       .drop("Origin_index")
       .drop("Dest_index")
       .drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")


    // Inspect the output
    finalPredictions.printSchema()


    val props = new Properties()
    props.put("bootstrap.servers", "kafka:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String]("flight-delay-ml-response", "init", "ready"))
    producer.close()

    // MongoDB output
    val mongoOutput = finalPredictions
      .writeStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017")
      .option("spark.mongodb.database", "agile_data_science")
      .option("spark.mongodb.collection", "flight_delay_ml_response")
      .option("checkpointLocation", "/tmp/")
      .outputMode("append")
      .start()

    val kafkaStreamWriter = finalPredictions
      .selectExpr("CAST(UUID AS STRING) as key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/kafka_checkpoint")
      .outputMode("append")
      .start()
      
    val hdfsOutput = finalPredictions
     .writeStream
     .outputMode("append")
     .format("parquet")
     .option("path", "hdfs://namenode:8020/user/agile/flight_predictions")  
     .option("checkpointLocation", "/tmp/hdfs_checkpoint")
     .start()

    val consoleOutput = finalPredictions
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

 
    mongoOutput.awaitTermination()
    kafkaStreamWriter.awaitTermination()
    consoleOutput.awaitTermination()
    hdfsOutput.awaitTermination()

  }
}

