package com.chinmay.cloud.spark.airportairlineavgdelay

//import java.sql.Time
import java.util.HashMap

import kafka.serializer.StringDecoder

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector._


/**
   Origin
   Dest
   UniqueCarrier
   DepDelay
   ArrDelay
   DayOfWeek

   def spltline(str: String): (String, (Double, Long)) = {
    return ((str.split(";")(1) + ";" + str.split(";")(3)), (str.split(";")(7).toDouble, 1l))
   }
   val srcdestdelay = distfile.map (line => spltline(line))
   val srcdestdelaysum = srcdestdelay.reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2) )
   val srcdestdelayavg =  srcdestdelaysum.map( x => (x._1, (x._2._1 / x._2._2) ) )


 * 
 */
object StatefulSrcDstAirportDelay {

  def spltline(str: String): (String, (Double, Long)) = {
     val splitstr = str.split("\\|")
    return ((splitstr(0) + ";" + splitstr(1)), (splitstr(3).toDouble, 1l))
  }

  def trackStateFunc(btchTime: Time, key: String, value: Option[(Double, Long)], state: State[(Double, Long, org.apache.spark.streaming.Time)]): Option[(String, (Double, Long, org.apache.spark.streaming.Time))] = {
     val tnow = new Time(btchTime.milliseconds)
     val newval = value.getOrElse(0.0, 0l)
     val statesum = state.getOption.getOrElse(0.0, 0l, tnow)
     val sum = (newval._1 + statesum._1, newval._2 + statesum._2, tnow)
     state.update(sum)
     val output = (key, sum)
     // output
     Some(output)
  }

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: <zkQuorum> <group> <topics> <numThreads> <cassandra ip>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads, cassandraIp) = args
    val sparkConf = new SparkConf(true)
                     .setAppName("StatefulSourceDestAirportDepDelay")
                     .set("spark.cassandra.connection.host", cassandraIp)

    // Create the context with a 10 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("srcdestairportdepdelay")

    // Initial state RDD for mapWithState operation
    val tnow = new Time(0)
    val initialRDD = ssc.sparkContext.parallelize(List( ("unknown;unknown", (10000.0, 1l, tnow)) ))

    val stateSpec = StateSpec.function(trackStateFunc _)
                             .initialState(initialRDD)
                             .numPartitions(3)

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val srcdestdelay = lines.map (line => spltline(line))

    val stateDstream = srcdestdelay.mapWithState(stateSpec)
    val snapshot = stateDstream.stateSnapshots()
    //stateDstream.print()
    //val mapStateDstream = stateDstream.map( (r) => (r._1.split(";")(0), r._1.split(";")(1), (r._2._1/r._2._2)) )
    val mapStateDstream = snapshot.map( (r) => (r._1.split(";")(0), r._1.split(";")(1), (r._2._1/r._2._2) , r._2._3) )
    mapStateDstream.foreachRDD { (rdd, t) =>
         //rdd.saveToCassandra("spark", "sourcedestairportdepdelay", SomeColumns("source", "dest", "avgdepdelay"))
         rdd
            .filter( r => r._4 >= t)
            .map ( (r) => (r._1, r._2, r._3) )
            .saveToCassandra("spark", "sourcedestairportdepdelay", SomeColumns("source", "dest", "avgdepdelay"))
    }

    //val stateSnapshotStream = mapStateDstream.stateSnapshots()  
    //val filteredSnapShotStream = stateSnapshotStream.foreachRDD { (rdd, t) => rdd.filter(r => r._2._3 >= t) }
    //val filteredSnapShotStream = stateSnapshotStream.filter ( (r, t) => r._2._3 >= t )
    //val convertedSnapShotStream = filteredSnapShotStream.map( r => r._1.split(";")(0), r._1.split(";")(1), (r._2._1/r._2._2))

    /*
    stateSnapshotStream.foreachRDD { (rdd, t) =>
       rdd.foreach {

       }
       rdd.saveToCassandra("spark", "sourcedestaiportdepdelay", SomeColumns("source", "dest", "depdelayaverage")) 
    }
    */
    /*
     stateSnapshotStream.foreachRDD { (rdd, t) =>
        // rdd.foreach(r => println(t + ":" + r))
        rdd.filter(r => r._2._3 >= t)
            .foreach(r => ( r._1.split(";")(0), r._1.split(";")(1), (r._2._1/r._2._2)).saveToCassandra("spark", "sourcedestaiportdepdelay", SomeColumns("source", "dest", "depdelayaverage")))
                                 // println("Newly Added / Changed " + t + ":" + r))
    } 
                                 */

    ssc.start()
    ssc.awaitTermination()
  }
}
