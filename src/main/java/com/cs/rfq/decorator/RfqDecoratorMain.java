package com.cs.rfq.decorator;

import breeze.linalg.operators.SparseVectorOps$CanZipMapKeyValuesSparseVector$mcIJ$sp;
import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Main class to emulate a Kafka stream. New RFQs can be input here and all extracted data will be
 * published here. Run in tandem with ChatterboxServer.
 */
public class RfqDecoratorMain {

    /**
     * Main method sets up a local spark index then connects to the localhost socket to emulate a
     * Kafka stream
     * @param args as String array
     * @throws Exception if an invalid JSON string is read
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        // Create a Spark configuration and set a sensible app name
         SparkConf conf = new SparkConf().setAppName("RfqStream");
        // Create a Spark streaming context
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        // Create a Spark session
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        // Create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor rfqProcessor = new RfqProcessor(session, context);
        rfqProcessor.startSocketListener();
    }

}
