package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.*;
import java.util.Properties;

/**
 * Main class to emulate a Kafka stream. New RFQs can be input here and all extracted data will be
 * published here. Run in tandem with ChatterboxServer.
 */
public class RfqDecoratorMain {

    /** Extractor flags for the RFQ Processor
     *  <ul>
     *      <li>ALL_METADATA: all extractors</li>
     *      <li>VOLUME: total trade volume, volume traded with entity extractors</li>
     *      <li>TOTAL_TRADES: total trades with entity extractor</li>
     *      <li>LIQUIDITY: instrument liquidity extractor</li>
     *      <li>BIAS: trade bias extractor</li>
     *      <li>AVG_PRICE: average instrrument price extractor</li>
     *  </ul>
     *  <p>
     *      Combine flags like so:
     *      <i>"LIQUIDITY | BIAS"</i>
     *      to customize extractor options
     *  </p>
     */
    private static final int PROGRAM_FLAGS = RfqProcessor.ALL_METADATA;

    /**
     * Main method sets up a local spark index then connects to the localhost socket to emulate a
     * Kafka stream
     * @param args as String array
     * @throws Exception if an invalid JSON string is read
     */
    public static void main(String[] args) throws Exception {
        //Location of config.properties file
        String filename = "src/main/resources/config.properties";
        String hadoop_dir = "";

        //Retrieve hadoop_dir from config.properties
        try{
            FileReader reader = new FileReader(filename);
            Properties props = new Properties();
            props.load(reader);

            hadoop_dir = props.getProperty("hadoop_dir");

            System.out.println("hadoop_dir is: " + hadoop_dir);
            reader.close();
        } catch (Exception e){
            e.printStackTrace();
            return;
        }

        System.setProperty("hadoop.home.dir", hadoop_dir);
        System.setProperty("spark.master", "local[4]");

        // Create a Spark configuration and set a sensible app name
         SparkConf conf = new SparkConf().setAppName("RfqStream");
        // Create a Spark streaming context
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        // Create a Spark session
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        // Create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor rfqProcessor = new RfqProcessor(session, context, PROGRAM_FLAGS);
        rfqProcessor.startSocketListener();
    }

}
