package com.cs.rfq.decorator;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.*;
import java.util.Properties;
import java.util.stream.Stream;

public class RfqDecoratorMain {

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

        //TODO: create a Spark configuration and set a sensible app name
         SparkConf conf = new SparkConf().setAppName("RfqStream");
        //TODO: create a Spark streaming context
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        //TODO: create a Spark session
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor rfqProcessor = new RfqProcessor(session, context);
        rfqProcessor.startSocketListener();
    }

}
