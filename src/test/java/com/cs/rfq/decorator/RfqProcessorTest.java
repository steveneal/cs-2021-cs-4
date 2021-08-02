package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.AbstractSparkUnitTest;
import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Properties;

public class RfqProcessorTest {

    private final static Logger log = LoggerFactory.getLogger(AbstractSparkUnitTest.class);

    protected static SparkSession session;
    protected static SparkConf conf;

    @BeforeAll
    public static void setupClass() {
        //Location of config.properties file
        String filename = "src/main/resources/config.properties";
        String hadoop_dir = "";

        //Retrieve hadoop_dir from config.properties
        try{
            FileReader reader = new FileReader(filename);
            Properties props = new Properties();
            props.load(reader);

            hadoop_dir = props.getProperty("hadoop_dir");

            //System.out.println("hadoop_dir is: " + hadoop_dir);
            reader.close();
        } catch (Exception e){
            e.printStackTrace();
            return;
        }

        System.setProperty("hadoop.home.dir", hadoop_dir);

        conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkUnitTest");

        session = SparkSession.builder().config(conf).getOrCreate();

        log.info("Spark setup complete");
    }

    @AfterAll
    public static void teardownClass() {
        session.stop();
        log.info("Spark teardown complete");

    }

    @Test
    public void testProcessRfq() {
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
        System.out.println(validRfqJson);
        Rfq rfq = Rfq.fromJson(validRfqJson);
        RfqProcessor process = new RfqProcessor(session, null);

        process.processRfq(rfq);
    }
}
