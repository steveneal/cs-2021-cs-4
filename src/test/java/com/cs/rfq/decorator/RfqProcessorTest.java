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

public class RfqProcessorTest {

    private final static Logger log = LoggerFactory.getLogger(AbstractSparkUnitTest.class);

    protected static SparkSession session;
    protected static SparkConf conf;

    @BeforeAll
    public static void setupClass() {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");

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
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));

        RfqProcessor process = new RfqProcessor(session, context);
        process.processRfq(rfq);
    }
}
