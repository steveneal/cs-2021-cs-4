package com.cs.rfq.decorator.extractors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Properties;

public class AbstractSparkUnitTest {

    private final static Logger log = LoggerFactory.getLogger(AbstractSparkUnitTest.class);

    protected static SparkSession session;

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
        //System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");

        SparkConf conf = new SparkConf()
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

}
