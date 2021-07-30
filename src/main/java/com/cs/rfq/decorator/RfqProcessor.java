package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader loader = new TradeDataLoader();
        trades = loader.loadTrades(session, "src\\test\\resources\\trades\\trades.json");


        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new TotalVolumeExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //stream data from the input socket on localhost:9000
        JavaDStream<String> jsonData = streamingContext.socketTextStream("localhost", 9000);

        //convert each incoming line to a Rfq object and call processRfq method with it
        JavaDStream<Rfq> rfqObj = jsonData.map(x -> Rfq.fromJson(x));
        rfqObj.foreachRDD(rdd -> {
            rdd.collect().forEach(rfq -> processRfq(rfq));
        });

        //start the streaming context
        streamingContext.start();

        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));
        System.out.println(rfq);

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //Get metadata from each of the extractors
        Map<RfqMetadataFieldNames, Object> map  = new HashMap<RfqMetadataFieldNames, Object>();
        for(RfqMetadataExtractor extractor : extractors) {
            map.putAll(extractor.extractMetaData(rfq, session, trades));
        }

        //Publish the metadata
        publisher.publishMetadata(map);
    }
}
