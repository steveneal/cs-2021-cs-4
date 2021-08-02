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

/**
 * <h1>Rfq Processor</h1>
 * The RFQ Processor class allows a Spark session to interpolate RFQ messages with
 * relevant metadata info to assist trader decisions.
 */
public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public static final int VOLUME = 1;         // 00001
    public static final int TOTAL_TRADES = 2;   // 00010
    public static final int LIQUIDITY = 4;      // 00100
    public static final int BIAS = 8;           // 01000
    public static final int AVG_PRICE = 16;     // 10000
    public static final int ALL_METADATA = 31;  // 11111



    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext, int flags) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader loader = new TradeDataLoader();
        trades = loader.loadTrades(session, "src\\test\\resources\\trades\\trades.json");

        addExtractors(flags);
    }

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this(session, streamingContext, ALL_METADATA);
    }

    /**
     * List of extractors used by the Rfq Processor.
     * <p>
     * If future extractors are developed, add them to this function.
     * @param flags Which extractors to run for this RFQ Processor.
     */
    private void addExtractors(int flags){
        if ((flags & VOLUME) == VOLUME){
            extractors.add(new VolumeTradedWithEntityYTDExtractor());
            extractors.add(new TotalVolumeExtractor());
        }
        if ((flags & TOTAL_TRADES) == TOTAL_TRADES){
            extractors.add(new TotalTradesWithEntityExtractor());
        }
        if ((flags & LIQUIDITY) == LIQUIDITY){
            extractors.add(new InstrumentLiquidityExtractor());
        }
        if ((flags & BIAS) == BIAS){
            extractors.add(new TradeBiasExtractor());
        }
        if ((flags & AVG_PRICE) == AVG_PRICE){
            extractors.add(new AverageTradePriceExtractor());
        }
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
        System.out.println("Received Rfq: " + rfq);

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
