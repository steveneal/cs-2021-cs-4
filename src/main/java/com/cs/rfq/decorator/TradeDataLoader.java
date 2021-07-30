package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import jdk.nashorn.internal.runtime.regexp.joni.encoding.CharacterType;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //Create an explicit schema for the trade data in the JSON files
        StructType schema =  new StructType(new StructField[] {
                new StructField("TraderId", LongType, false, Metadata.empty()),
                new StructField("EntityId", LongType, false, Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty()),
                new StructField("Side", IntegerType, false, Metadata.empty())
        });

        //load the trades dataset
        Dataset<Row> trades = session.read().schema(schema).json(path);

        //log a message indicating number of records loaded and the schema used
        System.out.println(String.format("Number of records loaded: %d", trades.count()));
        trades.printSchema();

        return trades;
    }

}
