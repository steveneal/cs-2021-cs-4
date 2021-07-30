package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.averageTradedPrice;
import static org.apache.spark.sql.functions.avg;

public class AverageTradePriceExtractor extends AbstractExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = getNow().getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))  //
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Dataset<Row> avgTradedPrice = filtered.filter(
                trades.col("TradeDate")
                        .$greater(new java.sql.Date(pastWeekMs)))
                .select(avg("LastPx").as("PriceAverage"));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradedPrice, (int)Math.round(avgTradedPrice.first().getDouble(0)));
        return results;
    }
}
