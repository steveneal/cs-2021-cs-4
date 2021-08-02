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

/**
 * AverageTradePriceExtractor gets the average trade price with the entity and the object requested in
 * the Rfq over the last week.
 */
public class AverageTradePriceExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    /**
     * extractMetaData returns a map with the average traded price mapped to "averageTradedPrice".
     * @param rfq as Rfq to supply the Isin and entity ID to match
     * @param session as SparkSession
     * @param trades as Dataset<Row> with previous trade data to extract from
     * @return Map<RfqMetadtaFieldNames, Object> with the extacted data
     */
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = getNow().getMillis();
        long pastWeekMs = getNow().minusWeeks(1).getMillis();

        // Filter the historical trades to only include the desired entity and instrument
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        // Get a Dataset that contains the average of LastPx filtered over the last week
        Dataset<Row> avgTradedPrice = filtered.filter(
                trades.col("TradeDate")
                        .$greater$eq(new java.sql.Date(pastWeekMs)))
                .select(avg("LastPx").as("PriceAverage"));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        try {
            results.put(averageTradedPrice, (int) Math.round(avgTradedPrice.first().getDouble(0)));
        } catch(NullPointerException e) {
            results.put(averageTradedPrice, 0);
        }
        return results;
    }
}
