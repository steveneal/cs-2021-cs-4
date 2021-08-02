package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

/**
 * TotalTradesWithEntityExtractor gets the total number of trades of a instrument with an entity over the
 * past day, week, month, and year. This will be used to assess the health of the relationship with
 * the entity.
 */
public class TotalTradesWithEntityExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    /**
     * extractMetaData returns a map with tradesWithEntityToday, tradesWithEntityPastWeek, tradesWithEntityPastMonth,
     * and tradesWithEntityPastYear
     * @param rfq as Rfq to supply the Isin and entity ID to match
     * @param session as SparkSession
     * @param trades as Dataset<Row> with previous trade data to extract from
     * @return Map<RfqMetadtaFieldNames, Object> with the extacted data
     */
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = getNow().getMillis();
        long pastWeekMs = getNow().minusWeeks(1).getMillis();
        long pastMonthMs = getNow().minusMonths(1).getMillis();
        long pastYearMs = getNow().minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long tradesToday = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(todayMs))).count();
        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs))).count();
        long tradesPastMonth = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs))).count();
        long tradesPastYear = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastYearMs))).count();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradesWithEntityToday, tradesToday);
        results.put(tradesWithEntityPastWeek, tradesPastWeek);
        results.put(tradesWithEntityPastMonth, tradesPastMonth);
        results.put(tradesWithEntityPastYear, tradesPastYear);
        return results;
    }

}
