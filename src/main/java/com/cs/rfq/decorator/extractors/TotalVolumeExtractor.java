package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import static org.apache.spark.sql.functions.*;


import java.util.HashMap;
import java.util.Map;

public class TotalVolumeExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = getNow().getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));


        Dataset<Row> tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)))
                .select(sum("LastQty"));
        Dataset<Row> tradesPastMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)))
                .select(sum("LastQty"));
        Dataset<Row> tradesPastYear = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs)))
                .select(sum("LastQty"));

        System.out.println(tradesPastWeek.first());
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        if(tradesPastWeek.first().get(0) != null) {
            results.put(RfqMetadataFieldNames.qtyLastWeek, tradesPastWeek.first().get(0));
        } else {
            results.put(RfqMetadataFieldNames.qtyLastWeek, 0);
        }
        if(tradesPastMonth.first().get(0) != null) {
            results.put(RfqMetadataFieldNames.qtyLastMonth, tradesPastMonth.first().get(0));
        } else {
            results.put(RfqMetadataFieldNames.qtyLastMonth, 0);
        }
        if(tradesPastYear.first().get(0) != null) {
            results.put(RfqMetadataFieldNames.qtyLastYear, tradesPastYear.first().get(0));
        } else {
            results.put(RfqMetadataFieldNames.qtyLastYear, 0);
        }

        return results;
    }
}
