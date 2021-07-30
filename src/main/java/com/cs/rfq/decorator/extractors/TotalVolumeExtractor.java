package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import static org.apache.spark.sql.functions.*;


import java.util.HashMap;
import java.util.Map;

public class TotalVolumeExtractor  implements RfqMetadataExtractor {

    private String since;

    public TotalVolumeExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
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
        results.put(RfqMetadataFieldNames.qtyLastWeek, tradesPastWeek.first().get(0));
        results.put(RfqMetadataFieldNames.qtyLastMonth, tradesPastMonth.first().get(0));
        results.put(RfqMetadataFieldNames.qtyLastYear, tradesPastYear.first().get(0));
        return results;
    }

    @Override
    public void setSince(String since) {
        this.since = since;
    }

}
