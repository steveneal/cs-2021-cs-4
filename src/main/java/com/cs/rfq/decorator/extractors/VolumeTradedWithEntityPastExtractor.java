package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class VolumeTradedWithEntityPastExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = getNow().getMillis();
        long pastWeekMs = getNow().minusWeeks(1).getMillis();
        long pastMonthMs = getNow().minusMonths(1).getMillis();
        long pastYearMs = getNow().minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))  //
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Dataset<Row> dataVolumeTradedPastWeek = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs)))
                .select(sum("LastQty").as("WeekVolume"));

        Dataset<Row> dataVolumeTradedPastMonth = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .select(sum("LastQty").as("MonthVolume"));

        Dataset<Row> dataVolumeTradedPastYear = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastYearMs)))
                .select(sum("LastQty").as("YearVolume"));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeTradedPastWeek, (int)dataVolumeTradedPastWeek.first().getLong(0));
        results.put(volumeTradedPastMonth, (int)dataVolumeTradedPastMonth.first().getLong(0));
        results.put(volumeTradedPastYear, (int)dataVolumeTradedPastYear.first().getLong(0));
        return results;
    }
}

