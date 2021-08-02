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

public class TradeBiasExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = getNow().getMillis();
        long pastWeekMs = getNow().minusWeeks(1).getMillis();
        long pastMonthMs = getNow().minusMonths(1).getMillis();

        //Dataset of trades for this instrument between us and the entity
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long weekBuys = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs)))
                .filter(trades.col("Side").$less(2)).count();

        long weekSells = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs)))
                .filter(trades.col("Side").$greater(1)).count();

        long monthBuys = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("Side").$less(2)).count();

        long monthSells = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("Side").$greater(1)).count();

        //System.out.println("weekBuys: " + weekBuys + " weekSells: " + weekSells + " monthBuys: " + monthBuys + " monthSells: " + monthSells);

        double a = (double)weekBuys / ((double)weekBuys + (double)weekSells) * 100L;
        double b = (double)monthBuys / ((double)monthBuys + (double)monthSells) * 100L;
//System.out.println(a + " ... months: " + b);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeBiasBuyPercentagePastWeek, (int)Math.round(a));
        results.put(tradeBiasBuyPercentagePastMonth, (int)Math.round(b));
        return results;
    }

}

