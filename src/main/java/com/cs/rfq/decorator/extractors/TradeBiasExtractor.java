package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TradeBiasExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long pastWeekMs = getNow().minusWeeks(1).getMillis();
        long pastMonthMs = getNow().minusMonths(1).getMillis();

        //Dataset of trades for this instrument between us and the entity
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityID").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        //SQL function to get the number of buys in the past week
        long weekBuys = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs)))
                .filter(trades.col("Side").$less(2)).count();

        //Number of sells in the past week
        long weekSells = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs)))
                .filter(trades.col("Side").$greater(1)).count();

        //Number of buys in the past month
        long monthBuys = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("Side").$less(2)).count();

        //Number of sells in the past month
        long monthSells = filtered.filter(
                trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .filter(trades.col("Side").$greater(1)).count();


        //Perform double-casting arithmetic to avoid weird Java rounding
        //Resultant is a double in range 0-100: percentage of trades in the period that were buy-side
        double weekTradeBias = (double)weekBuys / ((double)weekBuys + (double)weekSells) * 100L;
        double monthTradeBias = (double)monthBuys / ((double)monthBuys + (double)monthSells) * 100L;

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeBiasBuyPercentagePastWeek, (int)Math.round(weekTradeBias));
        results.put(tradeBiasBuyPercentagePastMonth, (int)Math.round(monthTradeBias));
        return results;
    }

}

