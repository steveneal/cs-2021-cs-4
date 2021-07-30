package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {

    private String since;

    public InstrumentLiquidityExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));

        Dataset<Row> volumePastMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)))
                .select(sum("LastQty"));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(liquidityVolume, volumePastMonth.first().get(0));
        return results;
    }
    @Override
    public void setSince(String since) {
        this.since = since;
    }
}


