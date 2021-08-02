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

/**
 * InstrumentLiquidityExtractor reports the volume of an instrument traded by the bank in the Rfq to all other
 * parties in the last month. This will be used later to calculate the liquidity.
 */
public class InstrumentLiquidityExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    /**
     * extractMetaData returns a map with the volume traded in the last month mapped to "liquidityVolume".
     * @param rfq as Rfq to supply the Isin and entity ID to match
     * @param session as SparkSession
     * @param trades as Dataset<Row> with previous trade data to extract from
     * @return Map<RfqMetadtaFieldNames, Object> with the extacted data
     */
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = getNow().getMillis();
        long pastMonthMs = getNow().minusMonths(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));

        Dataset<Row> volumePastMonth = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastMonthMs)))
                .select(sum("LastQty"));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        System.out.println(volumePastMonth.first());
        if(volumePastMonth.first().get(0) != null) {
            results.put(liquidityVolume, volumePastMonth.first().get(0));
        } else {
            results.put(liquidityVolume, 0);
        }
        return results;
    }
}


