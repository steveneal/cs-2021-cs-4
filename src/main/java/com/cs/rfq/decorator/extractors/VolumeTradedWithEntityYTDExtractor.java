package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * VolumeTradedWithEntityYTDExtractor gets the volume of an instrument traded with an entity since the beginning
 * of the year.
 */
public class VolumeTradedWithEntityYTDExtractor extends AbstractExtractor implements RfqMetadataExtractor {

    /** The time to treat as "now" */
    private String since;

    /**
     * Constructs the class with the date of January first of the current year
     */
    public VolumeTradedWithEntityYTDExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    /**
     * extractMetaData returns a map volumeTradedYearToDate.
     * @param rfq as Rfq to supply the Isin and entity ID to match
     * @param session as SparkSession
     * @param trades as Dataset<Row> with previous trade data to extract from
     * @return Map<RfqMetadtaFieldNames, Object> with the extacted data
     */
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT sum(LastQty) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        if (volume == null) {
            volume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeTradedYearToDate, volume);
        return results;
    }

    /**
     * Set the year to start from
     * @param since as String to treat as today
     */
    @Override
    public void setSince(String since){
        this.since = since;
    }


}
