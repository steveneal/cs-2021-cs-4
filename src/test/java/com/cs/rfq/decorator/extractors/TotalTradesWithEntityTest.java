package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalTradesWithEntityTest extends AbstractSparkUnitTest {
    private Rfq rfq;
    Dataset<Row> trades;
    Dataset<Row> trades2;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("average-traded-1.json").getPath();
        String filePath2 = getClass().getResource("total-volume-testing.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        trades2 = new TradeDataLoader().loadTrades(session, filePath2);
    }

    @Test
    public void checkTotalWhenAllTradesMatch() {

        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object days = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);
        Object weeks = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object years = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(5L, days);
        assertEquals(5L, weeks);
        assertEquals(5L, years);
    }

    @Test
    public void testVariousMonthsAndEntities() {

        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades2);

        Object days = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);
        Object weeks = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object years = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(1L, days);
        assertEquals(2L, weeks);
        assertEquals(4L, years);
    }

    @Test
    public void testForNoMatches() {

        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        extractor.setSince("2023-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades2);

        Object days = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);
        Object weeks = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object years = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(0L, days);
        assertEquals(0L, weeks);
        assertEquals(0L, years);
    }

}
