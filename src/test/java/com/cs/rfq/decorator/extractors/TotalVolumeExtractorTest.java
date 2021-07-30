package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalVolumeExtractorTest extends AbstractSparkUnitTest {
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
    public void checkVolumeWhenAllTradesMatch() {

        TotalVolumeExtractor extractor = new TotalVolumeExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object weeks = meta.get(RfqMetadataFieldNames.qtyLastWeek);
        Object months = meta.get(RfqMetadataFieldNames.qtyLastMonth);
        Object years = meta.get(RfqMetadataFieldNames.qtyLastYear);

        assertEquals(1_350_000L, weeks);
        assertEquals(1_350_000L, months);
        assertEquals(1_350_000L, years);
    }

    @Test
    public void getVolumesForDifferentsMonthsWeeksYears() {

        TotalVolumeExtractor extractor = new TotalVolumeExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades2);

        Object weeks = meta.get(RfqMetadataFieldNames.qtyLastWeek);
        Object months = meta.get(RfqMetadataFieldNames.qtyLastMonth);
        Object years = meta.get(RfqMetadataFieldNames.qtyLastYear);

        assertEquals(500_000L, weeks);
        assertEquals(550_000L, months);
        assertEquals(1_300_000L, years);
    }





}
