package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TradeBiasUnitTest extends AbstractSparkUnitTest{
    private Rfq rfq;
    Dataset<Row> trades;
    Dataset<Row> trades2;
    Dataset<Row> noMatches;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("average-traded-1.json").getPath();
        String noMatchPath = getClass().getResource("volume-traded-1.json").getPath();
        String filePath2 = getClass().getResource("liquiditytest.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        noMatches = new TradeDataLoader().loadTrades(session, noMatchPath);
        trades2 = new TradeDataLoader().loadTrades(session, filePath2);
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object months = meta.get(RfqMetadataFieldNames.liquidityVolume);

        assertEquals(1_350_000L, months);
    }

    @Test
    public void getVolumesForDifferentsMonthsWeeksYears() {

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades2);

        Object months = meta.get(RfqMetadataFieldNames.liquidityVolume);

        assertEquals(950_000L, months);
    }

    @Test
    public void checkNoMatchesonTrades() {
        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setSince("2021-07-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, noMatches);

        Object result = meta.get(RfqMetadataFieldNames.liquidityVolume);

        assertEquals(0, (int) result);
    }
}
