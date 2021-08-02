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
    Dataset<Row> trades3;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("trade-bias.json").getPath();
        String filePath2 = getClass().getResource("trade-bias-2.json").getPath();
        String filePath3 = getClass().getResource("trade-bias-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        trades2 = new TradeDataLoader().loadTrades(session, filePath2);
        trades3 = new TradeDataLoader().loadTrades(session, filePath3);
    }

    @Test
    public void checkPositiveTradeBiasPercentage(){
        TradeBiasExtractor extractor = new TradeBiasExtractor();
        extractor.setSince("2021-05-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object monthBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastMonth);
        Object weekBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastWeek);

        assertEquals(40, monthBias);
        assertEquals(0, weekBias);
    }

    @Test
    public void check100PercentTradeBias(){
        TradeBiasExtractor extractor = new TradeBiasExtractor();

        extractor.setSince("2021-05-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades2);

        Object monthBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastMonth);
        Object weekBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastWeek);

        assertEquals(100, monthBias);
        assertEquals(0, weekBias);


    }

    @Test
    public void checkPercentageRoundingTradeBias(){
        TradeBiasExtractor extractor = new TradeBiasExtractor();

        extractor.setSince("2021-05-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades3);

        Object monthBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastMonth);
        Object weekBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastWeek);

        assertEquals(33, monthBias);
        assertEquals(67, weekBias);
    }
}
