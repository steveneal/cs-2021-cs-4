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

        String filePath = getClass().getResource("trade-bias.json").getPath();
        String noMatchPath = getClass().getResource("volume-traded-1.json").getPath();
        String filePath2 = getClass().getResource("liquiditytest.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        noMatches = new TradeDataLoader().loadTrades(session, noMatchPath);
        trades2 = new TradeDataLoader().loadTrades(session, filePath2);
    }

    @Test
    public void checkPositiveTradeBiasPercentage(){
        TradeBiasExtractor extractor = new TradeBiasExtractor();
        extractor.setSince("2021-05-30");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object monthBias = meta.get(RfqMetadataFieldNames.tradeBiasBuyPercentagePastMonth);

        assertEquals(40, monthBias);
    }
}
