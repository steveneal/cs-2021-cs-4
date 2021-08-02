package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Class for publishing the extracted data
 */
public class MetadataJsonLogPublisher implements MetadataPublisher {

    /** Logger to log the data to */
    private static final Logger log = LoggerFactory.getLogger(MetadataJsonLogPublisher.class);

    /**
     * publishMetadata takes in an map and publishes the data to the log.
     * @param metadata as Map<RfqMetadataFieldNames, Object>
     */
    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
        log.info(String.format("Publishing metadata:%n%s", s));
    }
}
