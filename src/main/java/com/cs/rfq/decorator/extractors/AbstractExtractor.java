package com.cs.rfq.decorator.extractors;

import org.joda.time.DateTime;

public abstract class AbstractExtractor {

    private DateTime now;

    public AbstractExtractor() {
        now = DateTime.now().withMillisOfDay(0);

    }

    public void setSince(String since) {
        now = DateTime.parse(since).withMillisOfDay(0);
    }

    public DateTime getNow() {
        return now;
    }
}
