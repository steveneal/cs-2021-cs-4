package com.cs.rfq.decorator.extractors;

import org.joda.time.DateTime;

public abstract class AbstractExtractor {

    private DateTime now;

    public AbstractExtractor() {
        now = DateTime.now().withMillisOfDay(0);

    }

    public void setSince(String since) {
        System.out.println("old millis: " + now.getMillis());
        now = DateTime.parse(since).withMillisOfDay(0);
        System.out.println("new millis: " + now.getMillis());
    }

    public DateTime getNow() {
        return now;
    }
}
