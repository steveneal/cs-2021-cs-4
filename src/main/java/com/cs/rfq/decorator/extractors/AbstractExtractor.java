package com.cs.rfq.decorator.extractors;

import org.joda.time.DateTime;

/**
 * AbstractExtractor class implements functionality to allow extractors to change their "now" date for testing.
 * This is to allow testing of functions that filter based on the week, month, etc to be more consitent.
 */
public abstract class AbstractExtractor {

    /** now is a DateTime object that defualts to the current date. */
    private DateTime now;

    /**
     * Constructor sets now to the current date with no seconds passed. So at date 00:00:00 :
     */
    public AbstractExtractor() {
        now = DateTime.now().withMillisOfDay(0);
    }

    /**
     * setSince sets the now field to the DateTime from a given string. Acceptable values are of the form
     * YYYY-MM-DD and YYYY-MM-DDTHH:MM:SS
     * @param since as String to treat as today
     */
    public void setSince(String since) {
        now = DateTime.parse(since).withMillisOfDay(0);
    }

    /**
     * getNow returns the DateTime associated with now
     * @return now as DateTime
     */
    public DateTime getNow() {
        return now;
    }
}
