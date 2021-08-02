package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jdk.nashorn.internal.parser.JSONParser;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Rfq class holds the info associated with a Request For Quote
 */
public class Rfq implements Serializable {
    /** Unique identifier for this RFQ */
    private String id;
    /** An ID for the asset being traded also seen as ISIN */
    private String instrumentId;
    /** The bank's ID for this trader */
    private Long traderId;
    /** The bank's ID for this legal entity */
    private Long entityId;
    /** The quantity to trade */
    private Long qty;
    /** The price at which to trade */
    private Double price;
    /** 'B' for buy, 'S' for sell */
    private String side;

    /**
     * static method to create an Rfq object from a line read from a JSON file.
     * @param json as String
     * @return Rfq with the approprate field values read from the string.
     */
    public static Rfq fromJson(String json) {

        /*
        String validRfqJson = "{" +
                "'id': '123ABC', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 5561279226039690843, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'B' " +
                "}";
         */
        //Create RFQ object
        Gson gson = new Gson();
        return gson.fromJson(json, Rfq.class);
    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + instrumentId + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + qty +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    /**
     * Check if the RFQ wants to buy.
     * @return boolean, true if this.side equals "B"
     */
    public boolean isBuySide() {
        return "B".equals(side);
    }

    /**
     * Check if the RFQ wants to sell.
     * @return boolean, true if this.side equals "S"
     */
    public boolean isSellSide() {
        return "S".equals(side);
    }

    /**
     * Getter for ID
     * @return String as ID
     */
    public String getId() {
        return id;
    }

    /**
     * Setter for ID
     * @param id as String to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Getter for Isin
     * @return String as Isin
     */
    public String getIsin() {
        return instrumentId;
    }

    /**
     * Setter for Isin
     * @param isin as String
     */
    public void setIsin(String isin) {
        instrumentId = isin;
    }

    /**
     * Getter for traderId
     * @return Long as traderId
     */
    public Long getTraderId() {
        return traderId;
    }

    /**
     * Setter for traderId
     * @param traderId as Long
     */
    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    /**
     * Getter for entityId
     * @return Long as entityId
     */
    public Long getEntityId() {
        return entityId;
    }

    /**
     * Setter for entityId
     * @param entityId as Long
     */
    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    /**
     * Getter for Quantity
     * @return Long as qty
     */
    public Long getQuantity() {
        return qty;
    }

    /**
     * Setter for quantity
     * @param quantity as Long
     */
    public void setQuantity(Long quantity) {
        qty = quantity;
    }

    /**
     * Getter for price
     * @return Double as price
     */
    public Double getPrice() {
        return price;
    }

    /**
     * Setter for Price
     * @param price as Double
     */
    public void setPrice(Double price) {
        this.price = price;
    }

    /**
     * Getter for side
     * @return side as String
     */
    public String getSide() {
        return side;
    }

    /**
     * setter for side
     * @param side as String
     */
    public void setSide(String side) {
        this.side = side;
    }
}
