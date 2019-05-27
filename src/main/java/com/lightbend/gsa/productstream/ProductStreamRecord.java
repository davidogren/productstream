package com.lightbend.gsa.productstream;

import java.util.Optional;

//A dummy record for product records. Yours will be much more complex, of course, but I wanted
//to focus on showing the infrastructure and you can fill in the business functionality during
//the hackathon.
//
//This record is designed to be easily parsable by Jackson.
public class ProductStreamRecord {
    //"DELIVER", "RETURN", "SELL", or "REQUEST"
    //DELIVER adds to inventory
    //RETURN,SELL removes from inventory
    //REQUEST is ignored by this application
    public String recordType;
    public String productId;
    public Optional<String> fromStoreId;
    public Optional<String> toStoreId;
    public Optional<Integer> quantity;
}
