package com.learning.zmart.app.joiners;

import com.learning.zmart.app.model.CorrelatedPurchase;
import com.learning.zmart.app.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase value1, Purchase value2) {
        CorrelatedPurchase.Builder builder = CorrelatedPurchase.newBuilder();

        //handles null purchase in case of an outer join
        Date purchaseDate = value1!=null?value1.getPurchaseDate():null;
        Double price = value1!=null?value1.getPrice():0.0;
        String itemPurchased = value1!=null?value1.getItemPurchased():null;

        //handles null purchase in case of an left outer join
        Date otherPurchaseDate = value2!=null?value2.getPurchaseDate():null;
        Double otherPrice = value1!=null?value2.getPrice():0.0;
        String otherItemPurchased = value2!=null?value2.getItemPurchased():null;

        //constructing the return object
        List<String> purchasedItems = new ArrayList<>();
        if(itemPurchased != null) {
            purchasedItems.add(itemPurchased);
        }

        if(otherItemPurchased != null){
            purchasedItems.add(otherItemPurchased);
        }

        String customerId = value1 != null ? value1.getCustomerId() : null;
        String otherCustomerId = value2 != null ? value2.getCustomerId() : null;

        builder.withCustomerId(customerId != null ? customerId : otherCustomerId)
                .withFirstPurchaseDate(purchaseDate)
                .withSecondPurchaseDate(otherPurchaseDate)
                .withItemsPurchased(purchasedItems)
                .withTotalAmount(price + otherPrice);
        return builder.build();
    }
}
