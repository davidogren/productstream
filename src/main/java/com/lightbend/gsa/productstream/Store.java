package com.lightbend.gsa.productstream;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.event.*;

import java.util.HashMap;
import java.util.Map;

//Store is our worker actor. There will be one Store actor per store.
//The store actor has one primary piece of state that it is keeping: the inventory.
//
// Since Store only communicates with the outside via messages (and the createReceive() handlers)
//that state is inherently thread-safe.
//
// When Store gets messages like "InventoryCheck" it can reply asynchronously in the same way
// that it is receiving messages. This non-blocking communication is very efficient and is why
// we can handle so many Actors within a single JVM and with a small number of threads that
// are shared amongst the entire actor pool.
public class Store extends AbstractLoggingActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(Store.class, Store::new);
    }

    private Map<String, Integer> storeInventory = new HashMap();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(
                //These messages update the state
                //"DELIVER" records add to inventory, other records remove inventory
                ProductStreamRecord.class,
                r -> {
                    //Deliver = add to inventory, or record types subtract, if not present no change
                    Integer quantity = (r.recordType.equals("DELIVER")) ? r.quantity.orElse(0) : -r.quantity.orElse(0);
                    if (quantity != 0) {
                        storeInventory.put(r.productId, quantity + storeInventory.getOrDefault(r.productId, 0));
                    }
                }
            )
            .match(
                //This message is how outside services could communicate to request the aggregate inventory state
                StoreManager.InventoryCheck.class,
                i -> {
                    int quantity = storeInventory.getOrDefault(i.getProductId(), 0);
                    getSender().tell(new StoreManager.InventoryStatus(i.getStoreId(), i.getProductId(), quantity), getSelf());
                }
            )
            .build();
    }

}