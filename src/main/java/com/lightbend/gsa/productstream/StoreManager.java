package com.lightbend.gsa.productstream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.HashMap;
import java.util.Map;

//In a real application, I would use Akka Sharding to distribute this work over multiple
//JVMs. With Akka Sharding, there is a ShardRegion actor that automatically acts like a
//router and proxy for the messages. You send the messages to the ShardRegion and the
//work automatically gets sent to worker actors balanced throughout the entire cluster.
//
//In this little demo, however, I didn't want complicate things with distributed computing.
//So I created this Actor that acts a bit like like a ShardRegion actor (automatically creating
//child worker actors and proxying messages to them), but only within a single JVM.
//It's very primitive compared to Akka Sharding, but it is similar enough in functionality
//that you could swap it out for real Akka Sharding fairly easily.

public class StoreManager extends AbstractLoggingActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private Map<String, ActorRef> children = new HashMap<>();

    public static Props props() {
        return Props.create(StoreManager.class, StoreManager::new);
    }

    //Message for requesting current inventory state. Send it to the StoreManager and the StoreManager
    //will automatically route the request to the correct store. The sending Actor will receive
    //an InventoryStatus message in response.
    public static class InventoryCheck {
        private final String storeId;
        private final String productId;

        public InventoryCheck(String storeId, String productId) {
            this.storeId = storeId;
            this.productId = productId;
        }

        public String getProductId() {
            return productId;
        }

        public String getStoreId() {
            return storeId;
        }
    }

    //Reply that will be received after sending InventoryCheck.
    public static class InventoryStatus {
        private final String storeId;
        private final String productId;
        private final int quantity;

        public InventoryStatus(String storeId, String productId, int quantity) {
            this.storeId = storeId;
            this.productId = productId;
            this.quantity = quantity;
        }

        public String getStoreId() {
            return storeId;
        }

        public String getProductId() {
            return productId;
        }

        public int getQuantity() {
            return quantity;
        }
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(
                ProductStreamRecord.class,
                r -> {
                    log.debug("Update store: " + r.toStoreId.orElse("missing id"));
                    if (r.toStoreId.isPresent()) {
                        getChild(r.toStoreId.get()).forward(r, getContext());
                    }
                }
            )
            .match(
                InventoryCheck.class,
                i -> {
                    log.debug("Check: " + i.getStoreId());
                    getChild(i.getStoreId()).forward(i, getContext());
                }
            )
            //Generally we shouldn't be getting "stream complete" messages with something
            //like RabbitMQ, but this comes up in some our test cases.
            .match(
                ProductStream.StreamComplete.class,
                s -> {
                    log.warning("End of stream reached for StoreManager");
                }
            )
            .build();
    }

    private ActorRef getChild(String storeId) {
        ActorRef child = children.get(storeId);
        if (child == null) {
            child = getContext().actorOf(Store.props(), storeId);
            children.put(storeId, child);
        }
        return child;
    }
}
