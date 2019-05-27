package com.lightbend.gsa.productstream;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


//Our "main" class. You could easily expand this with some additional shapes, but right now it
//will listen to the RabbitMQ source we setup in ProductStream, listen indefinitely, and route
//the messages into our Akka Actors for aggregation.
public class ProductStreaming {

    public static void main(String[] args) {

        final Config config = ConfigFactory.load();
        final ActorSystem system = ActorSystem.create("ProductStreaming");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ProductStream recs = new ProductStream();

        ActorRef storeManager = system.actorOf(StoreManager.props(), "StoreManager");

        //This stream listens indefinitely and does not return any result.
        recs.getRabbitSource()
            // uses our prebuilt flow logic to parse/filter/process the elements
            .via(recs.getParserFlow())
            // sends the resulting records to our StoreManager actor (which will distribute them to the actual children
            // actors. We are using ACKs so that backpressure is maintained: we won't pull messages faster than
            // we can consume them.
            .runWith(recs.getActorSink(storeManager), mat);
    }
}
