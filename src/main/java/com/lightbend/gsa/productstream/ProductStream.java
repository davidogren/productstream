package com.lightbend.gsa.productstream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.stream.Attributes;
import akka.stream.alpakka.amqp.*;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

//This class contains all of our helper functions around running the streams.
//It has getter methods to return various stream components, utility message
//types, Jackson JSON mapping utilities, and our RabbitMQ connectivity.

public class ProductStream {

    public ProductStream() {
        mapper.registerModule(new Jdk8Module());
    }

    public static class StreamComplete { }

    //Stream shapes we expose for use
    final public Source<String, NotUsed> getRabbitSource() {
        return rabbitSource;
    }

    final public Sink<ByteString, CompletionStage<Done>> getRabbitSink() {
        return rabbitSink;
    }

    final public Sink<String, CompletionStage<Done>> getPrintSink() {
        return printSink;
    }

    final public Flow<String,ProductStreamRecord, NotUsed> getParserFlow() {
        return recordParserFlow;
    }

    final public Sink<ProductStreamRecord,NotUsed> getActorSink(ActorRef actor) {
        return Sink.actorRef(actor, new ProductStream.StreamComplete());
    }

    //RabbitMQ constants
    final private String rabbitQueue = "newq";

    //This is a free RabbitMQ online host I used for building this demo. Please don't overload my free plan.
    final private String rabbitUrl = "SECRET";
    final private int rabbitBufferSize = 10;

    //Akka Streams Constants
    final private int parallelism = 4;

    //Internal values
    private ObjectMapper mapper = new ObjectMapper();

    final private QueueDeclaration rabbitDeclaration = QueueDeclaration.create(rabbitQueue);
    final AmqpConnectionProvider connProvider = AmqpUriConnectionProvider.create(rabbitUrl);
    final Attributes logAttributes = Attributes.createLogLevels(Attributes.logLevelInfo());

    //Internal shapes

    //- Prebuilt Alpakka adapter for RabbitMQ/AMQP that wil produce one element for every message received on the queue
    //  combined with a simple operator to convert the raw bytes into a unicode string. (We ignore the envelope.)
    //  We could easily configure it for a pub/sub mode, if that is desired, but for this simple demo a named queue
    //  was the most straightforward to explain.
    final private Source<String, NotUsed> rabbitSource = AmqpSource.atMostOnceSource(
        NamedQueueSourceSettings.create(connProvider, rabbitQueue)
            .withDeclaration(rabbitDeclaration)
            .withAckRequired(false),
        //- We  use a map operator to transform the type received from the queue (ReadResult, essentially a
        //  wrapper object around the bytes of the message, into a Unicode string.
        rabbitBufferSize).map((ReadResult r) -> r.bytes().utf8String());

    //Unmodified raw Sink for placing messages in our RabbitMQ queue: used by our tests
    final private Sink<ByteString, CompletionStage<Done>> rabbitSink = AmqpSink.createSimple(
        AmqpWriteSettings.create(connProvider).withRoutingKey(rabbitQueue).withDeclaration(rabbitDeclaration));

    //Sink that prints to stdout. Sometimes useful for debugging
    final private Sink<String, CompletionStage<Done>> printSink = Sink.foreach(s -> System.out.println(s));

    //Source that actually includes our business logic for parsing the RabbitMQ messages
    final private Flow<String,ProductStreamRecord, NotUsed> recordParserFlow =
        Flow.of(String.class)
            //- If you want to debug your stream, uncomment this line and all messages received will be logged
            //  via SFLJ at INFO
            .log("parsedRabbitSource").addAttributes(logAttributes)
            //- We then use a mapAsynUnordered operator to transform the string into a structured object. Doing this
            //  async is probably overkill for simple parsing like this, I'm merely using it to demonstrate that we can
            //  include parallelism and/or unordered processing as part of a stream.
            .map((String s) -> parseRecord(s))
            //.mapAsyncUnordered(parallelism, ((String s) -> CompletableFuture.completedFuture(parseRecord(s))))
            //-We then filter the stream to only the messages we care about. For this demo, let's say that we
            // only care about records that parse and that are of types "DELIVER", "RETURN", or "SELL".
            // This may be unnecessary, but it's just another example of something interesting that can be done.
            .filter(o -> o.isPresent() && includeRecordType(o.get().recordType))
            // Remove optional wrapper, all elements are now present filter has removed the ones not present.
            .map(r -> r.get());


    //Helper methods
    private Optional<ProductStreamRecord> parseRecord(String json) {
        try {
            ProductStreamRecord rec = mapper.readValue(json, ProductStreamRecord.class);
            return Optional.of(rec);
            // This includes the parsing exception types so if it doesn't parse for any
            // reason we should get an Optional.empty
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private boolean includeRecordType(String recordType) {
        switch (recordType) {
            case "DELIVER":
            case "RETURN":
            case "SELL":
                return true;
            case "REQUEST":
                return false;
            default:
                return false;
        }
    }
}
