package com.lightbend.gsa.productstream;

import static org.junit.Assert.assertEquals;

import akka.NotUsed;
import akka.actor.PoisonPill;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;


public class ProductStreamingTest {
    static ActorSystem system;
    static Materializer mat;
    ProductStream recs = new ProductStream();

    //This simple sink counts the elements the received. The contents of the elements is ignored.
    final Sink<String, CompletionStage<Integer>> stringCounter = Sink.fold(0,(Integer i, String s) -> i+1);
    final Sink<ProductStreamRecord, CompletionStage<Integer>> recordCounter = Sink.fold(0,(Integer i, ProductStreamRecord s) -> i+1);

    private static ObjectMapper mapper = new ObjectMapper();
    private static ProductStreamRecord goodRecord1;
    private static ProductStreamRecord goodRecord2;
    private static ProductStreamRecord goodRecord3;
    private static ProductStreamRecord badRecord1;

    private static Duration replyTimeout = Duration.ofSeconds(3);

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        mat = ActorMaterializer.create(system);
        mapper.registerModule(new Jdk8Module());

        goodRecord1 = new ProductStreamRecord();
        goodRecord1.productId = "1234";
        goodRecord1.recordType = "DELIVER";
        goodRecord1.quantity = Optional.of(500);
        goodRecord1.toStoreId = Optional.of("2345");

        goodRecord2 = new ProductStreamRecord();
        goodRecord2.productId = "1234";
        goodRecord2.recordType = "SELL";
        goodRecord2.quantity = Optional.of(100);
        goodRecord2.toStoreId = Optional.of("2345");

        goodRecord3 = new ProductStreamRecord();
        goodRecord3.productId = "1234";
        goodRecord3.recordType = "DELIVER";
        goodRecord3.quantity = Optional.of(500);
        goodRecord3.toStoreId = Optional.of("2346");

        badRecord1 = new ProductStreamRecord();
        badRecord1.productId = "1234";
        badRecord1.recordType = "REQUEST";
        badRecord1.quantity = Optional.of(500);
        badRecord1.toStoreId = Optional.of("2345");
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    //Removing this test while I'm disconnected (and while I have URI removed for security reasons.
    public void rabbitMQConnectivity() throws Exception {
        /*
        final Source<String, NotUsed> sourceUnderTest = recs.getRabbitSource();

        final Sink rabbitConsumer = recs.getRabbitSink();

        //Attach a counter to our test source, we use a take(3) so that it will complete after 3 elements.
        final CompletionStage<Integer> resultCountFuture = sourceUnderTest.take(3).runWith(stringCounter,mat);
        //Then send four elements. The take three means the last should be ignored.

        Source.from(Arrays.asList("one","two","three","four")).map(ByteString::fromString).runWith(rabbitConsumer,mat);

        //Make sure that we receive timely records. (If there are less than three, we are probably competing with
        //another reader. If there are more than three something is wrong with the take(3) in the test.

        final Integer resultCount = resultCountFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);
        assert(resultCount == 3);
        */
    }

    @Test
    public void ignoreNonRecords() throws Exception {
        final Flow<String,ProductStreamRecord, NotUsed> flowUnderTest = recs.getParserFlow();
        //Send three elements, only one of which is valid
        final Source<String,NotUsed> mixedSource = Source.from(Arrays.asList("one",mapper.writeValueAsString(goodRecord1),"three"));

        final CompletionStage<Integer> resultCountFuture = mixedSource.via(flowUnderTest).runWith(recordCounter,mat);
        final Integer resultCount = resultCountFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

        //Make sure that only the valid record is counted
        System.out.println(resultCount);
        assert(resultCount == 1);
    }

    @Test
    public void ignoreRequestRecords() throws Exception {
        final Flow<String,ProductStreamRecord, NotUsed> flowUnderTest = recs.getParserFlow();
        //Send three elements, only one of which is valid
        final Source<String,NotUsed> mixedSource = Source.from(Arrays.asList(
            mapper.writeValueAsString(badRecord1),
            mapper.writeValueAsString(goodRecord1),
            mapper.writeValueAsString(goodRecord2)));

        final CompletionStage<Integer> resultCountFuture = mixedSource.via(flowUnderTest).runWith(recordCounter,mat);
        final Integer resultCount = resultCountFuture.toCompletableFuture().get(3, TimeUnit.SECONDS);

        //Make sure that only the valid record is counted
        System.out.println(resultCount);
        assert(resultCount == 2);
    }
    @Test
    public void messagesAreSentToStoreManager() throws Exception {
        final Flow<String,ProductStreamRecord, NotUsed> flowUnderTest = recs.getParserFlow();
        //Send three elements, only one of which is valid
        final Source<String,NotUsed> mixedSource = Source.from(Arrays.asList(
            mapper.writeValueAsString(badRecord1),
            mapper.writeValueAsString(goodRecord1),
            mapper.writeValueAsString(goodRecord2)));

        //The two good records should be delivered to our test actor, but not the first badRecord1
        final TestKit probe = new TestKit(system);
        NotUsed consumeResult = mixedSource.via(flowUnderTest).runWith(Sink.actorRef(probe.getRef(), PoisonPill.getInstance()),mat);
        final ProductStreamRecord myrec = probe.expectMsgClass(replyTimeout,ProductStreamRecord.class);
        assert(myrec.recordType.equals("DELIVER"));
        final ProductStreamRecord myrec2 = probe.expectMsgClass(replyTimeout,ProductStreamRecord.class);
        assert(myrec2.recordType.equals("SELL"));
    }

    @Test
    public void messagesAreSentToChild() throws Exception {
        final Flow<String,ProductStreamRecord, NotUsed> flowUnderTest = recs.getParserFlow();
        //Send three elements, only one of which is valid
        final Source<String,NotUsed> mixedSource = Source.from(Arrays.asList(
            mapper.writeValueAsString(badRecord1),
            mapper.writeValueAsString(goodRecord1),
            mapper.writeValueAsString(goodRecord2),
            mapper.writeValueAsString(goodRecord2),
            mapper.writeValueAsString(goodRecord3)
            ));

        //Deliver 500 to store "2345", then sell 100, sell 100 = 300 in inventory
        //Deliver 500 ,                         = 500 in inventory
        ActorRef storeManager = system.actorOf(StoreManager.props(),"StoreManagerTest");
//        NotUsed consumeResult = mixedSource.via(flowUnderTest).runWith(akka.stream.scaladsl.Sink.
//            actorRef(storeManager, new ProductStream.StreamComplete()),mat);
        NotUsed notUsed = mixedSource.via(flowUnderTest).runWith(recs.getActorSink(storeManager),mat);

        final TestKit probe = new TestKit(system);

        //Verify store #2345's count (3 xtns , 500-100-100)
        final StoreManager.InventoryCheck check1 = new StoreManager.InventoryCheck(goodRecord1.toStoreId.get(),goodRecord1.productId);
        storeManager.tell(check1,probe.getTestActor());
        StoreManager.InventoryStatus result1 = probe.expectMsgClass(replyTimeout,StoreManager.InventoryStatus.class);
        assert(result1.getQuantity()==300);
        assert(result1.getStoreId())==goodRecord1.toStoreId.get();
        assert(result1.getProductId()==goodRecord1.productId);

        //Verify store #2346's count (1 xtn, 500)
        final StoreManager.InventoryCheck check2 = new StoreManager.InventoryCheck(goodRecord3.toStoreId.get(),goodRecord3.productId);
        storeManager.tell(check2,probe.getTestActor());
        StoreManager.InventoryStatus result2 = probe.expectMsgClass(replyTimeout,StoreManager.InventoryStatus.class);
        assert(result2.getQuantity()==500);

        //Verify that non-existing products give quantity zero
        final StoreManager.InventoryCheck check3 = new StoreManager.InventoryCheck(goodRecord3.toStoreId.get(),"1238");
        storeManager.tell(check3,probe.getTestActor());
        StoreManager.InventoryStatus result3 = probe.expectMsgClass(replyTimeout,StoreManager.InventoryStatus.class);
        assert(result3.getQuantity()==0);

        //Verify that new stores are automatically created as needed
        final StoreManager.InventoryCheck check4 = new StoreManager.InventoryCheck("5555","1238");
        storeManager.tell(check4,probe.getTestActor());
        StoreManager.InventoryStatus result4 = probe.expectMsgClass(replyTimeout,StoreManager.InventoryStatus.class);
        assert(result4.getQuantity()==0);


    }


}
