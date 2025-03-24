////
//// package QueueConsumer;
////
//// import com.fasterxml.jackson.databind.ObjectMapper;
//// import com.rabbitmq.client.Channel;
//// import com.rabbitmq.client.DeliverCallback;
//// import dto.ConsumerLiftRideDto;
////
//// import java.io.IOException;
//// import java.nio.charset.StandardCharsets;
//// import java.util.List;
//// import java.util.Map;
//// import java.util.concurrent.BlockingQueue;
//// import java.util.concurrent.ConcurrentHashMap;
//// import java.util.concurrent.CopyOnWriteArrayList;
////
//// public class WorkerThread implements Runnable {
////    private final BlockingQueue<Channel> channelPool;
////    private final String queueName;
////    private final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap;
////    private final ObjectMapper objectMapper;
////    private static final int WorkCONSUMEAMOUNT = 1;
////
////    // constructor
////    public WorkerThread(BlockingQueue<Channel> channelPool, String queueName,
////                        ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap,
//// ObjectMapper objectMapper) {
////        this.channelPool = channelPool;
////        this.queueName = queueName;
////        this.objectMapper = objectMapper;
////        this.skierRidesHashMap = skierRidesHashMap;
////    }
////
////    @Override
////    public void run() {
////        try {
////            // Takes a channel from the pool
////            final Channel channel = channelPool.take();
////            channel.basicQos(WorkCONSUMEAMOUNT);
////
////            // Defines a callback for processing messages
////            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
////                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//////                System.out.println("Received: " + message);
////                try {
////                    // Parse and store the message
////                    processMessage(message, channel, delivery.getEnvelope().getDeliveryTag());
////                    // Acknowledge
////                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
////                } catch (Exception e) {
////                    //  failed messages, sending them to a Dead Letter Queue
////                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
////                }
////            };
////
////            // Starts consuming messages
////            channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});
////
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
////    }
////
////    private void processMessage(String message, Channel channel, long deliveryTag) {
////        try {
////            ConsumerLiftRideDto consumerLiftRideDto = objectMapper.readValue(message,
//// ConsumerLiftRideDto.class);
////            skierRidesHashMap.computeIfAbsent(consumerLiftRideDto.getSkierID(), k -> new
//// CopyOnWriteArrayList<>())
////                    .add(consumerLiftRideDto);
////        } catch (Exception e) {
////            e.printStackTrace();
////            try {
////                System.err.println("JSON parse failed, sending message to Dead Letter Queue");
////                channel.basicNack(deliveryTag, false, false);
////            } catch (IOException ex) {
////                ex.printStackTrace();
////            }
////        }
////    }
//// }
//
// package QueueConsumer;
//
// import AWS.DynamoDB_Client;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.rabbitmq.client.Channel;
// import com.rabbitmq.client.DeliverCallback;
// import dto.ConsumerLiftRideDto;
// import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
// import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
//
// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.BlockingQueue;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.CopyOnWriteArrayList;
//
// public class WorkerThread implements Runnable {
//  private final BlockingQueue<Channel> channelPool;
//  private final String queueName;
//  private final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap;
//  private final ObjectMapper objectMapper;
//  private static final int WorkCONSUMEAMOUNT = 5;
//  private final DynamoDbClient dynamoDbClient;
//  private final List<WriteRequest> batchBuffer = new ArrayList<>();
//  private static final int BATCH_SIZE = 25;  // batch write to DynamoDB, 25 per time
//
//  // constructor
//  public WorkerThread(
//      BlockingQueue<Channel> channelPool,
//      String queueName,
//      ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap,
//      ObjectMapper objectMapper) {
//    this.channelPool = channelPool;
//    this.queueName = queueName;
//    this.objectMapper = objectMapper;
//    this.skierRidesHashMap = skierRidesHashMap;
//  }
//
//  @Override
//  public void run() {
//    try {
//      // Takes a channel from the pool
//      final Channel channel = channelPool.take();
//      channel.basicQos(WorkCONSUMEAMOUNT);
//
//      // Defines a callback for processing messages
//      DeliverCallback deliverCallback =
//          (consumerTag, delivery) -> {
//            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//            //                System.out.println("Received: " + message);
//            try {
//              // Parse and store the message
//              processMessage(message, channel, delivery.getEnvelope().getDeliveryTag());
//              // Acknowledge
//              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//            } catch (Exception e) {
//              //  failed messages, sending them to a Dead Letter Queue
//              System.err.println("error message, send to dead message queue");
//              channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
//              // The unique ID of the message being processed， requeue this message.
//            }
//          };
//
//      // Starts consuming messages
//      channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});
//
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  private void processMessage(String message, Channel channel, long deliveryTag) {
//    try {
////      ConsumerLiftRideDto consumerLiftRideDto =
////          objectMapper.readValue(message, ConsumerLiftRideDto.class);
////      skierRidesHashMap
////          .computeIfAbsent(consumerLiftRideDto.getSkierID(), k -> new CopyOnWriteArrayList<>())
////          .add(consumerLiftRideDto);
//
//      // write to DynamoDB per message
//      DynamoDbClient client = DynamoDB_Client.getInstance();
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      try {
//        System.err.println("JSON parse failed, sending message to Dead Letter Queue");
//        channel.basicNack(
//            deliveryTag, false, false); // move it to the Dead Letter Queue (DLQ), do not requeues
//      } catch (IOException ex) {
//        ex.printStackTrace();
//      }
//    } finally {
//      // return channel to pool
//      if (channel != null && !channel.isOpen()) {
//        channelPool.offer(channel);
//      }
//    }
//  }
// }

// package QueueConsumer;
//
// import AWS.DynamoDB_Client;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.rabbitmq.client.Channel;
// import com.rabbitmq.client.DeliverCallback;
// import dto.ConsumerLiftRideDto;
// import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
// import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
// import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
// import software.amazon.awssdk.services.dynamodb.model.PutRequest;
// import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
//
// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
// import java.util.*;
// import java.util.concurrent.BlockingQueue;
//
// public class WorkerThread implements Runnable {
//  private static final int BATCH_SIZE = 25; // write 25 everytime to DynamoDB
//  private final BlockingQueue<Channel> channelPool;
//  private final String queueName;
//  private final ObjectMapper objectMapper;
//  private final DynamoDbClient dynamoDbClient; // DynamoDB client singleton
//  private final int FETCH_AMOUNT = 5;
//  // buffer for batching write to DynamoDB
//  private final List<WriteRequest> batchBuffer = new ArrayList<>();
//  private final String DB_NAME = "SkiLiftRides";
//
//  // timer for time-based flush
//  private long lastFlushTime = System.currentTimeMillis();
//
//  // workerthread constructor
//  public WorkerThread(
//          BlockingQueue<Channel> channelPool, String queueName, ObjectMapper objectMapper) {
//    this.channelPool = channelPool;
//    this.queueName = queueName;
//    this.objectMapper = objectMapper;
//    this.dynamoDbClient = DynamoDB_Client.getInstance();
//  }
//
//  // when the thread start sets up MQ consumer with deliver callback.
//  @Override
//  public void run() {
//    try {
//      final Channel channel = channelPool.take();
//      channel.basicQos(FETCH_AMOUNT);
//
//      // background thread to flush every 3 seconds
//      new Timer(true)
//              .schedule(
//                      new TimerTask() {
//                        @Override
//                        public void run() {
//                          synchronized (batchBuffer) {
//                            if (!batchBuffer.isEmpty()
//                                    && System.currentTimeMillis() - lastFlushTime >= 3_000) {
//                              flushBatch();
//                            }
//                          }
//                        }
//                      },
//                      3_000,
//                      3_000); // schedule every 3 seconds
//
//      DeliverCallback deliverCallback =
//              (consumerId, delivery) -> {
//                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                try {
//                  processMessage(message); // parse the message and add to write buffer
//                  // Acknowledge
//                  channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//                } catch (Exception e) {
//                  //  failed messages, sending them to a Dead Letter Queue
//                  System.err.println("error message, send to dead message queue");
//                  channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
//                }
//              };
//      // Starts consuming messages
//      channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  //
//  private void processMessage(String message) throws Exception {
//    // Deserialize the JSON payload into DTO
//
//    ConsumerLiftRideDto dto = objectMapper.readValue(message, ConsumerLiftRideDto.class);
//
//    // make dto to dynamoDB item format
//    Map<String, AttributeValue> item = new HashMap<>();
//    item.put("skierID", AttributeValue.builder().n(String.valueOf(dto.getSkierID())).build());
//    item.put(
//            "dayID#time", AttributeValue.builder().s(dto.getDayID() + "#" +
// dto.getTime()).build());
//    item.put("resortID", AttributeValue.builder().n(String.valueOf(dto.getResortID())).build());
//    item.put("seasonID", AttributeValue.builder().s(dto.getSeasonID()).build());
//    item.put("liftID", AttributeValue.builder().n(String.valueOf(dto.getLiftID())).build());
//    item.put("time", AttributeValue.builder().n(String.valueOf(dto.getTime())).build());
//    item.put(
//            "dayID#skierID",
//            AttributeValue.builder().s(dto.getDayID() + "#" + dto.getSkierID()).build());
//
//    // Add to batch buffer (synchronized block for thread safety)
//    synchronized (batchBuffer) {
//      batchBuffer.add(
//              WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
//
//      // write the batch to dynamod if full
//      if (batchBuffer.size() >= BATCH_SIZE) {
//        flushBatch();
//      }
//    }
//  }
//
//  private void flushBatch() {
//    try {
//      Map<String, List<WriteRequest>> batchMap = new HashMap<>();
//      batchMap.put(DB_NAME, batchBuffer);
//
//      // send to DynamoDB
//      BatchWriteItemRequest batchRequest =
//              BatchWriteItemRequest.builder().requestItems(batchMap).build();
//
//      Map<String, List<WriteRequest>> unprocessed =
//              dynamoDbClient.batchWriteItem(batchRequest).unprocessedItems();
//
//      int retryCount = 0;
//      while (!unprocessed.isEmpty() && retryCount < 5) {
//        Thread.sleep((long) Math.pow(2, retryCount) * 100L); // simple exponential backoff
//        System.out.println("Retrying unprocessed items, attempt #" + (retryCount + 1));
//
//        BatchWriteItemRequest retryRequest =
//                BatchWriteItemRequest.builder().requestItems(unprocessed).build();
//
//        unprocessed = dynamoDbClient.batchWriteItem(retryRequest).unprocessedItems();
//        retryCount++;
//      }
//
//      if (!unprocessed.isEmpty()) {
//        System.err.println("WARNING: Some items failed after retries and will be lost!");
//      }
//
//      batchBuffer.clear();
//      lastFlushTime = System.currentTimeMillis();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
// }

package QueueConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import dto.ConsumerLiftRideDto;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class WorkerThread implements Runnable {
  private static final int BATCH_SIZE = 200; // batch write size
  private final BlockingQueue<Channel> channelPool;
  private final String queueName;
  private final ObjectMapper objectMapper;
  private final MongoClient mongoClient;
  private final MongoCollection<Document> liftRidesCollection;
  private final List<Document> batchBuffer = new ArrayList<>();
  private final int FETCH_AMOUNT = 200;
  private final String MONGODBUSER = "admin";
  private final String MONGODBPASSWORD = "admin";
  private final String MONGODIP = "172.31.26.91";  //private ip
  private final int timerFlushIntervalMs = 5000;

  public WorkerThread(
      BlockingQueue<Channel> channelPool, String queueName, ObjectMapper objectMapper) {
    this.channelPool = channelPool;
    this.queueName = queueName;
    this.objectMapper = objectMapper;
    // initialize MongoDB client and select database and collection
    String mongoUri =
        "mongodb://" + MONGODBUSER + ":" + MONGODBPASSWORD + "@" + MONGODIP + ":27017";
    this.mongoClient = MongoClients.create(mongoUri);
    MongoDatabase db = mongoClient.getDatabase("skiApp");
    this.liftRidesCollection = db.getCollection("liftRides");

    // Create indexes for optimized queries
    //  For skier N, how many days have they skied this season
    liftRidesCollection.createIndex(new Document("skierID", 1).append("seasonID", 1).append("dayID", 1));
    // For skier N, what are the vertical totals for each ski day
    liftRidesCollection.createIndex(new Document("skierID", 1).append("dayID", 1));
    // For Show lifts they rode on each ski day
    liftRidesCollection.createIndex(new Document("skierID", 1).append("dayID", 1).append("time", 1));
    // For How many unique skiers visited resort X on day N
    liftRidesCollection.createIndex(new Document("resortID", 1).append("dayID", 1).append("skierID", 1));
    // Start timer-based batch flush
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (batchBuffer) {
          if (!batchBuffer.isEmpty()) {
            flushBatch();
          }
        }
      }
    }, this.timerFlushIntervalMs, this.timerFlushIntervalMs);
  }

  /** Sets up RabbitMQ consumer and starts processing messages. */
  @Override
  public void run() {
    try {
      final Channel channel = channelPool.take();
      channel.basicQos(FETCH_AMOUNT);

      // define the callback for message delivery
      DeliverCallback deliverCallback =
          (consumerId, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
              processMessage(message); // Parse and process the message
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
              System.err.println("error message, send to dead message queue");
              channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
          };
      channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void processMessage(String message) throws Exception {
    ConsumerLiftRideDto dto = objectMapper.readValue(message, ConsumerLiftRideDto.class);

    Document doc =
        new Document()
            .append("skierID", dto.getSkierID())
            .append("resortID", dto.getResortID())
            .append("seasonID", dto.getSeasonID())
            .append("dayID", dto.getDayID())
            .append("time", dto.getTime())
            .append("liftID", dto.getLiftID())
            .append("vertical", dto.getLiftID() * 10); // precalculate vertial based on the liftID
    synchronized (batchBuffer) { // Synchronize to prevent race conditions
      batchBuffer.add(doc);
      System.out.println("[Buffer] Added document to buffer. Current size: " + batchBuffer.size());
      if (batchBuffer.size() >= BATCH_SIZE) {
        System.out.println("[Buffer] Buffer reached BATCH_SIZE. Flushing now...");
        flushBatch();
      }
    }
  }

  /** Flushes the batch buffer to MongoDB using unordered bulkWrite.  */
  private void flushBatch() {
    try {
      // Map each Document in batchBuffer to an InsertOneModel (for bulkWrite)
      List<WriteModel<Document>> operations = batchBuffer.stream()
              .map(doc -> new InsertOneModel<>(doc))
              .collect(Collectors.toList());

      // Perform unordered bulkWrite (MongoDB can parallelize internally)
      liftRidesCollection.bulkWrite(operations, new BulkWriteOptions().ordered(false));

      System.out.println("[MongoDB] Bulk inserted batch of size: " + batchBuffer.size());
      batchBuffer.clear();
    } catch (MongoBulkWriteException bwe) {
      System.err.println("[MongoDB] Bulk write completed with errors: " + bwe.getWriteErrors().size() + " errors.");
      bwe.printStackTrace();
      batchBuffer.clear(); //
    } catch (Exception e) {
      System.err.println("[MongoDB] Bulk write failed!");
      e.printStackTrace();
      batchBuffer.clear(); //
    }
  }
}
