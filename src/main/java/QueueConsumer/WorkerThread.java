//
//package QueueConsumer;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.rabbitmq.client.Channel;
//import com.rabbitmq.client.DeliverCallback;
//import dto.ConsumerLiftRideDto;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CopyOnWriteArrayList;
//
//public class WorkerThread implements Runnable {
//    private final BlockingQueue<Channel> channelPool;
//    private final String queueName;
//    private final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap;
//    private final ObjectMapper objectMapper;
//    private static final int WorkCONSUMEAMOUNT = 1;
//
//    // constructor
//    public WorkerThread(BlockingQueue<Channel> channelPool, String queueName,
//                        ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap, ObjectMapper objectMapper) {
//        this.channelPool = channelPool;
//        this.queueName = queueName;
//        this.objectMapper = objectMapper;
//        this.skierRidesHashMap = skierRidesHashMap;
//    }
//
//    @Override
//    public void run() {
//        try {
//            // Takes a channel from the pool
//            final Channel channel = channelPool.take();
//            channel.basicQos(WorkCONSUMEAMOUNT);
//
//            // Defines a callback for processing messages
//            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
////                System.out.println("Received: " + message);
//                try {
//                    // Parse and store the message
//                    processMessage(message, channel, delivery.getEnvelope().getDeliveryTag());
//                    // Acknowledge
//                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//                } catch (Exception e) {
//                    //  failed messages, sending them to a Dead Letter Queue
//                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
//                }
//            };
//
//            // Starts consuming messages
//            channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void processMessage(String message, Channel channel, long deliveryTag) {
//        try {
//            ConsumerLiftRideDto consumerLiftRideDto = objectMapper.readValue(message, ConsumerLiftRideDto.class);
//            skierRidesHashMap.computeIfAbsent(consumerLiftRideDto.getSkierID(), k -> new CopyOnWriteArrayList<>())
//                    .add(consumerLiftRideDto);
//        } catch (Exception e) {
//            e.printStackTrace();
//            try {
//                System.err.println("JSON parse failed, sending message to Dead Letter Queue");
//                channel.basicNack(deliveryTag, false, false);
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
//    }
//}


package QueueConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import dto.ConsumerLiftRideDto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class WorkerThread implements Runnable {
    private final BlockingQueue<Channel> channelPool;
    private final String queueName;
    private final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap;
    private final ObjectMapper objectMapper;
    private static final int WorkCONSUMEAMOUNT = 1;

    // constructor
    public WorkerThread(BlockingQueue<Channel> channelPool, String queueName,
                        ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap, ObjectMapper objectMapper) {
        this.channelPool = channelPool;
        this.queueName = queueName;
        this.objectMapper = objectMapper;
        this.skierRidesHashMap = skierRidesHashMap;
    }

    @Override
    public void run() {
        try {
            // Takes a channel from the pool
            final Channel channel = channelPool.take();
            channel.basicQos(WorkCONSUMEAMOUNT);

            // Defines a callback for processing messages
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                System.out.println("Received: " + message);
                try {
                    // Parse and store the message
                    processMessage(message, channel, delivery.getEnvelope().getDeliveryTag());
                    // Acknowledge
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    //  failed messages, sending them to a Dead Letter Queue
                    System.err.println("error message, send to dead message queue");
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    // The unique ID of the message being processed， requeue this message.
                }
            };

            // Starts consuming messages
            channel.basicConsume(this.queueName, false, deliverCallback, consumerTag -> {});

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processMessage(String message, Channel channel, long deliveryTag) {
        try {
            ConsumerLiftRideDto consumerLiftRideDto = objectMapper.readValue(message, ConsumerLiftRideDto.class);
            skierRidesHashMap.computeIfAbsent(consumerLiftRideDto.getSkierID(), k -> new CopyOnWriteArrayList<>())
                    .add(consumerLiftRideDto);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                System.err.println("JSON parse failed, sending message to Dead Letter Queue");
                channel.basicNack(deliveryTag, false, false);  //move it to the Dead Letter Queue (DLQ), do not requeues
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } finally {
            //return channel to pool
            if (channel != null && !channel.isOpen()) {
                channelPool.offer(channel);
            }
        }

    }
}
