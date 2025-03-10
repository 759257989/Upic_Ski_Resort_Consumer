//package QueueConsumer;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dto.ConsumerLiftRideDto;
//import com.rabbitmq.client.*;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.*;
//
//public class LiftRideMQConsumer {
//    private static final String QUEUE_NAME = "skiQueue";
//    private static final String DLX_NAME = "skiQueue.dlx";
//    private static final String DLQ_NAME = "skiQueue.DLQ";
//    //    private static final String RMQ_HOST_ADDRESS = "localhost";
//    private static final String RMQ_HOST_ADDRESS = "52.37.128.99";
//    private static final int THREAD_COUNT = 10;
//    private static final String MQ_USERNAME = "admin";
//    private static final String MQ_PASSWORD = "admin";
//
//    // 线程安全的 ConcurrentHashMap 存储 SkierID -> LiftRide 记录
//    private static final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap = new ConcurrentHashMap<>();
//    public static final ObjectMapper objectMapper = new ObjectMapper();
//
//    public static void main(String[] args) {
//        // 确保队列存在
//        setupRabbitMQQueues();
//
//        // 创建线程池
//        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            executorService.submit(new WorkerThread(RMQ_HOST_ADDRESS, QUEUE_NAME, skierRidesHashMap, objectMapper));
//        }
//
//        // 优雅关闭线程池**
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            System.out.println("Shutting down consumer...");
//            executorService.shutdown();
//            try {
//                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
//                    executorService.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                executorService.shutdownNow();
//            }
//            System.out.println("Consumer shutdown complete.");
//        }));
//    }
//
//    private static void setupRabbitMQQueues() {
//        try {
//            ConnectionFactory factory = new ConnectionFactory();
//            factory.setHost(RMQ_HOST_ADDRESS);
//            factory.setUsername(MQ_USERNAME);  // 默认用户名
//            factory.setPassword(MQ_PASSWORD);  // 默认密码
//            Connection connection = factory.newConnection();
//            Channel channel = connection.createChannel();
//
//            // 声明死信交换机（DLX）
//            channel.exchangeDeclare(DLX_NAME, "fanout", true);
//
//            // 声明死信队列（DLQ）
//            channel.queueDeclare(DLQ_NAME, true, false, false, null);
//            channel.queueBind(DLQ_NAME, DLX_NAME, "");
//
//            // 声明正常队列，并绑定 DLX
//            channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of(
//                    "x-dead-letter-exchange", DLX_NAME
//            ));
//
//            channel.close();
//            connection.close();
//            System.out.println("RabbitMQ Queues setup complete.");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.err.println("Failed to set up RabbitMQ Queues.");
//        }
//    }
//}
package QueueConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import dto.ConsumerLiftRideDto;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class LiftRideMQConsumer {
    private static final String QUEUE_NAME = "skiQueue";
    private static final String RMQ_HOST_ADDRESS = "52.25.147.184";
//    private static final String RMQ_HOST_ADDRESS = "localhost";
    private static final int THREAD_COUNT = 20;
    private static final int CHANNEL_POOL_SIZE = 20;
    private static final String MQ_USERNAME = "admin";
    private static final String MQ_PASSWORD = "admin";

    private static final ConcurrentHashMap<Integer, List<ConsumerLiftRideDto>> skierRidesHashMap = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Connection connection;
    private static final BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>();
    // A pool of channels to prevent excessive opening/closing of channels.

    public static void main(String[] args) {
        try {
            // Setting Up RabbitMQ and Workers, connection,
            setupRabbitMQConnectionAndChannelPool();

            // concurrently process messages
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.submit(new WorkerThread(channelPool, QUEUE_NAME, skierRidesHashMap, objectMapper));
            }

            // print message consumed
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                int totalMessages = skierRidesHashMap.values().stream().mapToInt(List::size).sum();
                System.out.println(" Current total messages stored: " + totalMessages);
            }, 5, 15, TimeUnit.SECONDS); // every 15seconds

            // Shutdown Hook shut down consumer when the program exits
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down consumer...");
                executorService.shutdown();
                try {
                    // Tries to shut down all worker threads, waits 5 seconds, then forcefully shuts them down if they don't terminate in time.
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executorService.shutdownNow();
                }
                closeRabbitMQConnection();
                System.out.println("Consumer shutdown complete.");
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupRabbitMQConnectionAndChannelPool() throws IOException, TimeoutException {
        // Creates a RabbitMQ connection.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_ADDRESS);
        factory.setUsername(MQ_USERNAME);
        factory.setPassword(MQ_PASSWORD);
        connection = factory.newConnection();

        // creates a pool of 10 channels
        for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
            channelPool.offer(connection.createChannel());
        }

        System.out.println("RabbitMQ Connection and Channel Pool setup complete.");
    }

    private static void closeRabbitMQConnection() {
        try {
            for (Channel channel : channelPool) {
                if (channel.isOpen()) {
                    channel.close();
                }
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
