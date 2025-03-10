# Upic Ski Resort Consumer System 🎿🏔️

This service is responsible for consuming ski lift event messages from **RabbitMQ** and processing them efficiently.

## 1. Update the RabbitMQ Server Configuration
Before running the consumer, ensure the **RabbitMQ server details** are correctly set.
Locate these **configuration** in `LiftRideMQConsumer.java`:

```java
private static final String QUEUE_NAME = "Your Queue Name";
    private static final String RMQ_HOST_ADDRESS = "Your RabbitMQ address";
    private static final String MQ_USERNAME = "Your MQ username";
    private static final String MQ_PASSWORD = "Your MQ password";
```
## 2. Build and Deploy the Consumer
To compile and run the consumer, navigate to the project directory and execute:

```java
mvn clean install
java -jar HWSkiListEventConsumer-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 3. Troubleshooting
- If messages are not being processed, ensure RabbitMQ is running and accessible.
- Verify the queue name matches the one defined in QUEUE_NAME.
- If the consumer fails due to connection issues, check firewall and security group settings.
- For high failure rates, consider increasing THREAD_POOL_SIZE