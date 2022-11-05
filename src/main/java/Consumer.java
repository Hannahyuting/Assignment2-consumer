import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {

    private static final String QUEUE_NAME = "skiersInfo";
    private static final String SERVER = "35.89.5.219";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(SERVER);
        factory.setUsername("yutingz");
        factory.setPassword("yutingz");
        factory.setVirtualHost("vhost");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        ConcurrentHashMap<Integer, String> hashMap = new ConcurrentHashMap<>();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    channel.basicQos(1);

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        JSONObject jsonObject = new JSONObject(message);
                        int skierId = jsonObject.getInt("skierId");
                        hashMap.put(skierId, message);
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    };
                    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        for (int i = 0; i < 200; i++) {
            Thread thread = new Thread(runnable);
            thread.start();
        }
    }
}
