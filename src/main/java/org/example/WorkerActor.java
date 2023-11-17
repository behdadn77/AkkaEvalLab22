package org.example;

import akka.actor.AbstractActor;
import java.util.HashSet;
import java.util.Set;

public class WorkerActor extends AbstractActor {
    private final Set<String> activeSubscriptions = new HashSet<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubscribeMsg.class, this::handleSubscribeMsg)
                .match(PublishMsg.class, this::handlePublishMsg)
                .build();
    }

    private void handleSubscribeMsg(SubscribeMsg subscribeMsg) {
        // Implement logic for handling SubscribeMsg
        String topic = subscribeMsg.getTopic();
        activeSubscriptions.add(topic);
    }

    private void handlePublishMsg(PublishMsg publishMsg) {
        // Implement logic for handling PublishMsg
        String topic = publishMsg.getTopic();
        if (activeSubscriptions.contains(topic)) {
            // Worker is aware of the topic, handle the event
            // You can add additional logic here based on your requirements
            System.out.println("Worker received event for topic " + topic + ": " + publishMsg.getValue());
        } else {
            // Worker is not aware of the topic, handle accordingly
            // You may choose to ignore or log the event
            System.out.println("Worker ignoring event for unknown topic " + topic);
        }
    }
}
