package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class BrokerActor extends AbstractActor {
    private final Map<String, ActorRef> subscribers = new HashMap<>();
    private ActorRef worker1;
    private ActorRef worker2;
    private boolean batchMode = false;
    private final List<PublishMsg> eventBuffer = new ArrayList<>();

    public static Props props() {
        return Props.create(BrokerActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubscribeMsg.class, this::handleSubscribeMsg)
                .match(PublishMsg.class, this::handlePublishMsg)
                .match(BatchMsg.class, this::handleBatchMsg)
                .build();
    }

    private void handleSubscribeMsg(SubscribeMsg subscribeMsg) {
        // Implement logic for handling SubscribeMsg
        String topic = subscribeMsg.getTopic();
        ActorRef subscriber = getSender();
        subscribers.put(topic, subscriber);

        // Partition and assign topics to worker actors (even keys to worker1, odd keys to worker2)
        if (Integer.getInteger(topic) % 2 == 0) {
            worker1.tell(subscribeMsg, getSelf());
        } else {
            worker2.tell(subscribeMsg, getSelf());
        }
    }

    private void handlePublishMsg(PublishMsg publishMsg) {
        // Implement logic for handling PublishMsg
        if (subscribers.containsKey(publishMsg.getTopic())) {
            // Send the event directly to the subscriber
            subscribers.get(publishMsg.getTopic()).tell(new NotifyMsg(publishMsg.getValue()), getSelf());
        } else {
            // Topic not found, handle accordingly
            if (batchMode) {
                // Buffer the event in batch mode
                eventBuffer.add(publishMsg);
            } else {
                // Handle immediately
                // You can add additional logic here based on your requirements
            }
        }
    }

    private void handleBatchMsg(BatchMsg batchMsg) {
        // Implement logic for handling BatchMsg
        batchMode = batchMsg.isOn();
        if (!batchMode && !eventBuffer.isEmpty()) {
            // Process buffered events
            for (PublishMsg publishMsg : eventBuffer) {
                handlePublishMsg(publishMsg);
            }
            // Clear the buffer
            eventBuffer.clear();
        }
    }
}
