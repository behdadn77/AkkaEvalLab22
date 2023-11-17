package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

public class SubscriberActor extends AbstractActor {
    private final ActorRef broker;

    public SubscriberActor(ActorRef broker) {
        this.broker = broker;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubscribeMsg.class, this::handleSubscribeMsg)
                .match(NotifyMsg.class, this::handleNotificationMsg)
                .build();
    }

    private void handleSubscribeMsg(SubscribeMsg subscribeMsg) {
        // Implement logic for handling SubscribeMsg
        // Forward the subscription message to the broker
        broker.tell(subscribeMsg, getSelf());
    }

    private void handleNotificationMsg(NotifyMsg notificationMsg) {
        // Implement logic for handling NotificationMsg
        // Process the notification message received from the broker
        System.out.println("Subscriber received notification: " + notificationMsg.getValue());
    }
}
