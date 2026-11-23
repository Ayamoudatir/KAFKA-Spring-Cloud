package com.ebankservice.kafkaspringcloudstream.events;

import org.springframework.kafka.support.serializer.JsonSerde;

/** JSON serializer/deserializer with the concrete type required by Kafka Streams. */
public class PageEventSerde extends JsonSerde<PageEvent> {

    public PageEventSerde() {
        super(PageEvent.class);
    }
}
