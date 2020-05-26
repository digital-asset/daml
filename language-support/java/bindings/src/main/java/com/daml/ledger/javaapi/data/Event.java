// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * This interface represents events in {@link Transaction}s.
 *
 * @see CreatedEvent
 * @see ArchivedEvent
 * @see Transaction
 *
 */
public interface Event {

    @NonNull List<@NonNull String> getWitnessParties();

    @NonNull String getEventId();

    @NonNull Identifier getTemplateId();

    @NonNull String getContractId();

    default EventOuterClass.Event toProtoEvent() {
        EventOuterClass.Event.Builder eventBuilder = EventOuterClass.Event.newBuilder();
        if (this instanceof ArchivedEvent) {
            ArchivedEvent event = (ArchivedEvent) this;
            eventBuilder.setArchived(event.toProto());
        } else if (this instanceof CreatedEvent) {
            CreatedEvent event = (CreatedEvent) this;
            eventBuilder.setCreated(event.toProto());
        } else {
            throw new RuntimeException("this should be ArchivedEvent or CreatedEvent or ExercisedEvent, found " + this.toString());
        }
        return eventBuilder.build();
    }

    static Event fromProtoEvent(EventOuterClass.Event event) {
        if (event.hasCreated()) {
            return CreatedEvent.fromProto(event.getCreated());
        } else if (event.hasArchived()) {
            return ArchivedEvent.fromProto(event.getArchived());
        } else {
            throw new UnsupportedEventTypeException(event.toString());
        }
    }
}
