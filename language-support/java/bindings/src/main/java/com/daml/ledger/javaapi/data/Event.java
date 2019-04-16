// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;

import com.digitalasset.ledger.api.v1.TransactionOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

public abstract class Event {

    public abstract @NonNull List<@NonNull String> getWitnessParties();

    @NonNull
    public abstract String getEventId();

    @NonNull
    public abstract Identifier getTemplateId();

    @NonNull
    public abstract String getContractId();

    public static Event fromProtoEvent(EventOuterClass.Event event) {

        if (event.hasCreated()) {
            EventOuterClass.CreatedEvent createdEvent = event.getCreated();
            return new CreatedEvent(createdEvent.getWitnessPartiesList(), createdEvent.getEventId(),
                    Identifier.fromProto(createdEvent.getTemplateId()),
                    createdEvent.getContractId(), Record.fromProto(createdEvent.getCreateArguments()));
        } else if (event.hasArchived()) {
            EventOuterClass.ArchivedEvent archivedEvent = event.getArchived();
            return new ArchivedEvent(archivedEvent.getWitnessPartiesList(), archivedEvent.getEventId(),
                    Identifier.fromProto(archivedEvent.getTemplateId()),
                    archivedEvent.getContractId());
        } else if (event.hasExercised()) {
            EventOuterClass.ExercisedEvent exercisedEvent = event.getExercised();
            return new ExercisedEvent(exercisedEvent.getWitnessPartiesList(), exercisedEvent.getEventId(),
                    Identifier.fromProto(exercisedEvent.getTemplateId()),
                    exercisedEvent.getContractId(), exercisedEvent.getContractCreatingEventId(),
                    exercisedEvent.getChoice(), Value.fromProto(exercisedEvent.getChoiceArgument()),
                    exercisedEvent.getActingPartiesList(), exercisedEvent.getConsuming(),
                    exercisedEvent.getChildEventIdsList(), Value.fromProto(exercisedEvent.getExerciseResult()));
        } else {
            throw new UnsupportedEventTypeException(event.toString());
        }
    }

    public static Event fromProtoTreeEvent(TransactionOuterClass.TreeEvent event) {

        if (event.hasCreated()) {
            EventOuterClass.CreatedEvent createdEvent = event.getCreated();
            return new CreatedEvent(createdEvent.getWitnessPartiesList(), createdEvent.getEventId(),
                    Identifier.fromProto(createdEvent.getTemplateId()),
                    createdEvent.getContractId(), Record.fromProto(createdEvent.getCreateArguments()));
        } else if (event.hasExercised()) {
            EventOuterClass.ExercisedEvent exercisedEvent = event.getExercised();
            return new ExercisedEvent(exercisedEvent.getWitnessPartiesList(), exercisedEvent.getEventId(),
                    Identifier.fromProto(exercisedEvent.getTemplateId()),
                    exercisedEvent.getContractId(), exercisedEvent.getContractCreatingEventId(),
                    exercisedEvent.getChoice(), Value.fromProto(exercisedEvent.getChoiceArgument()),
                    exercisedEvent.getActingPartiesList(), exercisedEvent.getConsuming(),
                    exercisedEvent.getChildEventIdsList(), Value.fromProto(exercisedEvent.getExerciseResult()));
        } else {
            throw new UnsupportedEventTypeException(event.toString());
        }
    }

    public EventOuterClass.Event toProtoEvent() {
        EventOuterClass.Event.Builder eventBuilder = EventOuterClass.Event.newBuilder();
        if (this instanceof ArchivedEvent) {
            ArchivedEvent event = (ArchivedEvent) this;
            eventBuilder.setArchived(event.toProto());
        } else if (this instanceof CreatedEvent) {
            CreatedEvent event = (CreatedEvent) this;
            eventBuilder.setCreated(event.toProto());
        } else if (this instanceof ExercisedEvent) {
            ExercisedEvent event = (ExercisedEvent) this;
            eventBuilder.setExercised(event.toProto());
        } else {
            throw new RuntimeException("this should be ArchivedEvent or CreatedEvent or ExercisedEvent, found " + this.toString());
        }
        return eventBuilder.build();
    }

    public TransactionOuterClass.TreeEvent toProtoTreeEvent() {
        TransactionOuterClass.TreeEvent.Builder eventBuilder = TransactionOuterClass.TreeEvent.newBuilder();
        if (this instanceof CreatedEvent) {
            CreatedEvent event = (CreatedEvent) this;
            eventBuilder.setCreated(event.toProto());
        } else if (this instanceof ExercisedEvent) {
            ExercisedEvent event = (ExercisedEvent) this;
            eventBuilder.setExercised(event.toProto());
        } else {
            throw new RuntimeException("this should be CreatedEvent or ExercisedEvent, found " + this.toString());
        }
        return eventBuilder.build();
    }
}

class UnsupportedEventTypeException extends RuntimeException {
    public UnsupportedEventTypeException(String eventStr) {
        super("Unsupported event " + eventStr);
    }
}
