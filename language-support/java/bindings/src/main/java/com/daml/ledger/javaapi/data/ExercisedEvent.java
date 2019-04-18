// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ExercisedEvent extends Event {

    private final List<String> witnessParties;

    private final String eventId;

    private final Identifier templateId;

    private final String contractId;

    private final String contractCreatingEventId;

    private final String choice;

    private final Value choiceArgument;

    private final java.util.List<String> actingParties;

    private final boolean consuming;

    private final List<String> childEventIds;

    public ExercisedEvent(List<String> witnessParties,
                          String eventId,
                          Identifier templateId,
                          String contractId,
                          String contractCreatingEventId,
                          String choice,
                          Value choiceArgument,
                          List<String> actingParties,
                          boolean consuming,
                          List<String> childEventIds) {
        this.witnessParties = witnessParties;
        this.eventId = eventId;
        this.templateId = templateId;
        this.contractId = contractId;
        this.contractCreatingEventId = contractCreatingEventId;
        this.choice = choice;
        this.choiceArgument = choiceArgument;
        this.actingParties = actingParties;
        this.consuming = consuming;
        this.childEventIds = childEventIds;
    }

    @Override
    public List<String> getWitnessParties() {
        return witnessParties;
    }

    @Override
    public String getEventId() {
        return eventId;
    }

    @Override
    public Identifier getTemplateId() {
        return templateId;
    }

    @Override
    public String getContractId() {
        return contractId;
    }

    public String getContractCreatingEventId() {
        return contractCreatingEventId;
    }

    public String getChoice() {
        return choice;
    }

    public List<String> getChildEventIds() {
        return childEventIds;
    }

    public Value getChoiceArgument() {
        return choiceArgument;
    }

    public List<String> getActingParties() {
        return actingParties;
    }

    public boolean isConsuming() {
        return consuming;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExercisedEvent that = (ExercisedEvent) o;
        return consuming == that.consuming &&
                Objects.equals(witnessParties, that.witnessParties) &&
                Objects.equals(eventId, that.eventId) &&
                Objects.equals(templateId, that.templateId) &&
                Objects.equals(contractId, that.contractId) &&
                Objects.equals(contractCreatingEventId, that.contractCreatingEventId) &&
                Objects.equals(choice, that.choice) &&
                Objects.equals(choiceArgument, that.choiceArgument) &&
                Objects.equals(actingParties, that.actingParties);
    }

    @Override
    public int hashCode() {

        return Objects.hash(witnessParties, eventId, templateId, contractId, contractCreatingEventId, choice, choiceArgument, actingParties, consuming);
    }

    @Override
    public String toString() {
        return "ExercisedEvent{" +
                "witnessParties=" + witnessParties +
                ", eventId='" + eventId + '\'' +
                ", templateId=" + templateId +
                ", contractId='" + contractId + '\'' +
                ", contractCreatingEventId='" + contractCreatingEventId + '\'' +
                ", choice='" + choice + '\'' +
                ", choiceArgument=" + choiceArgument +
                ", actingParties=" + actingParties +
                ", consuming=" + consuming +
                ", childEventIds=" + childEventIds +
                '}';
    }

    public EventOuterClass.ExercisedEvent toProto() {
        return EventOuterClass.ExercisedEvent.newBuilder()
                .setEventId(getEventId())
                .setChoice(getChoice())
                .setChoiceArgument(getChoiceArgument().toProto())
                .setConsuming(isConsuming())
                .setContractId(getContractId())
                .setTemplateId(getTemplateId().toProto())
                .setContractCreatingEventId(getContractCreatingEventId())
                .addAllActingParties(getActingParties())
                .addAllWitnessParties(getWitnessParties())
                .addAllChildEventIds(getChildEventIds())
                .build();
    }

    public static ExercisedEvent fromProto(EventOuterClass.ExercisedEvent exercisedEvent) {
        return new ExercisedEvent(
                exercisedEvent.getWitnessPartiesList(),
                exercisedEvent.getEventId(),
                Identifier.fromProto(exercisedEvent.getTemplateId()),
                exercisedEvent.getContractId(),
                exercisedEvent.getContractCreatingEventId(),
                exercisedEvent.getChoice(),
                Value.fromProto(exercisedEvent.getChoiceArgument()),
                exercisedEvent.getActingPartiesList(),
                exercisedEvent.getConsuming(),
                exercisedEvent.getChildEventIdsList());
    }
}
