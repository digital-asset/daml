// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.EventOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

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

    private final Value exerciseResult;

    public ExercisedEvent(@NonNull List<@NonNull String> witnessParties,
                          @NonNull String eventId,
                          @NonNull Identifier templateId,
                          @NonNull String contractId,
                          @NonNull String contractCreatingEventId,
                          @NonNull String choice,
                          @NonNull Value choiceArgument,
                          @NonNull List<@NonNull String> actingParties,
                          boolean consuming,
                          @NonNull List<@NonNull String> childEventIds,
                          @NonNull Value exerciseResult) {
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
        this.exerciseResult = exerciseResult;
    }

    @NonNull
    @Override
    public List<@NonNull String> getWitnessParties() {
        return witnessParties;
    }

    @NonNull
    @Override
    public String getEventId() {
        return eventId;
    }

    @NonNull
    @Override
    public Identifier getTemplateId() {
        return templateId;
    }

    @NonNull
    @Override
    public String getContractId() {
        return contractId;
    }

    @NonNull
    public String getContractCreatingEventId() {
        return contractCreatingEventId;
    }

    @NonNull
    public String getChoice() {
        return choice;
    }

    @NonNull
    public List<@NonNull String> getChildEventIds() {
        return childEventIds;
    }

    @NonNull
    public Value getChoiceArgument() {
        return choiceArgument;
    }

    public @NonNull List<@NonNull String> getActingParties() {
        return actingParties;
    }

    public boolean isConsuming() {
        return consuming;
    }

    @NonNull
    public Value getExerciseResult() {
        return exerciseResult;
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
                Objects.equals(actingParties, that.actingParties) &&
                Objects.equals(exerciseResult, that.exerciseResult);
    }

    @Override
    public int hashCode() {

        return Objects.hash(witnessParties, eventId, templateId, contractId, contractCreatingEventId, choice, choiceArgument, actingParties, consuming, exerciseResult);
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
                ", exerciseResult=" + exerciseResult +
                '}';
    }

    public EventOuterClass.@NonNull ExercisedEvent toProto() {
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
                .setExerciseResult(getExerciseResult().toProto())
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
                exercisedEvent.getChildEventIdsList(),
                Value.fromProto(exercisedEvent.getExerciseResult()));
    }
}
