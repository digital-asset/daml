// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.EventOuterClass;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ExercisedEvent implements TreeEvent {

  private final List<String> witnessParties;

  private final String eventId;

  private final Identifier templateId;

  private final Optional<Identifier> interfaceId;

  private final String contractId;

  private final String choice;

  private final Value choiceArgument;

  private final java.util.List<String> actingParties;

  private final boolean consuming;

  private final List<String> childEventIds;

  private final Value exerciseResult;

  public ExercisedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull String eventId,
      @NonNull Identifier templateId,
      @NonNull Optional<Identifier> interfaceId,
      @NonNull String contractId,
      @NonNull String choice,
      @NonNull Value choiceArgument,
      @NonNull List<@NonNull String> actingParties,
      boolean consuming,
      @NonNull List<@NonNull String> childEventIds,
      @NonNull Value exerciseResult) {
    this.witnessParties = witnessParties;
    this.eventId = eventId;
    this.templateId = templateId;
    this.interfaceId = interfaceId;
    this.contractId = contractId;
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
  public Optional<Identifier> getInterfaceId() {
    return interfaceId;
  }

  @NonNull
  @Override
  public String getContractId() {
    return contractId;
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
    return consuming == that.consuming
        && Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(eventId, that.eventId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(interfaceId, that.interfaceId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(choice, that.choice)
        && Objects.equals(choiceArgument, that.choiceArgument)
        && Objects.equals(actingParties, that.actingParties)
        && Objects.equals(childEventIds, that.childEventIds)
        && Objects.equals(exerciseResult, that.exerciseResult);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        witnessParties,
        eventId,
        templateId,
        interfaceId,
        contractId,
        choice,
        choiceArgument,
        actingParties,
        childEventIds,
        consuming,
        exerciseResult);
  }

  @Override
  public String toString() {
    return "ExercisedEvent{"
        + "witnessParties="
        + witnessParties
        + ", eventId='"
        + eventId
        + '\''
        + ", templateId="
        + templateId
        + ", interfaceId="
        + interfaceId
        + ", contractId='"
        + contractId
        + '\''
        + ", choice='"
        + choice
        + '\''
        + ", choiceArgument="
        + choiceArgument
        + ", actingParties="
        + actingParties
        + ", consuming="
        + consuming
        + ", childEventIds="
        + childEventIds
        + ", exerciseResult="
        + exerciseResult
        + '}';
  }

  public EventOuterClass.@NonNull ExercisedEvent toProto() {
    EventOuterClass.ExercisedEvent.Builder builder = EventOuterClass.ExercisedEvent.newBuilder();
    builder.setEventId(getEventId());
    builder.setChoice(getChoice());
    builder.setChoiceArgument(getChoiceArgument().toProto());
    builder.setConsuming(isConsuming());
    builder.setContractId(getContractId());
    builder.setTemplateId(getTemplateId().toProto());
    interfaceId.ifPresent(i -> builder.setInterfaceId(i.toProto()));
    builder.addAllActingParties(getActingParties());
    builder.addAllWitnessParties(getWitnessParties());
    builder.addAllChildEventIds(getChildEventIds());
    builder.setExerciseResult(getExerciseResult().toProto());
    return builder.build();
  }

  public static ExercisedEvent fromProto(EventOuterClass.ExercisedEvent exercisedEvent) {
    return new ExercisedEvent(
        exercisedEvent.getWitnessPartiesList(),
        exercisedEvent.getEventId(),
        Identifier.fromProto(exercisedEvent.getTemplateId()),
        exercisedEvent.hasInterfaceId()
            ? Optional.of(Identifier.fromProto(exercisedEvent.getInterfaceId()))
            : Optional.empty(),
        exercisedEvent.getContractId(),
        exercisedEvent.getChoice(),
        Value.fromProto(exercisedEvent.getChoiceArgument()),
        exercisedEvent.getActingPartiesList(),
        exercisedEvent.getConsuming(),
        exercisedEvent.getChildEventIdsList(),
        Value.fromProto(exercisedEvent.getExerciseResult()));
  }
}
