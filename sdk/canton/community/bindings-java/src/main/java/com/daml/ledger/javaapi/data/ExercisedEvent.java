// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.EventOuterClass;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ExercisedEvent implements TreeEvent {

  private final List<String> witnessParties;

  private final Long offset;

  private final Integer nodeId;

  private final Identifier templateId;

  private final String packageName;

  private final Optional<Identifier> interfaceId;

  private final String contractId;

  private final String choice;

  private final Value choiceArgument;

  private final java.util.List<String> actingParties;

  private final boolean consuming;

  private final List<Integer> childNodeIds;

  private final Integer lastDescendantNodeId;

  private final Value exerciseResult;

  public ExercisedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull Long offset,
      @NonNull Integer nodeId,
      @NonNull Identifier templateId,
      @NonNull String packageName,
      @NonNull Optional<Identifier> interfaceId,
      @NonNull String contractId,
      @NonNull String choice,
      @NonNull Value choiceArgument,
      @NonNull List<@NonNull String> actingParties,
      boolean consuming,
      @NonNull List<@NonNull Integer> childNodeIds,
      @NonNull Integer lastDescendantNodeId,
      @NonNull Value exerciseResult) {
    this.witnessParties = witnessParties;
    this.offset = offset;
    this.nodeId = nodeId;
    this.templateId = templateId;
    this.packageName = packageName;
    this.interfaceId = interfaceId;
    this.contractId = contractId;
    this.choice = choice;
    this.choiceArgument = choiceArgument;
    this.actingParties = actingParties;
    this.consuming = consuming;
    this.childNodeIds = childNodeIds;
    this.lastDescendantNodeId = lastDescendantNodeId;
    this.exerciseResult = exerciseResult;
  }

  @NonNull
  @Override
  public List<@NonNull String> getWitnessParties() {
    return witnessParties;
  }

  @NonNull
  @Override
  public Long getOffset() {
    return offset;
  }

  @NonNull
  @Override
  public Integer getNodeId() {
    return nodeId;
  }

  @NonNull
  @Override
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  @Override
  public String getPackageName() {
    return packageName;
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
  public List<@NonNull Integer> getChildNodeIds() {
    return childNodeIds;
  }

  @NonNull
  public Integer getLastDescendantNodeId() {
    return lastDescendantNodeId;
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
        && Objects.equals(offset, that.offset)
        && Objects.equals(nodeId, that.nodeId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(interfaceId, that.interfaceId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(choice, that.choice)
        && Objects.equals(choiceArgument, that.choiceArgument)
        && Objects.equals(actingParties, that.actingParties)
        && Objects.equals(childNodeIds, that.childNodeIds)
        && Objects.equals(lastDescendantNodeId, that.lastDescendantNodeId)
        && Objects.equals(exerciseResult, that.exerciseResult);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        witnessParties,
        offset,
        nodeId,
        templateId,
        packageName,
        interfaceId,
        contractId,
        choice,
        choiceArgument,
        actingParties,
        childNodeIds,
        lastDescendantNodeId,
        consuming,
        exerciseResult);
  }

  @Override
  public String toString() {
    return "ExercisedEvent{"
        + "witnessParties="
        + witnessParties
        + ", offset="
        + offset
        + ", nodeId="
        + nodeId
        + ", templateId="
        + templateId
        + ", packageName="
        + packageName
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
        + childNodeIds
        + ", lastDescendantNodeId="
        + lastDescendantNodeId
        + ", exerciseResult="
        + exerciseResult
        + '}';
  }

  public EventOuterClass.@NonNull ExercisedEvent toProto() {
    EventOuterClass.ExercisedEvent.Builder builder = EventOuterClass.ExercisedEvent.newBuilder();
    builder.setOffset(getOffset());
    builder.setNodeId(getNodeId());
    builder.setChoice(getChoice());
    builder.setChoiceArgument(getChoiceArgument().toProto());
    builder.setConsuming(isConsuming());
    builder.setContractId(getContractId());
    builder.setTemplateId(getTemplateId().toProto());
    builder.setPackageName(getPackageName());
    interfaceId.ifPresent(i -> builder.setInterfaceId(i.toProto()));
    builder.addAllActingParties(getActingParties());
    builder.addAllWitnessParties(getWitnessParties());
    builder.addAllChildNodeIds(getChildNodeIds());
    builder.setLastDescendantNodeId(getLastDescendantNodeId());
    builder.setExerciseResult(getExerciseResult().toProto());
    return builder.build();
  }

  public static ExercisedEvent fromProto(EventOuterClass.ExercisedEvent exercisedEvent) {
    return new ExercisedEvent(
        exercisedEvent.getWitnessPartiesList(),
        exercisedEvent.getOffset(),
        exercisedEvent.getNodeId(),
        Identifier.fromProto(exercisedEvent.getTemplateId()),
        exercisedEvent.getPackageName(),
        exercisedEvent.hasInterfaceId()
            ? Optional.of(Identifier.fromProto(exercisedEvent.getInterfaceId()))
            : Optional.empty(),
        exercisedEvent.getContractId(),
        exercisedEvent.getChoice(),
        Value.fromProto(exercisedEvent.getChoiceArgument()),
        exercisedEvent.getActingPartiesList(),
        exercisedEvent.getConsuming(),
        exercisedEvent.getChildNodeIdsList(),
        exercisedEvent.getLastDescendantNodeId(),
        Value.fromProto(exercisedEvent.getExerciseResult()));
  }
}
