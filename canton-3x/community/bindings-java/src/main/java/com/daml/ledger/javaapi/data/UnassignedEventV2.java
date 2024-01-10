// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.*;

// TODO (i15873) Eliminate V2 suffix
public final class UnassignedEventV2 {

  private final @NonNull String unassignId;

  private final @NonNull String contractId;

  private final @NonNull Identifier templateId;

  private final @NonNull String source;

  private final @NonNull String target;

  private final @NonNull String submitter;

  private final long reassignmentCounter;

  private final @NonNull Instant assignmentExclusivity;

  private final @NonNull List<@NonNull String> witnessParties;

  public UnassignedEventV2(
      @NonNull String unassignId,
      @NonNull String contractId,
      @NonNull Identifier templateId,
      @NonNull String source,
      @NonNull String target,
      @NonNull String submitter,
      long reassignmentCounter,
      @NonNull Instant assignmentExclusivity,
      @NonNull List<@NonNull String> witnessParties) {
    this.unassignId = unassignId;
    this.contractId = contractId;
    this.templateId = templateId;
    this.source = source;
    this.target = target;
    this.submitter = submitter;
    this.reassignmentCounter = reassignmentCounter;
    this.assignmentExclusivity = assignmentExclusivity;
    this.witnessParties = List.copyOf(witnessParties);
  }

  @NonNull
  public String getUnassignId() {
    return unassignId;
  }

  @NonNull
  public String getContractId() {
    return contractId;
  }

  @NonNull
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  @NonNull
  public String getSubmitter() {
    return submitter;
  }

  public long getReassignmentCounter() {
    return reassignmentCounter;
  }

  @NonNull
  public Instant getAssignmentExclusivity() {
    return assignmentExclusivity;
  }

  @NonNull
  public List<@NonNull String> getWitnessParties() {
    return witnessParties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnassignedEventV2 that = (UnassignedEventV2) o;
    return Objects.equals(unassignId, that.unassignId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(submitter, that.submitter)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter)
        && Objects.equals(assignmentExclusivity, that.assignmentExclusivity)
        && Objects.equals(witnessParties, that.witnessParties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        unassignId,
        contractId,
        templateId,
        source,
        target,
        submitter,
        reassignmentCounter,
        assignmentExclusivity,
        witnessParties);
  }

  @Override
  public String toString() {
    return "UnassignedEvent{"
        + "unassignId='"
        + unassignId
        + '\''
        + ", contractId='"
        + contractId
        + '\''
        + ", templateId="
        + templateId
        + ", source="
        + source
        + ", target="
        + target
        + ", submitter="
        + submitter
        + ", reassignmentCounter="
        + reassignmentCounter
        + ", assignmentExclusivity="
        + assignmentExclusivity
        + ", witnessParties="
        + witnessParties
        + '}';
  }

  public ReassignmentOuterClass.UnassignedEvent toProto() {
    return ReassignmentOuterClass.UnassignedEvent.newBuilder()
        .setUnassignId(this.unassignId)
        .setContractId(this.contractId)
        .setTemplateId(this.getTemplateId().toProto())
        .setSource(this.source)
        .setTarget(this.target)
        .setSubmitter(this.submitter)
        .setReassignmentCounter(this.reassignmentCounter)
        .setAssignmentExclusivity(Utils.instantToProto(this.assignmentExclusivity))
        .addAllWitnessParties(this.getWitnessParties())
        .build();
  }

  public static UnassignedEventV2 fromProto(
      ReassignmentOuterClass.UnassignedEvent unassignedEvent) {
    return new UnassignedEventV2(
        unassignedEvent.getUnassignId(),
        unassignedEvent.getContractId(),
        Identifier.fromProto(unassignedEvent.getTemplateId()),
        unassignedEvent.getSource(),
        unassignedEvent.getTarget(),
        unassignedEvent.getSubmitter(),
        unassignedEvent.getReassignmentCounter(),
        Instant.ofEpochSecond(
            unassignedEvent.getAssignmentExclusivity().getSeconds(),
            unassignedEvent.getAssignmentExclusivity().getNanos()),
        unassignedEvent.getWitnessPartiesList());
  }
}
