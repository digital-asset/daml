// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.*;

public final class UnassignedEvent implements ReassignmentEvent {

  private final long offset;

  private final @NonNull String unassignId;

  private final @NonNull String contractId;

  private final @NonNull Identifier templateId;

  private final @NonNull String packageName;

  private final @NonNull String source;

  private final @NonNull String target;

  private final @NonNull String submitter;

  private final long reassignmentCounter;

  private final @NonNull Instant assignmentExclusivity;

  private final @NonNull List<@NonNull String> witnessParties;

  private final int nodeId;

  public UnassignedEvent(
      long offset,
      @NonNull String unassignId,
      @NonNull String contractId,
      @NonNull Identifier templateId,
      @NonNull String packageName,
      @NonNull String source,
      @NonNull String target,
      @NonNull String submitter,
      long reassignmentCounter,
      @NonNull Instant assignmentExclusivity,
      @NonNull List<@NonNull String> witnessParties,
      int nodeId) {
    this.offset = offset;
    this.unassignId = unassignId;
    this.contractId = contractId;
    this.templateId = templateId;
    this.packageName = packageName;
    this.source = source;
    this.target = target;
    this.submitter = submitter;
    this.reassignmentCounter = reassignmentCounter;
    this.assignmentExclusivity = assignmentExclusivity;
    this.witnessParties = List.copyOf(witnessParties);
    this.nodeId = nodeId;
  }

  public long getOffset() {
    return offset;
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
  public String getPackageName() {
    return packageName;
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

  public int getNodeId() {
    return nodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnassignedEvent that = (UnassignedEvent) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(unassignId, that.unassignId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(submitter, that.submitter)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter)
        && Objects.equals(assignmentExclusivity, that.assignmentExclusivity)
        && Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        offset,
        unassignId,
        contractId,
        templateId,
        packageName,
        source,
        target,
        submitter,
        reassignmentCounter,
        assignmentExclusivity,
        witnessParties,
        nodeId);
  }

  @Override
  public String toString() {
    return "UnassignedEvent{"
        + "offset="
        + offset
        + ", unassignId='"
        + unassignId
        + '\''
        + ", contractId='"
        + contractId
        + '\''
        + ", packageName="
        + packageName
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
        + ", nodeId="
        + nodeId
        + '}';
  }

  public ReassignmentOuterClass.UnassignedEvent toProto() {
    return ReassignmentOuterClass.UnassignedEvent.newBuilder()
        .setOffset(this.offset)
        .setUnassignId(this.unassignId)
        .setContractId(this.contractId)
        .setTemplateId(this.getTemplateId().toProto())
        .setPackageName(this.packageName)
        .setSource(this.source)
        .setTarget(this.target)
        .setSubmitter(this.submitter)
        .setReassignmentCounter(this.reassignmentCounter)
        .setAssignmentExclusivity(Utils.instantToProto(this.assignmentExclusivity))
        .addAllWitnessParties(this.getWitnessParties())
        .setNodeId(this.getNodeId())
        .build();
  }

  public static UnassignedEvent fromProto(ReassignmentOuterClass.UnassignedEvent unassignedEvent) {
    return new UnassignedEvent(
        unassignedEvent.getOffset(),
        unassignedEvent.getUnassignId(),
        unassignedEvent.getContractId(),
        Identifier.fromProto(unassignedEvent.getTemplateId()),
        unassignedEvent.getPackageName(),
        unassignedEvent.getSource(),
        unassignedEvent.getTarget(),
        unassignedEvent.getSubmitter(),
        unassignedEvent.getReassignmentCounter(),
        Instant.ofEpochSecond(
            unassignedEvent.getAssignmentExclusivity().getSeconds(),
            unassignedEvent.getAssignmentExclusivity().getNanos()),
        unassignedEvent.getWitnessPartiesList(),
        unassignedEvent.getNodeId());
  }
}
