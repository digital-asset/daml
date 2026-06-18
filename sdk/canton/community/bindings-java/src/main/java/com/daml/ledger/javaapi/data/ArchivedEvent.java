// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.EventOuterClass;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ArchivedEvent implements Event {

  private final List<String> witnessParties;

  private final Long offset;

  private final Integer nodeId;

  private final Identifier templateId;

  private final String packageName;

  private final String contractId;

  private final List<Identifier> implementedInterfaces;

  public ArchivedEvent(
      @NonNull List<@NonNull String> witnessParties,
      @NonNull Long offset,
      @NonNull Integer nodeId,
      @NonNull Identifier templateId,
      @NonNull String packageName,
      @NonNull String contractId,
      @NonNull List<@NonNull Identifier> implementedInterfaces) {
    this.witnessParties = witnessParties;
    this.offset = offset;
    this.nodeId = nodeId;
    this.templateId = templateId;
    this.packageName = packageName;
    this.contractId = contractId;
    this.implementedInterfaces = implementedInterfaces;
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
  @Override
  public String getContractId() {
    return contractId;
  }

  @NonNull
  public List<Identifier> getImplementedInterfaces() {
    return implementedInterfaces;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArchivedEvent that = (ArchivedEvent) o;
    return Objects.equals(witnessParties, that.witnessParties)
        && Objects.equals(offset, that.offset)
        && Objects.equals(nodeId, that.nodeId)
        && Objects.equals(templateId, that.templateId)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(implementedInterfaces, that.implementedInterfaces);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        witnessParties, offset, nodeId, templateId, packageName, contractId, implementedInterfaces);
  }

  @Override
  public String toString() {
    return "ArchivedEvent{"
        + "witnessParties="
        + witnessParties
        + ", offset="
        + offset
        + ", nodeId="
        + nodeId
        + ", packageName="
        + packageName
        + ", templateId="
        + templateId
        + ", contractId='"
        + contractId
        + ", implementedInterfaces='"
        + implementedInterfaces
        + '\''
        + '}';
  }

  public EventOuterClass.ArchivedEvent toProto() {
    return EventOuterClass.ArchivedEvent.newBuilder()
        .setContractId(getContractId())
        .setOffset(getOffset())
        .setNodeId(getNodeId())
        .setTemplateId(getTemplateId().toProto())
        .setPackageName(getPackageName())
        .addAllWitnessParties(getWitnessParties())
        .addAllImplementedInterfaces(
            getImplementedInterfaces().stream().map(Identifier::toProto).toList())
        .build();
  }

  public static ArchivedEvent fromProto(EventOuterClass.ArchivedEvent archivedEvent) {
    return new ArchivedEvent(
        archivedEvent.getWitnessPartiesList(),
        archivedEvent.getOffset(),
        archivedEvent.getNodeId(),
        Identifier.fromProto(archivedEvent.getTemplateId()),
        archivedEvent.getPackageName(),
        archivedEvent.getContractId(),
        archivedEvent.getImplementedInterfacesList().stream().map(Identifier::fromProto).toList());
  }
}
