// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.ByteString;

import java.util.Objects;

public final class DisclosedContract {
  public final Identifier templateId;
  public final String contractId;
  public final ByteString createdEventBlob;

  public DisclosedContract(Identifier templateId, String contractId, ByteString createdEventBlob) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.createdEventBlob = createdEventBlob;
  }

  public CommandsOuterClass.DisclosedContract toProto() {
    return CommandsOuterClass.DisclosedContract.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setContractId(this.contractId)
        .setCreatedEventBlob(this.createdEventBlob)
        .build();
  }

  public static DisclosedContract fromProto(
      CommandsOuterClass.DisclosedContract disclosedContract) {
    return new DisclosedContract(
        Identifier.fromProto(disclosedContract.getTemplateId()),
        disclosedContract.getContractId(),
        disclosedContract.getCreatedEventBlob());
  }

  @Override
  public String toString() {
    return "DisclosedContract{"
        + "templateId="
        + templateId
        + ", contractId='"
        + contractId
        + '\''
        + ", createdEventBlob='"
        + createdEventBlob
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DisclosedContract that = (DisclosedContract) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(createdEventBlob, that.createdEventBlob);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, contractId, createdEventBlob);
  }
}
