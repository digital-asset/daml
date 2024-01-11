// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.ByteString;

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
}
