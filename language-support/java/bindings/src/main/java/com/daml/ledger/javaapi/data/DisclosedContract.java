// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Any;

public final class DisclosedContract {
  public final Identifier templateId;
  public final String contractId;
  public final Arguments arguments;
  public final ContractMetadata contractMetadata;

  public DisclosedContract(
      Identifier templateId,
      String contractId,
      Arguments arguments,
      ContractMetadata contractMetadata) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.arguments = arguments;
    this.contractMetadata = contractMetadata;
  }

  public static interface Arguments {
    public CommandsOuterClass.DisclosedContract.Builder toProto(
        CommandsOuterClass.DisclosedContract.Builder builder);
  }

  public static final class CreateArguments implements Arguments {
    public final DamlRecord arguments;

    public CreateArguments(DamlRecord arguments) {
      this.arguments = arguments;
    }

    public CommandsOuterClass.DisclosedContract.Builder toProto(
        CommandsOuterClass.DisclosedContract.Builder builder) {
      return builder.setCreateArguments(arguments.toProtoRecord());
    }
  }

  public static final class CreateArgumentsBlob implements Arguments {
    public final Any createArgumentsBlob;

    public CreateArgumentsBlob(Any createArgumentsBlob) {
      this.createArgumentsBlob = createArgumentsBlob;
    }

    public CommandsOuterClass.DisclosedContract.Builder toProto(
        CommandsOuterClass.DisclosedContract.Builder builder) {
      return builder.setCreateArgumentsBlob(createArgumentsBlob);
    }
  }

  public CommandsOuterClass.DisclosedContract toProto() {
    var builder =
        CommandsOuterClass.DisclosedContract.newBuilder()
            .setTemplateId(this.templateId.toProto())
            .setContractId(this.contractId)
            .setMetadata(this.contractMetadata.toProto());
    return this.arguments.toProto(builder).build();
  }
}
