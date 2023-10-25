// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

public final class DisclosedContract {
  public final Identifier templateId;
  public final String contractId;
  public final Arguments arguments;
  public final ContractMetadata contractMetadata;
  public final ByteString createEventPayload;

  public DisclosedContract(
      Identifier templateId,
      String contractId,
      Arguments arguments,
      ContractMetadata contractMetadata) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.arguments = arguments;
    this.contractMetadata = contractMetadata;
    this.createEventPayload = ByteString.EMPTY;
  }

  /** @deprecated Use the constructor with {@link #createEventPayload} instead. Since Daml 2.8.0 */
  public DisclosedContract(
      Identifier templateId, String contractId, ByteString createEventPayload) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.createEventPayload = createEventPayload;
    this.arguments = Arguments.Empty();
    this.contractMetadata = ContractMetadata.Empty();
  }

  public static interface Arguments {
    public CommandsOuterClass.DisclosedContract.Builder toProto(
        CommandsOuterClass.DisclosedContract.Builder builder);

    public static Arguments Empty() {
      return builder -> builder;
    }
  }

  public static final class CreateArguments implements Arguments {
    public final DamlRecord arguments;

    public CreateArguments(DamlRecord arguments) {
      this.arguments = arguments;
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
    public CommandsOuterClass.DisclosedContract.Builder toProto(
        CommandsOuterClass.DisclosedContract.Builder builder) {
      return builder.setCreateArgumentsBlob(createArgumentsBlob);
    }
  }

  public CommandsOuterClass.DisclosedContract toProto() {
    @SuppressWarnings("deprecation")
    var builder =
        CommandsOuterClass.DisclosedContract.newBuilder()
            .setTemplateId(this.templateId.toProto())
            .setContractId(this.contractId)
            .setMetadata(this.contractMetadata.toProto())
            .setCreateEventPayload(this.createEventPayload);
    return this.arguments.toProto(builder).build();
  }
}
