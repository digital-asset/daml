// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.util.Optional;

public final class DisclosedContract {
  public final Identifier templateId;
  public final String contractId;
  public final Optional<Arguments> arguments;
  public final Optional<ContractMetadata> contractMetadata;
  public final Optional<ByteString> createEventPayload;

  public DisclosedContract(
      Identifier templateId, String contractId, ByteString createEventPayload) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.createEventPayload = Optional.of(createEventPayload);
    this.arguments = Optional.empty();
    this.contractMetadata = Optional.empty();
  }

  /** @deprecated Use the constructor with {@link #createEventPayload} instead. Since Daml 2.8.0 */
  @Deprecated
  public DisclosedContract(
      Identifier templateId,
      String contractId,
      Arguments arguments,
      ContractMetadata contractMetadata) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.arguments = Optional.of(arguments);
    this.contractMetadata = Optional.of(contractMetadata);
    this.createEventPayload = Optional.empty();
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

  @SuppressWarnings("deprecation")
  public CommandsOuterClass.DisclosedContract toProto() {
    var builder =
        CommandsOuterClass.DisclosedContract.newBuilder()
            .setTemplateId(this.templateId.toProto())
            .setContractId(this.contractId);

    this.contractMetadata.ifPresent(cm -> builder.setMetadata(cm.toProto()));
    this.arguments.ifPresent(args -> args.toProto(builder));
    this.createEventPayload.ifPresent(builder::setCreateEventPayload);
    return builder.build();
  }
}
