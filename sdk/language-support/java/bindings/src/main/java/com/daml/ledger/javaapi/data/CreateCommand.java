// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CreateCommand extends Command {

  private final Identifier templateId;

  private final DamlRecord createArguments;

  public CreateCommand(@NonNull Identifier templateId, @NonNull DamlRecord createArguments) {
    this.templateId = templateId;
    this.createArguments = createArguments;
  }

  @Override
  public String toString() {
    return "CreateCommand{"
        + "templateId="
        + templateId
        + ", createArguments="
        + createArguments
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateCommand that = (CreateCommand) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(createArguments, that.createArguments);
  }

  @Override
  public int hashCode() {

    return Objects.hash(templateId, createArguments);
  }

  @NonNull
  @Override
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  public DamlRecord getCreateArguments() {
    return createArguments;
  }

  public static CreateCommand fromProto(CommandsOuterClass.CreateCommand create) {
    DamlRecord createArgument = DamlRecord.fromProto(create.getCreateArguments());
    Identifier templateId = Identifier.fromProto(create.getTemplateId());
    return new CreateCommand(templateId, createArgument);
  }

  public CommandsOuterClass.CreateCommand toProto() {
    return CommandsOuterClass.CreateCommand.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setCreateArguments(this.createArguments.toProtoRecord())
        .build();
  }
}
