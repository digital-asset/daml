// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class CreateCommand extends Command {

    private final Identifier templateId;

    private final Record createArguments;

    public CreateCommand(@NonNull Identifier templateId, @NonNull Record createArguments) {
        this.templateId = templateId;
        this.createArguments = createArguments;
    }

    @Override
    public String toString() {
        return "CreateCommand{" +
                "templateId=" + templateId +
                ", createArguments=" + createArguments +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateCommand that = (CreateCommand) o;
        return Objects.equals(templateId, that.templateId) &&
                Objects.equals(createArguments, that.createArguments);
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
    public Record getCreateArguments() {
        return createArguments;
    }

    public static CreateCommand fromProto(CommandsOuterClass.CreateCommand create) {
        Record createArgument = Record.fromProto(create.getCreateArguments());
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
