// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandsOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public class CreateAndExerciseCommand extends Command {
    private final Identifier templateId;

    private final Record createArguments;

    private final String choice;

    private final Value choiceArgument;

    public CreateAndExerciseCommand(@NonNull Identifier templateId, @NonNull Record createArguments, @NonNull String choice, @NonNull Value choiceArgument) {
        this.templateId = templateId;
        this.createArguments = createArguments;
        this.choice = choice;
        this.choiceArgument = choiceArgument;
    }

    public static CreateAndExerciseCommand fromProto(CommandsOuterClass.CreateAndExerciseCommand command) {
        Identifier templateId = Identifier.fromProto(command.getTemplateId());
        Record createArguments = Record.fromProto(command.getCreateArguments());
        String choice = command.getChoice();
        Value choiceArgument = Value.fromProto(command.getChoiceArgument());
        return new CreateAndExerciseCommand(templateId, createArguments, choice, choiceArgument);
    }

    public CommandsOuterClass.CreateAndExerciseCommand toProto() {
        return CommandsOuterClass.CreateAndExerciseCommand.newBuilder()
                .setTemplateId(this.templateId.toProto())
                .setCreateArguments(this.createArguments.toProtoRecord())
                .setChoice(this.choice)
                .setChoiceArgument(this.choiceArgument.toProto())
                .build();
    }

    @Override
    Identifier getTemplateId() {
        return templateId;
    }

    public Record getCreateArguments() {
        return createArguments;
    }

    public String getChoice() {
        return choice;
    }

    public Value getChoiceArgument() {
        return choiceArgument;
    }

    @Override
    public String toString() {
        return "CreateAndExerciseCommand{" +
                "templateId=" + templateId +
                ", createArguments=" + createArguments +
                ", choice='" + choice + '\'' +
                ", choiceArgument=" + choiceArgument +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateAndExerciseCommand that = (CreateAndExerciseCommand) o;
        return templateId.equals(that.templateId) &&
                createArguments.equals(that.createArguments) &&
                choice.equals(that.choice) &&
                choiceArgument.equals(that.choiceArgument);
    }

    @Override
    public int hashCode() {
        return Objects.hash(templateId, createArguments, choice, choiceArgument);
    }
}
