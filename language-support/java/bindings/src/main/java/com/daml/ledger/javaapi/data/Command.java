// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;

import java.util.Optional;

public abstract class Command {

    abstract Identifier getTemplateId();

    public static Command fromProtoCommand(CommandsOuterClass.Command command) {
        switch (command.getCommandCase()) {
            case CREATE:
                return CreateCommand.fromProto(command.getCreate());
            case EXERCISE:
                return ExerciseCommand.fromProto(command.getExercise());
            case CREATEANDEXERCISE:
                return CreateAndExerciseCommand.fromProto(command.getCreateAndExercise());
            case EXERCISEBYKEY:
                return ExerciseByKeyCommand.fromProto(command.getExerciseByKey());
            case COMMAND_NOT_SET:
            default:
                throw new ProtoCommandUnknown(command);
        }
    }

    public CommandsOuterClass.Command toProtoCommand() {
        CommandsOuterClass.Command.Builder builder = CommandsOuterClass.Command.newBuilder();
        if (this instanceof CreateCommand) {
            builder.setCreate(((CreateCommand) this).toProto());
        } else if (this instanceof ExerciseCommand) {
            builder.setExercise(((ExerciseCommand) this).toProto());
        } else if (this instanceof CreateAndExerciseCommand) {
            builder.setCreateAndExercise(((CreateAndExerciseCommand) this).toProto());
        } else if (this instanceof ExerciseByKeyCommand) {
            builder.setExerciseByKey(((ExerciseByKeyCommand) this).toProto());
        } else {
            throw new CommandUnknown(this);
        }
        return builder.build();
    }

    public final Optional<CreateCommand> asCreateCommand() {
        return (this instanceof CreateCommand) ? Optional.of((CreateCommand) this) : Optional.empty();
    }

    public final Optional<ExerciseCommand> asExerciseCommand() {
        return (this instanceof ExerciseCommand) ? Optional.of((ExerciseCommand) this) : Optional.empty();
    }
}

class CommandUnknown extends RuntimeException {
    public CommandUnknown(Command command) {
        super("Command unknown " + command.toString());
    }
}

class ProtoCommandUnknown extends RuntimeException {
    public ProtoCommandUnknown(CommandsOuterClass.Command command) {
        super("Command unknown " + command.toString());
    }
}