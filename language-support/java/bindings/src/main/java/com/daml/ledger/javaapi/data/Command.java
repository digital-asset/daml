// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.CommandsOuterClass;

import java.util.Optional;

public abstract class Command {

    abstract Identifier getTemplateId();

    public static Command fromProtoCommand(CommandsOuterClass.Command command) {
        switch (command.getCommandCase()) {
            case CREATE:
                return CreateCommand.fromProto(command.getCreate());
            case EXERCISE:
                return ExerciseCommand.fromProto(command.getExercise());
            case COMMAND_NOT_SET:
            default:
                throw new ProtoCommandUnknown(command);
        }
    }

    public CommandsOuterClass.Command toProtoCommand() {
        if (this instanceof CreateCommand) {
            CreateCommand command = (CreateCommand) this;
            return CommandsOuterClass.Command.newBuilder()
                    .setCreate(command.toProto())
                    .build();
        } else if (this instanceof ExerciseCommand) {
            ExerciseCommand command = (ExerciseCommand) this;
            return CommandsOuterClass.Command.newBuilder()
                    .setExercise(command.toProto())
                    .build();
        } else {
            throw new CommandUnknown(this);
        }
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