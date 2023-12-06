// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CreateAndExerciseCommand extends Command {
  private final Identifier templateId;

  private final DamlRecord createArguments;

  private final String choice;

  private final Value choiceArgument;

  public CreateAndExerciseCommand(
      @NonNull Identifier templateId,
      @NonNull DamlRecord createArguments,
      @NonNull String choice,
      @NonNull Value choiceArgument) {
    this.templateId = templateId;
    this.createArguments = createArguments;
    this.choice = choice;
    this.choiceArgument = choiceArgument;
  }

  public static CreateAndExerciseCommand fromProto(
      CommandsOuterClass.CreateAndExerciseCommand command) {
    Identifier templateId = Identifier.fromProto(command.getTemplateId());
    DamlRecord createArguments = DamlRecord.fromProto(command.getCreateArguments());
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

  public DamlRecord getCreateArguments() {
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
    return "CreateAndExerciseCommand{"
        + "templateId="
        + templateId
        + ", createArguments="
        + createArguments
        + ", choice='"
        + choice
        + '\''
        + ", choiceArgument="
        + choiceArgument
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateAndExerciseCommand that = (CreateAndExerciseCommand) o;
    return templateId.equals(that.templateId)
        && createArguments.equals(that.createArguments)
        && choice.equals(that.choice)
        && choiceArgument.equals(that.choiceArgument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, createArguments, choice, choiceArgument);
  }
}
