// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ExerciseByKeyCommand extends Command {

  private final Identifier templateId;

  private final Value contractKey;

  private final String choice;

  private final Value choiceArgument;

  public ExerciseByKeyCommand(
      @NonNull Identifier templateId,
      @NonNull Value contractKey,
      @NonNull String choice,
      @NonNull Value choiceArgument) {
    this.templateId = templateId;
    this.contractKey = contractKey;
    this.choice = choice;
    this.choiceArgument = choiceArgument;
  }

  public static ExerciseByKeyCommand fromProto(CommandsOuterClass.ExerciseByKeyCommand command) {
    Identifier templateId = Identifier.fromProto(command.getTemplateId());
    Value contractKey = Value.fromProto(command.getContractKey());
    String choice = command.getChoice();
    Value choiceArgument = Value.fromProto(command.getChoiceArgument());
    return new ExerciseByKeyCommand(templateId, contractKey, choice, choiceArgument);
  }

  public CommandsOuterClass.ExerciseByKeyCommand toProto() {
    return CommandsOuterClass.ExerciseByKeyCommand.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setContractKey(this.contractKey.toProto())
        .setChoice(this.choice)
        .setChoiceArgument(this.choiceArgument.toProto())
        .build();
  }

  @NonNull
  @Override
  public Identifier getTemplateId() {
    return templateId;
  }

  @NonNull
  public Value getContractKey() {
    return contractKey;
  }

  @NonNull
  public String getChoice() {
    return choice;
  }

  @NonNull
  public Value getChoiceArgument() {
    return choiceArgument;
  }

  @Override
  public String toString() {
    return "ExerciseByKeyCommand{"
        + "templateId="
        + templateId
        + ", contractKey='"
        + contractKey
        + '\''
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
    ExerciseByKeyCommand that = (ExerciseByKeyCommand) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(contractKey, that.contractKey)
        && Objects.equals(choice, that.choice)
        && Objects.equals(choiceArgument, that.choiceArgument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateId, contractKey, choice, choiceArgument);
  }
}
