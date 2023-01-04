// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandsOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ExerciseCommand extends Command {

  private final Identifier templateId;

  private final String contractId;

  private final String choice;

  private final Value choiceArgument;

  public ExerciseCommand(
      @NonNull Identifier templateId,
      @NonNull String contractId,
      @NonNull String choice,
      @NonNull Value choiceArgument) {
    this.templateId = templateId;
    this.contractId = contractId;
    this.choice = choice;
    this.choiceArgument = choiceArgument;
  }

  public static ExerciseCommand fromProto(CommandsOuterClass.ExerciseCommand command) {
    Identifier templateId = Identifier.fromProto(command.getTemplateId());
    String contractId = command.getContractId();
    String choice = command.getChoice();
    Value choiceArgument = Value.fromProto(command.getChoiceArgument());
    return new ExerciseCommand(templateId, contractId, choice, choiceArgument);
  }

  public CommandsOuterClass.ExerciseCommand toProto() {
    return CommandsOuterClass.ExerciseCommand.newBuilder()
        .setTemplateId(this.templateId.toProto())
        .setContractId(this.contractId)
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
  public String getContractId() {
    return contractId;
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
    return "ExerciseCommand{"
        + "templateId="
        + templateId
        + ", contractId='"
        + contractId
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
    ExerciseCommand that = (ExerciseCommand) o;
    return Objects.equals(templateId, that.templateId)
        && Objects.equals(contractId, that.contractId)
        && Objects.equals(choice, that.choice)
        && Objects.equals(choiceArgument, that.choiceArgument);
  }

  @Override
  public int hashCode() {

    return Objects.hash(templateId, contractId, choice, choiceArgument);
  }
}
