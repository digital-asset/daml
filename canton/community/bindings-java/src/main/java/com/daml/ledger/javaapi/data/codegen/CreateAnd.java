// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.Template;

/** Parent of all generated {@code CreateAnd} classes within templates and interfaces. */
public abstract class CreateAnd implements Exercises<CreateAndExerciseCommand> {
  protected final Template createArguments;

  protected CreateAnd(Template createArguments) {
    this.createArguments = createArguments;
  }

  @Override
  public <A, R> Update<Exercised<R>> makeExerciseCmd(
      Choice<?, ? super A, R> choice, A choiceArgument) {
    var command =
        new CreateAndExerciseCommand(
            getCompanion().TEMPLATE_ID,
            createArguments.toValue(),
            choice.name,
            choice.encodeArg.apply(choiceArgument));
    return new Update.ExerciseUpdate<>(command, x -> x, choice.returnTypeDecoder);
  }

  /** The origin of the choice, not the createArguments. */
  protected abstract ContractTypeCompanion<?, ?, ?, ?> getCompanion();

  /**
   * Parent of all generated {@code CreateAnd} classes within interfaces. These need to pass both
   * the template and interface ID.
   */
  public abstract static class ToInterface extends CreateAnd {
    private final ContractCompanion<?, ?, ?> createSource;

    protected ToInterface(ContractCompanion<?, ?, ?> createSource, Template createArguments) {
      super(createArguments);
      this.createSource = createSource;
    }

    @Override
    public final <A, R> Update<Exercised<R>> makeExerciseCmd(
        Choice<?, ? super A, R> choice, A choiceArgument) {
      // TODO i15638 use getCompanion().TEMPLATE_ID as the interface ID
      var command =
          new CreateAndExerciseCommand(
              createSource.TEMPLATE_ID,
              createArguments.toValue(),
              choice.name,
              choice.encodeArg.apply(choiceArgument));
      return new Update.ExerciseUpdate<>(command, x -> x, choice.returnTypeDecoder);
    }
  }
}
