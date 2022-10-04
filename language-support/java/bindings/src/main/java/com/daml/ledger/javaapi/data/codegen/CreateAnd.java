// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.Template;
import com.daml.ledger.javaapi.data.Value;

/** Parent of all generated {@code CreateAnd} classes within templates and interfaces. */
public abstract class CreateAnd implements Exercises<CreateAndExerciseCommand> {
  protected final Template createArguments;

  protected CreateAnd(Template createArguments) {
    this.createArguments = createArguments;
  }

  @Override
  public CreateAndExerciseCommand makeExerciseCmd(String choice, Value choiceArgument) {
    return new CreateAndExerciseCommand(
        getCompanion().TEMPLATE_ID, createArguments.toValue(), choice, choiceArgument);
  }

  /** The origin of the choice, not the createArguments. */
  protected abstract ContractTypeCompanion<?, ?> getCompanion();

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
    public final CreateAndExerciseCommand makeExerciseCmd(String choice, Value choiceArgument) {
      // TODO #14056 use getCompanion().TEMPLATE_ID as the interface ID
      return new CreateAndExerciseCommand(
          createSource.TEMPLATE_ID, createArguments.toValue(), choice, choiceArgument);
    }
  }
}
