// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.Template;
import com.daml.ledger.javaapi.data.Value;

public abstract class CreateAnd implements Exercises<CreateAndExerciseCommand> {
  private final Template createArguments;

  protected CreateAnd(Template createArguments) {
    this.createArguments = createArguments;
  }

  @Override
  public final CreateAndExerciseCommand makeExerciseCmd(String choice, Value choiceArgument) {
    return new CreateAndExerciseCommand(
        getCompanion().TEMPLATE_ID, createArguments.toValue(), choice, choiceArgument);
  }

  public abstract ContractTypeCompanion getCompanion();
}
