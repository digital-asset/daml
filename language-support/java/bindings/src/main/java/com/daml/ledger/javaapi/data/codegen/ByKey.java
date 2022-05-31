// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseByKeyCommand;
import com.daml.ledger.javaapi.data.Value;

public abstract class ByKey implements Exercises<ExerciseByKeyCommand> {
  protected final Value contractKey;

  protected ByKey(Value contractKey) {
    this.contractKey = contractKey;
  }

  @Override
  public final ExerciseByKeyCommand makeExerciseCmd(String choice, Value choiceArgument) {
    return new ExerciseByKeyCommand(
        getCompanion().TEMPLATE_ID, contractKey, choice, choiceArgument);
  }

  /** The origin of the choice, not the template relevant to contractKey. */
  protected abstract ContractTypeCompanion getCompanion();
}
