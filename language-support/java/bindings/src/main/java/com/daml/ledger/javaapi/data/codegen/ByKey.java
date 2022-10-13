// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseByKeyCommand;
import com.daml.ledger.javaapi.data.Value;

/** Parent of all generated {@code ByKey} classes within templates and interfaces. */
public abstract class ByKey implements Exercises<ExerciseByKeyCommand> {
  protected final Value contractKey;

  protected ByKey(Value contractKey) {
    this.contractKey = contractKey;
  }

  @Override
  public ExerciseByKeyCommand makeExerciseCmd(String choice, Value choiceArgument) {
    return new ExerciseByKeyCommand(
        getCompanion().TEMPLATE_ID, contractKey, choice, choiceArgument);
  }

  /** The origin of the choice, not the template relevant to contractKey. */
  protected abstract ContractTypeCompanion<?, ?> getCompanion();

  /**
   * Parent of all generated {@code ByKey} classes within interfaces. These need to pass both the
   * template and interface ID.
   */
  public abstract static class ToInterface extends ByKey {
    private final ContractCompanion<?, ?, ?> keySource;

    protected ToInterface(ContractCompanion<?, ?, ?> keySource, Value contractKey) {
      super(contractKey);
      this.keySource = keySource;
    }

    @Override
    public ExerciseByKeyCommand makeExerciseCmd(String choice, Value choiceArgument) {
      // TODO #14056 use getCompanion().TEMPLATE_ID as the interface ID
      return new ExerciseByKeyCommand(keySource.TEMPLATE_ID, contractKey, choice, choiceArgument);
    }
  }
}
