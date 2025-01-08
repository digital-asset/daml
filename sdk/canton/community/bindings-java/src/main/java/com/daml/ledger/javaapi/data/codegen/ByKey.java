// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseByKeyCommand;
import com.daml.ledger.javaapi.data.PrefetchContractKey;
import com.daml.ledger.javaapi.data.Value;

/** Parent of all generated {@code ByKey} classes within templates and interfaces. */
public abstract class ByKey implements Exercises<ExerciseByKeyCommand> {
  protected final Value contractKey;

  protected ByKey(Value contractKey) {
    this.contractKey = contractKey;
  }

  @Override
  public <A, R> Update<Exercised<R>> makeExerciseCmd(
      Choice<?, ? super A, R> choice, A choiceArgument) {
    var command =
        new ExerciseByKeyCommand(
            getCompanion().TEMPLATE_ID,
            contractKey,
            choice.name,
            choice.encodeArg.apply(choiceArgument));
    return new Update.ExerciseUpdate<>(command, x -> x, choice.returnTypeDecoder);
  }

  /** The origin of the choice, not the template relevant to contractKey. */
  protected abstract ContractTypeCompanion<?, ?, ?, ?> getCompanion();

  /** The key of this contract in the format it can be used for prefetching */
  public PrefetchContractKey toPrefetchKey() {
    return new PrefetchContractKey(this.getCompanion().TEMPLATE_ID, this.contractKey);
  }

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
    public <A, R> Update<Exercised<R>> makeExerciseCmd(
        Choice<?, ? super A, R> choice, A choiceArgument) {
      // TODO(i15638) use getCompanion().TEMPLATE_ID as the interface ID
      var command =
          new ExerciseByKeyCommand(
              keySource.TEMPLATE_ID,
              contractKey,
              choice.name,
              choice.encodeArg.apply(choiceArgument));
      return new Update.ExerciseUpdate<>(command, x -> x, choice.returnTypeDecoder);
    }
  }
}
