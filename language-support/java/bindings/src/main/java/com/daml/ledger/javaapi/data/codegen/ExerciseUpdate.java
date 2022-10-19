// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import java.util.function.Function;

public final class ExerciseUpdate<R> extends Update<R> {
  public Command command;

  public ValueDecoder<R> returnTypeDecoder;

  public ExerciseUpdate(Command command, ValueDecoder<R> returnTypeDecoder) {
    super(command);
    this.returnTypeDecoder = returnTypeDecoder;
  }

  @Override
  public <T> T match(Function<Create<R>, T> created, Function<ExerciseUpdate<R>, T> exercise) {
    return exercise.apply(this);
  }
}
