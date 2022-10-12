// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;

public class CreateAndExerciseUpdate<R> extends Update<R> {
  private final CreateAndExerciseCommand createAndExerciseCommand;
  private final ValueDecoder<R> returnTypeDecoder;

  CreateAndExerciseUpdate(
      CreateAndExerciseCommand createAndExerciseCommand, ValueDecoder<R> returnTypeDecoder) {
    this.createAndExerciseCommand = createAndExerciseCommand;
    this.returnTypeDecoder = returnTypeDecoder;
  }

  public CreateAndExerciseCommand getCreateAndExerciseUpdate() {
    return createAndExerciseCommand;
  }

  @Override
  public Command command() {
    return createAndExerciseCommand;
  }

  @Override
  public ValueDecoder<R> returnTypeDecoder() {
    return returnTypeDecoder;
  }
}
