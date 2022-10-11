// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.ExerciseByKeyCommand;

public class ExerciseByKeyUpdate<R> extends Update<R> {
  private final ExerciseByKeyCommand exerciseByKeyCommand;

  ExerciseByKeyUpdate(ExerciseByKeyCommand exerciseByKeyCommand) {
    this.exerciseByKeyCommand = exerciseByKeyCommand;
  }

  public ExerciseByKeyCommand getExerciseByKeyCommand() {
    return exerciseByKeyCommand;
  }

  @Override
  public Command command() {
    return exerciseByKeyCommand;
  }
}
