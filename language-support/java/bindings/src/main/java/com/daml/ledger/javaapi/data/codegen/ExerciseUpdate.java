// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.ExerciseCommand;

public class ExerciseUpdate<R> extends Update<R> {
  private final ExerciseCommand exerciseCommand;

  ExerciseUpdate(ExerciseCommand exerciseCommand) {
    this.exerciseCommand = exerciseCommand;
  }

  public ExerciseCommand getExerciseCommand() {
    return exerciseCommand;
  }

  @Override
  public Command command() {
    return exerciseCommand;
  }
}
