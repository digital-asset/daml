// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import java.util.function.Function;

public abstract class Update<R> {
  public Command command;

  public Update(Command command) {
    this.command = command;
  }

  public abstract <T> T match(
      Function<Create<R>, T> created, Function<ExerciseUpdate<R>, T> exercise);
}
