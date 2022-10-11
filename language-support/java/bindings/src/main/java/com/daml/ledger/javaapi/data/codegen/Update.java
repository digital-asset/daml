// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;

public abstract class Update<R> {

  public abstract Command command();

  //    public final Optional<CreateCommand> asCreateCommand() {
  //        return this instanceof CreateCommand ? Optional.of((CreateCommand)this) :
  // Optional.empty();
  //    }
  //
  //    public final Optional<ExerciseUpdate<R>> asExerciseCommand() {
  //        return this instanceof ExerciseUpdate ? Optional.of((ExerciseUpdate<R>)this) :
  // Optional.empty();
  //    }
}
