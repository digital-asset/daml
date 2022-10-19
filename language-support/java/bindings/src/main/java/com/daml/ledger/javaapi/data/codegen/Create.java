// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import java.util.function.Function;

public final class Create<CtId> extends Update<CtId> {
  public Command command;

  public Function<String, CtId> createdContractId;

  public Create(Command command, Function<String, CtId> createdContractId) {
    super(command);
    this.createdContractId = createdContractId;
  }

  @Override
  public <T> T match(
      Function<Create<CtId>, T> created, Function<ExerciseUpdate<CtId>, T> exercise) {
    return created.apply(this);
  }
}
