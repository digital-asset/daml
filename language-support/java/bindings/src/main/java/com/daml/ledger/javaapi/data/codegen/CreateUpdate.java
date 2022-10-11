// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.CreateCommand;

public class CreateUpdate<R> extends Update<R> {
  private final CreateCommand createCommand;

  CreateUpdate(CreateCommand createCommand) {
    this.createCommand = createCommand;
  }

  public CreateCommand getCreateCommand() {
    return createCommand;
  }

  @Override
  public Command command() {
    return createCommand;
  }
}
