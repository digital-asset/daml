// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.CreateCommand;

public class CreateUpdate<R> extends Update<R> {
  private final CreateCommand createCommand;
  private final ValueDecoder<R> returnTypeDecoder;

  CreateUpdate(CreateCommand createCommand, ValueDecoder<R> returnTypeDecoder) {
    this.createCommand = createCommand;
    this.returnTypeDecoder = returnTypeDecoder;
  }

  public CreateCommand getCreateCommand() {
    return createCommand;
  }

  @Override
  public Command command() {
    return createCommand;
  }

  @Override
  public ValueDecoder<R> returnTypeDecoder() {
    return returnTypeDecoder;
  }
}
