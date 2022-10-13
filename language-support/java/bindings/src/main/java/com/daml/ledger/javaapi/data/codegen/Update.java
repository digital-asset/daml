// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;

public final class Update<R> {
  public Command command;

  public ValueDecoder<R> returnTypeDecoder;

  public Update(Command command, ValueDecoder<R> returnTypeDecoder) {
    this.command = command;
    this.returnTypeDecoder = returnTypeDecoder;
  }
}
