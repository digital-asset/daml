// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import java.util.function.Function;

public abstract class Update<U> {
  public Command command;

  public Update(Command command) {
    this.command = command;
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be instantiated directly</em>. Applications should only obtain this {@link
   * Update} instantiated generated java classes by calling create and exercise methods.
   */
  public static final class ExerciseUpdate<R, U> extends Update<U> {
    public final Function<Exercised<R>, U> k;
    public final ValueDecoder<R> returnTypeDecoder;

    public ExerciseUpdate(
        Command command, Function<Exercised<R>, U> k, ValueDecoder<R> returnTypeDecoder) {
      super(command);
      this.k = k;
      this.returnTypeDecoder = returnTypeDecoder;
    }
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be instantiated directly</em>. Applications should only obtain this {@link
   * Update} instantiated generated java classes by calling create and exercise methods.
   */
  public static final class CreateUpdate<CtId, U> extends Update<U> {
    public final Function<Created<CtId>, U> k;
    public final Function<String, CtId> createdContractId;

    public CreateUpdate(
        Command command, Function<Created<CtId>, U> k, Function<String, CtId> createdContractId) {
      super(command);
      this.k = k;
      this.createdContractId = createdContractId;
    }
  }
}
