// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.javaapi.data.codegen.CreateAnd;

public abstract class Template {

  public abstract CreateCommand create();

  public abstract DamlRecord toValue();

  /**
   * Set up a {@link CreateAndExerciseCommand}; invoke an {@code exercise} method
   * on the result of this to finish creating the command, or convert to an
   * interface first with {@code toInterface} to invoke an interface {@code exercise}
   * method.
   */
  public abstract CreateAnd createAnd();
}
