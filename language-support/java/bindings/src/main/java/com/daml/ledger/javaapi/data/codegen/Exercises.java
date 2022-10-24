// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;

/**
 * Root of all generated {@code Exercises} interfaces for templates and Daml interfaces.
 *
 * @param <Cmd> The returned type of ledger command.
 */
public interface Exercises<Cmd> {
  /**
   * @hidden
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should call choice-specific {@code
   * exercise*} methods generated from their Daml code instead.
   */
  Cmd makeExerciseCmd(String choice, Value choiceArgument);
}
