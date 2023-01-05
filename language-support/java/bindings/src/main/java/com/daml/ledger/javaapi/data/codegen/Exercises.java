// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

/**
 * Root of all generated {@code Exercises} interfaces for templates and Daml interfaces.
 *
 * @param <Cmd> The returned type of ledger command.
 */
public interface Exercises<Cmd> {
  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should call choice-specific {@code
   * exercise*} methods generated from their Daml code instead.
   *
   * @hidden
   */
  <A, R> Update<Exercised<R>> makeExerciseCmd(Choice<?, ? super A, R> choice, A choiceArgument);
}
