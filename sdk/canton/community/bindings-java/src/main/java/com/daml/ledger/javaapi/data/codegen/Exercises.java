// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Unit;

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

  /**
   * Adds {@link #exerciseArchive} to every exercise target.
   *
   * <p>The goal is to correct the problem of having {@link ContractId} implement {@link
   * #makeExerciseCmd} directly. That was a mistake, but one at least obviated by the fact that
   * {@link #makeExerciseCmd} is clearly part of the internal API, so if you use it directly anyway
   * and you get weird exceptions, you get to keep both pieces of your program. With {@link
   * #exerciseArchive} we can correct the problem by having {@link Archivable} be the <em>real</em>
   * {@link Exercises} interface, and the distinction can be flattened when breaking compatibility.
   */
  interface Archivable<Cmd> extends Exercises<Cmd> {
    Update<Exercised<Unit>> exerciseArchive();
  }
}
