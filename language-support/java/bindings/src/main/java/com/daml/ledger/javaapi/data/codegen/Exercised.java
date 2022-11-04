// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExercisedEvent;

/**
 * This class contains information related to the result after a choice is exercised.
 *
 * <p>Application code <em>should not</em> instantiate or subclass;
 *
 * @param <R> The type of exercise result
 */
public final class Exercised<R> {
  public final R exerciseResult;

  private Exercised(R exerciseResult) {
    this.exerciseResult = exerciseResult;
  }

  /** @hidden */
  public static <R> Exercised<R> fromEvent(
      ValueDecoder<R> returnTypeDecoder, ExercisedEvent exercisedEvent) {
    return new Exercised<>(returnTypeDecoder.decode(exercisedEvent.getExerciseResult()));
  }

  @Override
  public String toString() {
    return "Exercised{" + "exerciseResult=" + exerciseResult + '}';
  }
}
