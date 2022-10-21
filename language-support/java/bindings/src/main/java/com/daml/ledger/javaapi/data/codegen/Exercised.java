// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExercisedEvent;

public class Exercised<R> {
  public final R exerciseResult;

  private Exercised(R exerciseResult) {
    this.exerciseResult = exerciseResult;
  }

  public static <R> Exercised<R> fromEvent(
      ValueDecoder<R> returnTypeDecoder, ExercisedEvent exercisedEvent) {
    return new Exercised<>(returnTypeDecoder.decode(exercisedEvent.getExerciseResult()));
  }
}
