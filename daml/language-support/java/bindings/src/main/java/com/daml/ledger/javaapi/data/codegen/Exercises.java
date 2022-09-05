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
  Cmd makeExerciseCmd(String choice, Value choiceArgument);
}
