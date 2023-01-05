// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Command;
import java.util.List;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This is an interface describing classes that contains or can generate a list of {@link Command}
 */
public interface HasCommands {
  List<Command> commands();

  /** @hidden */
  static List<Command> toCommands(@NonNull List<@NonNull ? extends HasCommands> hasCommands) {
    return hasCommands.stream().flatMap(c -> c.commands().stream()).collect(Collectors.toList());
  }
}
