// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.List;

public class EventUtils {

  private EventUtils() {}

  /** @hidden */
  public static CreatedEvent singleCreatedEvent(List<? extends Event> events) {
    if (events.size() == 1 && events.get(0) instanceof CreatedEvent)
      return (CreatedEvent) events.get(0);
    throw new IllegalArgumentException(
        "Expected exactly one created event from the transaction, got: " + events);
  }

  /** @hidden */
  public static ExercisedEvent firstExercisedEvent(TransactionTree txTree) {
    var maybeExercisedEvent =
        txTree.getRootNodeIds().stream()
            .map(nodeId -> txTree.getEventsById().get(nodeId))
            .filter(e -> e instanceof ExercisedEvent)
            .map(e -> (ExercisedEvent) e)
            .findFirst();

    return maybeExercisedEvent.orElseThrow(
        () ->
            new IllegalArgumentException("Expect an exercised event but not found. tx: " + txTree));
  }
}
