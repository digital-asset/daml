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
}
