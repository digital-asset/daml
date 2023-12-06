// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

public final class UnsupportedEventTypeException extends RuntimeException {
  public UnsupportedEventTypeException(String eventStr) {
    super("Unsupported event " + eventStr);
  }
}
