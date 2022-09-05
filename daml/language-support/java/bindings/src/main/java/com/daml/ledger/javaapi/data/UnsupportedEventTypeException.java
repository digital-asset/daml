// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

public class UnsupportedEventTypeException extends RuntimeException {
  public UnsupportedEventTypeException(String eventStr) {
    super("Unsupported event " + eventStr);
  }
}
