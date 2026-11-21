// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionFilterOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

public final class TransactionFormat {

  private final EventFormat eventFormat;
  private final TransactionShape transactionShape;

  public static TransactionFormat fromProto(
      TransactionFilterOuterClass.TransactionFormat transactionFormat) {
    return new TransactionFormat(
        EventFormat.fromProto(transactionFormat.getEventFormat()),
        TransactionShape.fromProto(transactionFormat.getTransactionShape()));
  }

  public TransactionFilterOuterClass.TransactionFormat toProto() {
    return TransactionFilterOuterClass.TransactionFormat.newBuilder()
        .setEventFormat(eventFormat.toProto())
        .setTransactionShape(transactionShape.toProto())
        .build();
  }

  public EventFormat getEventFormat() {
    return eventFormat;
  }

  public TransactionShape getTransactionShape() {
    return transactionShape;
  }

  public TransactionFormat(
      @NonNull EventFormat eventFormat, @NonNull TransactionShape transactionShape) {
    this.eventFormat = eventFormat;
    this.transactionShape = transactionShape;
  }

  @Override
  public String toString() {
    return "TransactionFormat{"
        + "eventFormat="
        + eventFormat
        + ", transactionShape="
        + transactionShape
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransactionFormat that = (TransactionFormat) o;
    return Objects.equals(eventFormat, that.eventFormat)
        && Objects.equals(transactionShape, that.transactionShape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventFormat, transactionShape);
  }
}
