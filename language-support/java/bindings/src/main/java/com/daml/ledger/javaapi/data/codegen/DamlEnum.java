// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

/**
 * Base interface of all decoded-to-codegen Daml enums.
 *
 * <p>Its encoded counterpart is {@link com.daml.ledger.javaapi.data.DamlEnum}, which can be
 * produced with {@link #toValue}.
 *
 * @param <T> A "self type", the {@code enum} that implements this interface.
 */
public interface DamlEnum<T> extends DefinedDataType<T> {
  com.daml.ledger.javaapi.data.DamlEnum toValue();
}
