// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

/**
 * Base class of all decoded-to-codegen Daml variants <em>with no type parameters</em>.
 *
 * <p>Its encoded counterpart is {@link com.daml.ledger.javaapi.data.Variant}, which can be produced
 * with {@link #toValue}.
 *
 * @param <T> A "self type", some subclass of this class that {@code T} implements.
 */
public abstract class Variant<T> implements DefinedDataType<T> {
  public abstract com.daml.ledger.javaapi.data.Variant toValue();
}
