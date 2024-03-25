// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

/**
 * Base class of all decoded-to-codegen Daml records <em>with no type parameters</em>.
 *
 * <p>This category includes
 *
 * <ol>
 *   <li>all {@link com.daml.ledger.javaapi.data.Template} payloads,
 *   <li>all interface views, and
 *   <li>[by convention albeit not by rule] all choice arguments.
 * </ol>
 *
 * <p>Its encoded counterpart is {@link com.daml.ledger.javaapi.data.DamlRecord}, which can be
 * produced with {@link #toValue}.
 *
 * @param <T> A "self type", some subclass of this class that {@code T} implements.
 */
public abstract class DamlRecord<T> implements DefinedDataType<T> {
  public abstract com.daml.ledger.javaapi.data.DamlRecord toValue();
}
