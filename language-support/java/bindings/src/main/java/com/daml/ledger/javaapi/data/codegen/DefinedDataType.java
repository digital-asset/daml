// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;

/**
 * The codegen-decoded form of any of these:
 *
 * <ol>
 *   <li>what {@link DamlRecord} describes,
 *   <li>a {@link Variant} without type parameters, or
 *   <li>any {@link DamlEnum}.
 * </ol>
 *
 * <p>Its encoded counterpart is {@link com.daml.ledger.javaapi.data.Value}, which can be produced
 * with {@link #toValue}.
 *
 * @param <T> A "self type", some subclass of this interface that {@code T} implements.
 */
public interface DefinedDataType<T> {
  /** Produce the encoded form. */
  Value toValue();
}
