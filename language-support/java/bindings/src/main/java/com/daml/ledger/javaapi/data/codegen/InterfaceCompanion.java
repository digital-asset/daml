// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

/**
 * Metadata and utilities associated with an interface as a whole. Its subclasses serve to
 * disambiguate various generated {@code toInterface} overloads.
 *
 * @param <I> The generated interface marker class.
 */
public abstract class InterfaceCompanion<I> {
  protected InterfaceCompanion() {}
}
