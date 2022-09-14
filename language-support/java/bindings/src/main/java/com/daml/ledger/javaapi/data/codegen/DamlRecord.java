// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

public abstract class DamlRecord<T> implements DefinedDataType<T> {
  public abstract com.daml.ledger.javaapi.data.DamlRecord toValue();
}
