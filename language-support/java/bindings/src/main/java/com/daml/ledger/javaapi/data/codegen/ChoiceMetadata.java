// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import java.util.function.Function;

public final class ChoiceMetadata<Tpl, ArgType, ResType> {
  public final String name;
  private final Function<ArgType, Value> encodeArg;

  private ChoiceMetadata(final String name, final Function<ArgType, Value> encodeArg) {
    this.name = name;
    this.encodeArg = encodeArg;
  }

  public static <Tpl, ArgType, ResType> ChoiceMetadata<Tpl, ArgType, ResType> create(
      final String name, final Function<ArgType, Value> encodeArg) {
    return new ChoiceMetadata<>(name, encodeArg);
  }
}
