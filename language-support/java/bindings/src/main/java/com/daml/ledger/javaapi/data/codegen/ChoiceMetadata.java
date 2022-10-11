// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import java.util.function.Function;

public final class ChoiceMetadata<Tpl, ArgType, ResType> {
  public final String name;
  public final Function<ArgType, Value> encodeArg;
  private final ValueDecoder<ResType> valueDecoder;

  private ChoiceMetadata(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ResType> valueDecoder) {
    this.name = name;
    this.encodeArg = encodeArg;
    this.valueDecoder = valueDecoder;
  }

  public static <Tpl, ArgType, ResType> ChoiceMetadata<Tpl, ArgType, ResType> create(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ResType> valueDecoder) {
    return new ChoiceMetadata<>(name, encodeArg, valueDecoder);
  }
}
