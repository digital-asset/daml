// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import java.util.function.Function;

/**
 * This represents a Daml choice that can be exercised on {@link ContractId}s of type {@code
 * ContractId<Tpl>}.
 *
 * @param <Tpl> The generated template class or marker interface for a Daml interface
 * @param <ArgType> The choice's argument type
 * @param <ResType> The result from exercising the choice
 */
public final class Choice<Tpl, ArgType, ResType> {

  /** The choice name * */
  public final String name;

  final Function<ArgType, Value> encodeArg;

  final ValueDecoder<ArgType> argTypeDecoder;
  final ValueDecoder<ResType> returnTypeDecoder;

  private Choice(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ArgType> argTypeDecoder,
      ValueDecoder<ResType> returnTypeDecoder) {
    this.name = name;
    this.encodeArg = encodeArg;
    this.argTypeDecoder = argTypeDecoder;
    this.returnTypeDecoder = returnTypeDecoder;
  }

  /**
   * @hidden <strong>INTERNAL API</strong>: this is meant for use by <a
   *     href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code
   *     generator</a>, and <em>should not be referenced directly</em>. Applications should refer to
   *     the generated {@code CHOICE_*} fields on templates or interfaces.
   */
  public static <Tpl, ArgType, ResType> Choice<Tpl, ArgType, ResType> create(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ArgType> argTypeDecoder,
      ValueDecoder<ResType> returnTypeDecoder) {
    return new Choice<>(name, encodeArg, argTypeDecoder, returnTypeDecoder);
  }
}
