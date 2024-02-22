// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
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

  final JsonLfDecoder<ArgType> argJsonDecoder;
  final JsonLfDecoder<ResType> resultJsonDecoder;

  private Choice(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ArgType> argTypeDecoder,
      ValueDecoder<ResType> returnTypeDecoder,
      JsonLfDecoder<ArgType> argJsonDecoder,
      JsonLfDecoder<ResType> resultJsonDecoder) {
    this.name = name;
    this.encodeArg = encodeArg;
    this.argTypeDecoder = argTypeDecoder;
    this.returnTypeDecoder = returnTypeDecoder;
    this.argJsonDecoder = argJsonDecoder;
    this.resultJsonDecoder = resultJsonDecoder;
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the generated
   * {@code CHOICE_*} fields on templates or interfaces.
   *
   * <p>TODO(raphael-speyer-da): Delete this method altogether, once codegen uses the other one.
   *
   * @hidden
   */
  public static <Tpl, ArgType, ResType> Choice<Tpl, ArgType, ResType> create(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ArgType> argTypeDecoder,
      ValueDecoder<ResType> returnTypeDecoder) {
    return create(name, encodeArg, argTypeDecoder, returnTypeDecoder, null, null);
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should refer to the generated
   * {@code CHOICE_*} fields on templates or interfaces.
   *
   * @hidden
   */
  public static <Tpl, ArgType, ResType> Choice<Tpl, ArgType, ResType> create(
      final String name,
      final Function<ArgType, Value> encodeArg,
      ValueDecoder<ArgType> argTypeDecoder,
      ValueDecoder<ResType> returnTypeDecoder,
      JsonLfDecoder<ArgType> argJsonDecoder,
      JsonLfDecoder<ResType> resultJsonDecoder) {
    return new Choice<>(
        name, encodeArg, argTypeDecoder, returnTypeDecoder, argJsonDecoder, resultJsonDecoder);
  }
}
