// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import java.util.function.Function;

/**
 * A converter from the encoded form of a Daml value, represented by {@link Value}, to the
 * codegen-decoded form, represented by {@code Data}.
 *
 * <p>Every codegen class for a template, record, or variant includes a {@code valueDecoder} method
 * that produces one of these. If the data type has type parameters, {@code valueDecoder} has
 * arguments that correspond to {@link ValueDecoder}s for those type arguments. For primitive types
 * that are not code-generated, see {@link PrimitiveValueDecoders}.
 *
 * <pre>
 * // given template 'Foo', and encoded payload 'Value fooValue'
 * Foo foo = Foo.valueDecoder().decode(fooValue);
 *
 * // given Daml datatypes 'Bar a b' and 'Baz',
 * // and encoded 'Bar' 'Value barValue'
 * Bar&lt;Baz, Long> bar = Bar.valueDecoder(
 *     Baz.valueDecoder(), PrimitiveValueDecoders.fromInt64)
 *   .decode(barValue);
 * </pre>
 *
 * @param <Data> The codegen or primitive type that this decodes a {@link Value} to.
 */
@FunctionalInterface
public interface ValueDecoder<Data> {
  /** @see ValueDecoder */
  Data decode(Value value);

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should pass this {@link
   * ValueDecoder} as an argument to a code-generated {@code valueDecoder} method instead.
   */
  default ContractId<Data> fromContractId(String contractId) {
    throw new IllegalArgumentException("Cannot create contract id for this data type");
  }

  // TODO #15120 delete
  /**
   * @deprecated since Daml 2.5.0; it is only used in deprecated fromValue method of all generated
   *     data class
   */
  @Deprecated
  static <A> ValueDecoder<A> fromFunction(Function<Value, A> fromValue) {
    return new ValueDecoder<A>() {
      @Override
      public A decode(Value value) {
        return fromValue.apply(value);
      }

      @Override
      public ContractId<A> fromContractId(String contractId) {
        return new ContractId<>(contractId);
      }
    };
  }
}
