// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;
import java.util.function.Function;

@FunctionalInterface
public interface ValueDecoder<Data> {
  Data decode(Value value);

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
    return new ValueDecoder<>() {
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
