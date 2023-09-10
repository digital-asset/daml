// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;

@FunctionalInterface
public interface JsonLfDecoder<T> {
  public T decode(JsonLfReader r) throws Error;

  public static class Error extends IOException {
    public Error(String msg) {
      super(msg);
    }
  }
}
