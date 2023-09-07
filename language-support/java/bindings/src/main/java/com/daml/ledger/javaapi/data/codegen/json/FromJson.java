// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;

// Functional interface, that can be used to either read a value of the given type directly
// (using .read()), or can be used to build a FromJson for types with generic arguments,
// to tell them how to read that argument type.
//
// e.g.
//   String str = reader.text().read();
// or
//   List<String> = reader.list(reader::text).read();
public interface FromJson<T> {
  public T read() throws Error;

  public static class Error extends IOException {
    public Error(String msg) {
      super(msg);
    }
  }
}
