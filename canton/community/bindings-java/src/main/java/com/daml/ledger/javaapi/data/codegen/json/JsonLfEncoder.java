// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import java.io.IOException;

@FunctionalInterface
public interface JsonLfEncoder {
  public void encode(JsonLfWriter w) throws IOException;
}
