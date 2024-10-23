// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger;

import de.mirkosertic.bytecoder.api.Import;

public class api {
  public static void logInfo(String msg) {
    internal.logInfo(msg.getBytes());
  }

  public static class internal {
    @Import(module = "env", name = "logInfo")
    public static native void logInfo(byte[] msg);
  }
}
