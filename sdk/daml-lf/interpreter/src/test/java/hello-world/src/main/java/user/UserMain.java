// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package user;

import com.digitalasset.daml.lf.ledger.api;
import de.mirkosertic.bytecoder.api.Export;

public class UserMain {
  @Export("main")
  public static void main(String[] args) {
    api.logInfo("hello-world");
  }
}
