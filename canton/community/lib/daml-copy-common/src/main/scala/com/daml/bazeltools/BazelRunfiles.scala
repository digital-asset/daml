// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.bazeltools

object BazelRunfiles {
  // Canton repo doesn't have Bazeltools which is used by the DamlPorts library
  // The Daml Ports library is in turn used by the Ledger API server
  // As we know that this is just used to look up sysctl, we override
  // the symbol here and return something that will likely work ...
  // TODO(#11229) remove me
  def rlocation(input: String): String =
    if (input.endsWith("sysctl")) "sysctl" else input
}
