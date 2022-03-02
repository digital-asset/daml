// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

final class SkipSplit extends ReproducesTransactions {
  val numParties = 2
  val skip = 2
  val description = "skip split"
}
