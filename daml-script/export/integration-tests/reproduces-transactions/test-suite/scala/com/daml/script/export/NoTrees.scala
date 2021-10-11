// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

final class NoTrees extends ReproducesTransactions {
  val numParties = 2
  val skip = 4
  val description = "no trees"
}
