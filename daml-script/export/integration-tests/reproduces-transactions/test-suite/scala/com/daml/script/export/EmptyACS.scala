// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

final class EmptyACS extends ReproducesTransactions {
  val numParties = 2
  val skip = 0
  val description = "empty ACS"
}
