// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

package object block {

  /** Height used by the block stores before a height has been set.
    * The DB store should be initialized to this value in its initial migration.
    */
  val UninitializedBlockHeight = -1L
}
