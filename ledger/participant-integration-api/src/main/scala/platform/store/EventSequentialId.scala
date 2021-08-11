// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

object EventSequentialId {
  /** The sequential id to use if there are no events in the index database. */
  val zero: Long = 0L
}