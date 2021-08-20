// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

object Concurrent {

  /** Equal to a Semaphore with max count equal to one. */
  final case class Mutex() extends java.util.concurrent.Semaphore(1, true)
}
