// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.{collection => sc}

private[scalautil] trait NonEmptyCollCompat {
  protected[this] type IterableOps[A, CC[_], C] = sc.IterableOps[A, CC, C]
  protected[this] type SeqOps[A, CC[_], C] = sc.SeqOps[A, CC, C]
}

