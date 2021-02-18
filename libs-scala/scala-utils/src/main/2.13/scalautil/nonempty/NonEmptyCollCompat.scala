// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import scala.{collection => sc}

private[nonempty] object NonEmptyCollCompat {
  type IterableOps[+A, +CC[_], +C] = sc.IterableOps[A, CC, C]
  type SeqOps[+A, +CC[_], +C] = sc.SeqOps[A, CC, C]
}
