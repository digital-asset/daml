// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.ledger.offset.Offset

/** Type aliases used throughout the package */
package object platform {
  type PruneBuffers = Offset => Unit
  val PruneBuffersNoOp: PruneBuffers = _ => ()
}
