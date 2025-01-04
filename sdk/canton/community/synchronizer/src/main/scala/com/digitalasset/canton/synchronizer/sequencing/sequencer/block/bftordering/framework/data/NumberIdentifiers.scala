// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data

// The trick to have opaque subtypes of JVM-native types in Scala 2 is relatively simple:
//  provide only a constraint on the type without defining it explicitly, and construct it
//  via `asInstanceOf` from the concrete type.
//  Opaque subtypes of JVM-native types are erased at runtime (check the bytecode), so they
//  don't have any runtime overhead.
//
//  However, subtypes of `Int` are annoying because Scala seems to assume that they are actually subtypes of `Long`
//  as soon as any operation is done on them, e.g., with `T <: Int`, `T(0) + 1` ends up being a `Long`.

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
object NumberIdentifiers {

  type EpochNumber <: Long
  object EpochNumber {
    val First: EpochNumber = EpochNumber(0L)
    def apply(x: Long): EpochNumber = x.asInstanceOf[EpochNumber]
  }

  type EpochLength <: Long
  object EpochLength {
    def apply(x: Long): EpochLength = x.asInstanceOf[EpochLength]
  }

  type BlockNumber <: Long
  object BlockNumber {
    val First: BlockNumber = BlockNumber(0L)
    def apply(x: Long): BlockNumber = x.asInstanceOf[BlockNumber]
  }

  type ViewNumber <: Long
  object ViewNumber {
    val First: ViewNumber = ViewNumber(0L)
    def apply(x: Long): ViewNumber = x.asInstanceOf[ViewNumber]
  }
}
