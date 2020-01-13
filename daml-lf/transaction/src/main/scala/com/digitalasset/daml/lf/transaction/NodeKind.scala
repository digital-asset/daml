// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

/** Transaction node kinds used in [[NodeInfo]] */
sealed trait NodeKind extends Product with Serializable
object NodeKind {
  case object Create extends NodeKind
  case object ExerciseConsuming extends NodeKind
  case object ExerciseNonConsuming extends NodeKind
  case object Fetch extends NodeKind
  case object LookupByKey extends NodeKind
}
