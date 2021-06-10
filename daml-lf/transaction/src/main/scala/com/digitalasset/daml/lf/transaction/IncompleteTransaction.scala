// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.value.Value

trait IncompleteTransaction {

  type Nid = NodeId
  type Cid = Value.ContractId
  type TX = GenTransaction[Nid, Cid]
  type ExerciseNode = Node.NodeExercises[Nid, Cid]

  def transaction: TX
  def exerciseContextMaybe: Option[ExerciseNode]
}
