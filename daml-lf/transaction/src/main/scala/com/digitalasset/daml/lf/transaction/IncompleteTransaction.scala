// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref.Location

trait IncompleteTransaction {

  type Nid = NodeId
  type TX = GenTransaction
  type ExerciseNode = Node.NodeExercises

  def transaction: TX

  def locationInfo: Map[Nid, Location]
}
