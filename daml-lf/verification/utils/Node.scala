// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang._
import stainless.annotation._
import stainless.proof._
import stainless.collection._

import Value.ContractId

/** Generic transaction node type for both update transactions and the
  * transaction graph.
  */
sealed trait Node

object Node {

  sealed trait Action extends Node {
    def gkeyOpt: Option[GlobalKey]

    def byKey: Boolean
  }

  sealed trait LeafOnlyAction extends Action

  final case class Create(coid: ContractId, override val gkeyOpt: Option[GlobalKey])
      extends LeafOnlyAction {
    override def byKey: Boolean = false
  }

  final case class Fetch(
      coid: ContractId,
      override val gkeyOpt: Option[GlobalKey],
      override val byKey: Boolean,
  ) extends LeafOnlyAction

  final case class Exercise(
      targetCoid: ContractId,
      consuming: Boolean,
      children: List[NodeId],
      override val gkeyOpt: Option[GlobalKey],
      override val byKey: Boolean,
  ) extends Action

  final case class LookupByKey(gkey: GlobalKey, result: Option[ContractId]) extends LeafOnlyAction {
    override def gkeyOpt: Option[GlobalKey] = Some(gkey)

    override def byKey: Boolean = true
  }

  final case class Rollback(children: List[NodeId]) extends Node
}

final case class NodeId(index: BigInt)
