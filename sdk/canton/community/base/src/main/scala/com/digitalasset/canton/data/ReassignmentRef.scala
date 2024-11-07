// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.ReassignmentRef.*
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}

sealed trait ReassignmentRef {
  override def toString: String = this match {
    case ContractIdRef(cid) => s"unassignment of $cid"
    case ReassignmentIdRef(rid) => s"assignment $rid"
  }
}

object ReassignmentRef {

  /** Used when submitting an unassignment (the reassignmentId is not yet known) */
  final case class ContractIdRef(contractId: LfContractId) extends ReassignmentRef

  /** Used when submitting an assignment */
  final case class ReassignmentIdRef(reassignmentId: ReassignmentId) extends ReassignmentRef

  def apply(cid: LfContractId): ReassignmentRef = ContractIdRef(cid)
  def apply(rid: ReassignmentId): ReassignmentRef = ReassignmentIdRef(rid)
}
