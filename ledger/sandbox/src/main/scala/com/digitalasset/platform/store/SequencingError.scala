// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value.AbsoluteContractId

sealed trait SequencingError extends Product with Serializable {

  /**
    * True if submitting the command again can't be successful even with a changed ledger effective time.
    * @return
    */
  def isFinal: Boolean
}

object SequencingError {

  sealed trait PredicateType extends Product with Serializable

  object PredicateType {

    final case object Exercise extends PredicateType

    final case object Fetch extends PredicateType

  }

  final case class InactiveDependencyError(cid: AbsoluteContractId, predicateType: PredicateType)
      extends SequencingError {

    override def isFinal: Boolean = true
  }

  final case class TimeBeforeError(
      cid: AbsoluteContractId,
      time: Instant,
      let: Instant,
      predicateType: PredicateType)
      extends SequencingError {

    override def isFinal: Boolean = false
  }

  final case class DuplicateKey(gk: GlobalKey) extends SequencingError {
    override def isFinal: Boolean = true
  }

  final case class InvalidLookup(
      gk: GlobalKey,
      cid: Option[AbsoluteContractId],
      currentCid: Option[AbsoluteContractId]
  ) extends SequencingError {
    override def isFinal: Boolean = true
  }
}
