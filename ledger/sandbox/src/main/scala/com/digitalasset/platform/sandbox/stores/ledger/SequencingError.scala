// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger
import java.time.Instant

import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

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
}
