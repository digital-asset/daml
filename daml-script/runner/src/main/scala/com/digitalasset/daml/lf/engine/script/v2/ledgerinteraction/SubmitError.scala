// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.v2.ledgerinteraction

import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.Time
import com.daml.lf.ledger.EventId
import com.daml.lf.speedy.SError.SError
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId

import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace

sealed abstract class SubmitError
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable

object SubmitError {

  final case class RunnerException(err: SError) extends SubmitError

  final case class Internal(reason: String) extends SubmitError {
    override def toString = "CRASH: " + reason
  }

  final case class Timeout(timeout: Duration) extends SubmitError

  final case class CanceledByRequest() extends SubmitError // Do we need this?

  final case class ContractNotEffective(
      coid: ContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends SubmitError

  final case class ContractNotActive(
      coid: ContractId,
      templateId: Identifier,
      consumedBy: Option[EventId],
  ) extends SubmitError

  /** A fetch or exercise was being made against a contract that has not
    * been disclosed to 'committer'.
    */
  final case class ContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      actAs: Set[Party],
      readAs: Set[Party],
      observers: Set[Party],
  ) extends SubmitError

  /** A fetchByKey or lookupByKey was being made against a key
    * for which the contract exists but has not
    * been disclosed to 'committer'.
    */
  final case class ContractKeyNotVisible(
      coid: ContractId,
      key: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      stakeholders: Set[Party],
  ) extends SubmitError

  /** The transaction created a contract that violate unique keys */
  final case class UniqueKeyViolation(gk: GlobalKey) extends SubmitError

  /** Submitted commands for parties that have not been allocated. */
  final case class PartiesNotAllocated(parties: Set[Party]) extends SubmitError
}
