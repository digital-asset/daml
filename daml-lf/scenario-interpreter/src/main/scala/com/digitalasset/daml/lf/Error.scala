// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.data.Time
import com.daml.lf.ledger.EventId
import com.daml.lf.speedy.SError.SError
import com.daml.lf.transaction.{GlobalKey, Transaction}
import com.daml.lf.value.Value.ContractId

import scala.util.control.NoStackTrace

/** Errors from scenario interpretation. */
sealed abstract class Error
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable

object Error {

  final case class RunnerException(err: SError) extends Error

  final case class Internal(reason: String) extends Error {
    override def toString = "CRASH: " + reason
  }

  final case class ContractNotEffective(
      coid: ContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends Error

  final case class ContractNotActive(
      coid: ContractId,
      templateId: Identifier,
      consumedBy: EventId,
  ) extends Error

  /** A fetch or exercise was being made against a contract that has not
    * been disclosed to 'committer'.
    */
  final case class ContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      actAs: Set[Party],
      readAs: Set[Party],
      observers: Set[Party],
  ) extends Error

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
  ) extends Error

  /** The transaction failed due to a commit error */
  final case class CommitError(commitError: ScenarioLedger.CommitError) extends Error

  /** The transaction produced by the update expression in a 'mustFailAt' succeeded. */
  final case class MustFailSucceeded(tx: Transaction.Transaction) extends Error

  /** Invalid party name supplied to 'getParty'. */
  final case class InvalidPartyName(name: String, msg: String) extends Error

  /** Tried to allocate a party that already exists. */
  final case class PartyAlreadyExists(name: String) extends Error

}
