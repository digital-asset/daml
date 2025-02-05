// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package script

import com.digitalasset.daml.lf.data.Ref.{Identifier, Party, PackageId}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.Ast.PackageMetadata
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.transaction.{GlobalKey, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace

/** Errors from script interpretation. */
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

  final case class Timeout(timeout: Duration) extends Error

  final case class CanceledByRequest() extends Error

  final case class ContractNotEffective(
      coid: ContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends Error

  final case class ContractNotActive(
      coid: ContractId,
      templateId: Identifier,
      consumedBy: Option[EventId],
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
  final case class CommitError(commitError: IdeLedger.CommitError) extends Error

  /** The transaction produced by the update expression in a 'mustFailAt' succeeded. */
  final case class MustFailSucceeded(tx: VersionedTransaction) extends Error

  /** Invalid party name supplied to 'getParty'. */
  final case class InvalidPartyName(name: String, msg: String) extends Error

  /** Tried to allocate a party that already exists. */
  final case class PartyAlreadyExists(name: String) extends Error

  /** Submitted commands for parties that have not been allocated. */
  final case class PartiesNotAllocated(parties: Set[Party]) extends Error

  /** Lookup error from the engine */
  final case class LookupError(
      err: language.LookupError,
      packageMeta: Option[PackageMetadata],
      packageId: PackageId,
  ) extends Error

  final case class DisclosureDecoding(message: String) extends Error
}
