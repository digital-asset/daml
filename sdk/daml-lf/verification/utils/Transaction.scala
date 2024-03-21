// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang._
import stainless.annotation._
import scala.annotation.nowarn
import stainless.proof._
import stainless.collection._

import Value.ContractId

object Transaction {

  /** The state of a key at the beginning of the transaction.
    */
  sealed trait KeyInput extends Product with Serializable {
    def toKeyMapping: Option[ContractId]

    def isActive: Boolean
  }

  /** No active contract with the given key.
    */
  sealed trait KeyInactive extends KeyInput {
    override def toKeyMapping: Option[ContractId] = None[ContractId]()

    override def isActive: Boolean = false
  }

  /** A contract with the key will be created so the key must be inactive.
    */
  @nowarn
  final case object KeyCreate extends KeyInactive

  /** Negative key lookup so the key mus tbe inactive.
    */
  @nowarn
  final case object NegativeKeyLookup extends KeyInactive

  /** Key must be mapped to this active contract.
    */
  final case class KeyActive(cid: ContractId) extends KeyInput {
    override def toKeyMapping: Option[ContractId] = Some(cid)

    override def isActive: Boolean = true
  }

  sealed abstract class TransactionError

  final case class DuplicateContractKey(key: GlobalKey) extends TransactionError
  final case class InconsistentContractKey(key: GlobalKey)

  type KeyInputError = Either[InconsistentContractKey, DuplicateContractKey]
}
