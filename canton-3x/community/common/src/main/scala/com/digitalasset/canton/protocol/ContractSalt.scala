// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.topology.{DomainId, MediatorRef}

import java.util.UUID

/** A blinded cryptographic hash of the information that ensures uniqueness of [[Unicum]]s in Canton.
  * The hash can be used to unblind the [[Unicum]]'s cryptographic commitment to the contract data.
  *
  * @see UnicumGenerator for the construction details
  */
final case class ContractSalt(unwrap: Salt) extends AnyVal

object ContractSalt {

  /** Creates a [[ContractSalt]] based on the information that ensures uniqueness of Canton contract IDs.
    *
    * @param hmacOps The hmac operations to derive the blinded hash.
    * @param transactionUuid The UUID of the transaction that creates the contract.
    * @param domainId The domain on which the contract is created.
    * @param mediatorId The mediator that handles the transaction that creates the contract
    * @param actionSalt The action salt of the view whose core contains the contract creation. This is used to blind the hash.
    *                   It therefore must contain good randomness.
    * @param createIndex The index of the create node in the view.
    * @param viewPosition The position of the view whose core creates the contract
    */
  def create(hmacOps: HmacOps)(
      transactionUuid: UUID,
      domainId: DomainId,
      mediator: MediatorRef,
      actionSalt: Salt,
      createIndex: Int,
      viewPosition: ViewPosition,
      contractIdVersion: CantonContractIdVersion,
  ): ContractSalt = {
    val bytestring = DeterministicEncoding
      .encodeInt(createIndex)
      .concat(viewPosition.encodeDeterministically)
      .concat(DeterministicEncoding.encodeString(transactionUuid.toString))
      .concat(DeterministicEncoding.encodeString(domainId.toProtoPrimitive))
      .concat(DeterministicEncoding.encodeString(mediator.toProtoPrimitive))

    val salt = Salt.tryDeriveSalt(actionSalt, bytestring, contractIdVersion, hmacOps)

    ContractSalt(salt)
  }
}
