// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import java.util.UUID

/** A blinded cryptographic hash of the information that ensures uniqueness of contract ID suffixes
  * in Canton. The hash can be used to authenticate the contract contents based on the contract
  * suffix.
  */
final case class ContractSalt(unwrap: Salt) extends AnyVal

object ContractSalt {

  /** Creates a [[ContractSalt]] based on the information that ensures global uniqueness of Canton
    * contract IDs of version [[com.digitalasset.daml.lf.value.Value.ContractId.V1]].
    *
    * @param hmacOps
    *   The hmac operations to derive the blinded hash.
    * @param transactionUuid
    *   The UUID of the transaction that creates the contract.
    * @param psid
    *   The synchronizer on which the contract is created.
    * @param mediator
    *   The mediator that handles the transaction that creates the contract
    * @param viewParticipantDataSalt
    *   The [[com.digitalasset.canton.data.ViewParticipantData]]'s salt of the view whose core
    *   contains the contract creation. This is used to blind the hash. It therefore must contain
    *   good randomness.
    * @param createIndex
    *   The index of the create node in the view (starting at 0). Only create nodes and only nodes
    *   that belong to the core of the view with salt `viewParticipantDataSalt` have an index.
    * @param viewPosition
    *   The position of the view whose core creates the contract
    *
    * @see
    *   UnicumGenerator for the construction details
    */
  def createV1(hmacOps: HmacOps)(
      transactionUuid: UUID,
      psid: PhysicalSynchronizerId,
      mediator: MediatorGroupRecipient,
      viewParticipantDataSalt: Salt,
      createIndex: Int,
      viewPosition: ViewPosition,
  ): ContractSalt = {
    val bytestring = DeterministicEncoding
      .encodeInt(createIndex)
      .concat(viewPosition.encodeDeterministically)
      .concat(DeterministicEncoding.encodeString(transactionUuid.toString))
      .concat(DeterministicEncoding.encodeString(psid.toProtoPrimitive))
      .concat(DeterministicEncoding.encodeString(mediator.toProtoPrimitive))

    val salt = Salt.tryDeriveSalt(viewParticipantDataSalt, bytestring, hmacOps)

    ContractSalt(salt)
  }

  /** Creates a [[ContractSalt]] based on the information that ensures uniqueness of Canton contract
    * IDs of version [[com.digitalasset.daml.lf.value.Value.ContractId.V2]]. Uniqueness is
    * guaranteed only within the scope of the creating transaction; global uniqueness is obtained by
    * absolutization.
    *
    * @param hmacOps
    *   The hmac operations to derive the blinded hash.
    * @param viewParticipantDataSalt
    *   The [[com.digitalasset.canton.data.ViewParticipantData]]'s salt of the view whose core
    *   contains the contract creation. This is used to blind the hash. It therefore must contain
    *   good randomness.
    * @param createIndex
    *   The index of the create node in the view (starting at 0). Only create nodes and only nodes
    *   that belong to the core of the view with salt `viewParticipantDataSalt` have an index.
    * @param viewPosition
    *   The position of the view whose core creates the contract
    */
  def createV2(hmacOps: HmacOps)(
      viewParticipantDataSalt: Salt,
      createIndex: Int,
      viewPosition: ViewPosition,
  ): ContractSalt = {
    val bytestring = DeterministicEncoding
      .encodeInt(createIndex)
      .concat(viewPosition.encodeDeterministically)
    val salt = Salt.tryDeriveSalt(viewParticipantDataSalt, bytestring, hmacOps)
    ContractSalt(salt)
  }
}
