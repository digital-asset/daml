// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** Trait for identities on ledgers that Canton integrates with.
  * For example, a ledger identity can be an Ethereum account or a Fabric identity.
  */
sealed trait LedgerIdentity {
  def identifier: String

  def toProtoV0: v0.LedgerIdentity
}

object LedgerIdentity {
  def fromProtoV0(
      identityP: v0.LedgerIdentity
  ): ParsingResult[LedgerIdentity] = {
    identityP.identifier match {
      case v0.LedgerIdentity.Identifier.Empty =>
        Left(FieldNotSet("LedgerIdentity.identifier"))
      case v0.LedgerIdentity.Identifier.EthereumAccount(account) =>
        EthereumAccount.fromProtoV0(account)
    }
  }
}

/** The address of an Ethereum account is derived by taking the last 20 bytes
  * of the Keccak-256 hash of the public key and adding 0x to the beginning. See, e.g.,
  * [here](https://ethereum.org/en/developers/docs/accounts/#account-creation) as a reference.
  */
final case class EthereumAccount(address: String) extends LedgerIdentity {
  override def identifier: String = address

  override def toProtoV0: v0.LedgerIdentity =
    v0.LedgerIdentity(identifier =
      v0.LedgerIdentity.Identifier.EthereumAccount(v0.EthereumAccount(address))
    )
}

object EthereumAccount {
  def create(address: String): Either[String, EthereumAccount] = {
    Right(new EthereumAccount(address))
  }

  // public address representation is used when communicating with Ethereum nodes
  def fromPublicAddress(address: String): Either[String, EthereumAccount] = create(
    address
  )

  def tryCreate(address: String): EthereumAccount = {
    create(address).valueOr(err => throw new IllegalArgumentException(err))
  }

  // protobuf representation is used when communicating over the Admin API
  def fromProtoV0(accountP: v0.EthereumAccount): ParsingResult[EthereumAccount] =
    EthereumAccount
      .create(accountP.address)
      .leftMap(
        ProtoDeserializationError.ValueDeserializationError("EthereumAccount.ethereum_address", _)
      )
}
