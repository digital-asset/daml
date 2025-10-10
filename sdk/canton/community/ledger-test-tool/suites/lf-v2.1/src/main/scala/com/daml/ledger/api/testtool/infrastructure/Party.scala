// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.crypto as lapicrypto
import com.daml.ledger.api.v2.crypto.SignatureFormat.SIGNATURE_FORMAT_RAW
import com.daml.ledger.javaapi.data.Party as ApiParty
import com.digitalasset.canton.crypto.Fingerprint
import com.google.protobuf.ByteString

import java.security.{KeyPair, Signature}
import scala.language.implicitConversions

sealed trait Party {
  def underlying: ApiParty
  def initialSynchronizers: List[String]
}
case class LocalParty(underlying: ApiParty, initialSynchronizers: List[String]) extends Party
case class ExternalParty(
    underlying: ApiParty,
    initialSynchronizers: List[String],
    signingFingerprint: Fingerprint,
    signingKeyPair: KeyPair,
) extends Party {
  def sign(data: ByteString): ByteString = {
    val signatureInstance = Signature.getInstance("Ed25519")
    signatureInstance.initSign(signingKeyPair.getPrivate)
    signatureInstance.update(data.toByteArray)
    ByteString.copyFrom(signatureInstance.sign())
  }

  def signProto(data: ByteString): lapicrypto.Signature =
    lapicrypto.Signature(
      format = SIGNATURE_FORMAT_RAW,
      signature = sign(data),
      signedBy = signingFingerprint.toProtoPrimitive,
      signingAlgorithmSpec = lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
    )
}

object Party {

  def external(
      value: String,
      signingFingerprint: Fingerprint,
      signingKeyPair: KeyPair,
      initialSynchronizers: List[String] = List.empty,
  ): ExternalParty =
    ExternalParty(new ApiParty(value), initialSynchronizers, signingFingerprint, signingKeyPair)

  def apply(value: String, initialSynchronizers: List[String] = List.empty): Party =
    LocalParty(new ApiParty(value), initialSynchronizers)
  implicit def toApiParty(party: Party): ApiParty = party.underlying
  implicit def toApiString(party: Party): String = party.underlying.getValue
}
