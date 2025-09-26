// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.javaapi.data.Party as ApiParty
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Fingerprint

import scala.language.implicitConversions

sealed trait Party {
  def underlying: ApiParty
  def initialSynchronizers: List[String]
}
case class LocalParty(underlying: ApiParty, initialSynchronizers: List[String]) extends Party
case class ExternalParty(
    underlying: ApiParty,
    initialSynchronizers: List[String],
    signingFingerprints: NonEmpty[Seq[Fingerprint]],
) extends Party

object Party {

  def external(
      value: String,
      signingFingerprints: NonEmpty[Seq[Fingerprint]],
      initialSynchronizers: List[String] = List.empty,
  ): ExternalParty =
    ExternalParty(new ApiParty(value), initialSynchronizers, signingFingerprints)

  def apply(value: String, initialSynchronizers: List[String] = List.empty): Party =
    LocalParty(new ApiParty(value), initialSynchronizers)
  implicit def toApiParty(party: Party): ApiParty = party.underlying
  implicit def toApiString(party: Party): String = party.underlying.getValue
}
