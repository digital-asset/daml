// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.openapi

import com.daml.ledger.api.v2 as lapi
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsInterfaceView,
  JsReassignmentEvent,
  JsTopologyEvent,
}
import org.scalacheck.{Arbitrary, Gen}
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}

object CantonGenerators {
  import StdGenerators.*
  def enumArbitrary[T <: GeneratedEnum](companion: GeneratedEnumCompanion[T]): Arbitrary[T] = {
    val gen: Gen[T] = Gen.oneOf(companion.values)
    Arbitrary(gen)
  }

  // We define custom generators for the enums here, so that UNRECOGNIZED values are not generated
  implicit val arbSignatureFormat
      : Arbitrary[lapi.interactive.interactive_submission_service.SignatureFormat] =
    enumArbitrary(lapi.interactive.interactive_submission_service.SignatureFormat.enumCompanion)
  implicit val arbSigningAlgorithmSpec
      : Arbitrary[lapi.interactive.interactive_submission_service.SigningAlgorithmSpec] =
    enumArbitrary(
      lapi.interactive.interactive_submission_service.SigningAlgorithmSpec.enumCompanion
    )
  implicit val arbHashingSchemeVersion
      : Arbitrary[lapi.interactive.interactive_submission_service.HashingSchemeVersion] =
    enumArbitrary(
      lapi.interactive.interactive_submission_service.HashingSchemeVersion.enumCompanion
    )
  implicit val arbTransactionShape: Arbitrary[lapi.transaction_filter.TransactionShape] =
    enumArbitrary(
      lapi.transaction_filter.TransactionShape.enumCompanion
    )
  implicit val arbParticipantPermission: Arbitrary[lapi.state_service.ParticipantPermission] =
    enumArbitrary(
      lapi.state_service.ParticipantPermission.enumCompanion
    )
  implicit val arbPackageStatus: Arbitrary[lapi.package_service.PackageStatus] =
    enumArbitrary(
      lapi.package_service.PackageStatus.enumCompanion
    )
//TransactionShape

  // limit size of random sequences
  import magnolify.scalacheck.auto.*
  implicit val arbReassignmentEventSeq: Arbitrary[Seq[JsReassignmentEvent.JsReassignmentEvent]] =
    smallSeqArbitrary
  implicit val arbTopologyEventSeq: Arbitrary[Seq[JsTopologyEvent.TopologyEvent]] =
    smallSeqArbitrary
  implicit val arbInterfaceViewSeq: Arbitrary[Seq[JsInterfaceView]] =
    smallSeqArbitrary

  // due to discrepancy between Circe relaxed codec  an deriver tapir Schema we enforce some values as always defined
  def arbSomeOnly[T](implicit arb: Arbitrary[T]): Arbitrary[Option[T]] =
    Arbitrary(arb.arbitrary.map(Some(_)))

  implicit val arbOptIdentifier: Arbitrary[Option[com.daml.ledger.api.v2.value.Identifier]] =
    arbSomeOnly[com.daml.ledger.api.v2.value.Identifier]
}
