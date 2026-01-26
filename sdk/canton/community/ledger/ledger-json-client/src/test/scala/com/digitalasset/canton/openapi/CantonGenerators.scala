// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.openapi

import com.daml.ledger.api.v2 as lapi
import com.digitalasset.canton.http.json.v2.JsSchema.{JsInterfaceView, JsReassignmentEvent}
import com.google.protobuf
import org.scalacheck.{Arbitrary, Gen}
import scalapb.{GeneratedEnum, GeneratedEnumCompanion}

object CantonGenerators {
  import com.digitalasset.canton.http.json.StdGenerators.*
  def enumArbitrary[T <: GeneratedEnum](companion: GeneratedEnumCompanion[T]): Arbitrary[T] = {
    val gen: Gen[T] = Gen.oneOf(companion.values)
    Arbitrary(gen)
  }

  // We define custom generators for the enums here, so that UNRECOGNIZED values are not generated
  implicit val arbSignatureFormat: Arbitrary[lapi.crypto.SignatureFormat] =
    enumArbitrary(lapi.crypto.SignatureFormat.enumCompanion)
  implicit val arbSigningAlgorithmSpec: Arbitrary[lapi.crypto.SigningAlgorithmSpec] =
    enumArbitrary(
      lapi.crypto.SigningAlgorithmSpec.enumCompanion
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
  implicit val arbUpdateVettedPackagesForceFlag
      : Arbitrary[lapi.admin.package_management_service.UpdateVettedPackagesForceFlag] =
    enumArbitrary(
      lapi.admin.package_management_service.UpdateVettedPackagesForceFlag.enumCompanion
    )

  // limit size of random sequences
  import magnolify.scalacheck.auto.*
  implicit val arbReassignmentEventSeq: Arbitrary[Seq[JsReassignmentEvent.JsReassignmentEvent]] =
    smallSeqArbitrary
  implicit val arbOptIdentifier: Arbitrary[Option[com.daml.ledger.api.v2.value.Identifier]] =
    arbSomeOnly[com.daml.ledger.api.v2.value.Identifier]

  implicit val arbOptTimestamp: Arbitrary[Option[protobuf.timestamp.Timestamp]] =
    arbSomeOnly[protobuf.timestamp.Timestamp]

  implicit val arbOptEventFormat: Arbitrary[Option[lapi.transaction_filter.EventFormat]] =
    arbSomeOnly[lapi.transaction_filter.EventFormat]

  implicit val arbOptSynchronizerTime: Arbitrary[Option[lapi.offset_checkpoint.SynchronizerTime]] =
    arbSomeOnly[lapi.offset_checkpoint.SynchronizerTime]

  implicit val arbOptIPC
      : Arbitrary[Option[lapi.admin.identity_provider_config_service.IdentityProviderConfig]] =
    arbSomeOnly[lapi.admin.identity_provider_config_service.IdentityProviderConfig]

  implicit val arbOptUser: Arbitrary[Option[lapi.admin.user_management_service.User]] =
    arbSomeOnly[lapi.admin.user_management_service.User]

  implicit val arbOptPRPackageReference
      : Arbitrary[Option[lapi.package_reference.PackageReference]] =
    arbSomeOnly[lapi.package_reference.PackageReference]

  implicit val arbOptPackageReference
      : Arbitrary[Option[lapi.interactive.interactive_submission_service.PackagePreference]] =
    arbSomeOnly[lapi.interactive.interactive_submission_service.PackagePreference]

  implicit val arbOptUpdateFormat: Arbitrary[Option[lapi.transaction_filter.UpdateFormat]] =
    arbSomeOnly[lapi.transaction_filter.UpdateFormat]

  implicit val arbOptReassignmentCommands
      : Arbitrary[Option[lapi.reassignment_commands.ReassignmentCommands]] =
    arbSomeOnly[lapi.reassignment_commands.ReassignmentCommands]

  implicit val arbOptUpdateMask: Arbitrary[Option[protobuf.field_mask.FieldMask]] =
    arbSomeOnly[protobuf.field_mask.FieldMask]

  implicit val arbOptPartyDetails
      : Arbitrary[Option[lapi.admin.party_management_service.PartyDetails]] =
    arbSomeOnly[lapi.admin.party_management_service.PartyDetails]

  implicit val arbOptSigningPublicKey: Arbitrary[Option[lapi.crypto.SigningPublicKey]] =
    arbSomeOnly[lapi.crypto.SigningPublicKey]

  // we do not provide unknownFields, as they are not used in the JSON API
  implicit val arbJsStatus: Arbitrary[com.google.rpc.status.Status] =
    Arbitrary {
      for {
        code <- Gen.chooseNum(0, Int.MaxValue)
        message <- Gen.alphaStr
        details <- arbProtoAnySeq.arbitrary
      } yield com.google.rpc.status.Status(code, message, details)
    }
  implicit val arbInterfaceViewSeq: Arbitrary[Seq[JsInterfaceView]] =
    smallSeqArbitrary

  // due to discrepancy between Circe relaxed codec  an deriver tapir Schema we enforce some values as always defined
  def arbSomeOnly[T](implicit arb: Arbitrary[T]): Arbitrary[Option[T]] =
    Arbitrary(arb.arbitrary.map(Some(_)))

}
