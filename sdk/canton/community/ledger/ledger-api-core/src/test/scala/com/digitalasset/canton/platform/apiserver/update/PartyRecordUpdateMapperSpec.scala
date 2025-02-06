// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta, PartyDetails}
import com.digitalasset.canton.ledger.localstore.api.{ObjectMetaUpdate, PartyDetailsUpdate}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PartyRecordUpdateMapperSpec extends AnyFreeSpec with Matchers with EitherValues {

  private val party1 = Ref.Party.assertFromString("party")

  def makePartyDetails(
      party: Ref.Party = party1,
      isLocal: Boolean = false,
      annotations: Map[String, String] = Map.empty,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): PartyDetails = PartyDetails(
    party = party,
    isLocal = isLocal,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
    identityProviderId = identityProviderId,
  )

  def makePartyDetailsUpdate(
      party: Ref.Party = party1,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
      isLocalUpdate: Option[Boolean] = None,
      annotationsUpdateO: Option[Map[String, String]] = None,
  ): PartyDetailsUpdate = PartyDetailsUpdate(
    party = party,
    identityProviderId = identityProviderId,
    isLocalUpdate = isLocalUpdate,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
  )

  val emptyUpdate: PartyDetailsUpdate = makePartyDetailsUpdate()

  private val testedMapper = PartyRecordUpdateMapper

  "map to party details updates" - {
    "for annotations" in {
      val newResourceSet = makePartyDetails(annotations = Map("a" -> "b"))
      val newResourceUnset = makePartyDetails(annotations = Map.empty)
      testedMapper
        .toUpdate(newResourceSet, FieldMask(Seq("local_metadata.annotations")))
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = Some(Map("a" -> "b")))
      testedMapper
        .toUpdate(newResourceSet, FieldMask(Seq("local_metadata")))
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = Some(Map("a" -> "b")))
      testedMapper
        .toUpdate(newResourceUnset, FieldMask(Seq("local_metadata.annotations")))
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = Some(Map.empty))
      testedMapper
        .toUpdate(newResourceUnset, FieldMask(Seq("local_metadata")))
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = None)
    }
    "for is_local" in {
      val newResourceSet = makePartyDetails(isLocal = true)
      val newResourceUnset = makePartyDetails(isLocal = false)
      testedMapper
        .toUpdate(newResourceSet, FieldMask(Seq("is_local")))
        .value shouldBe makePartyDetailsUpdate(isLocalUpdate = Some(true))
      testedMapper
        .toUpdate(newResourceUnset, FieldMask(Seq("is_local")))
        .value shouldBe makePartyDetailsUpdate(isLocalUpdate = Some(false))
    }
    "when exact path match on the metadata annotations field" in {
      val prWithAnnotations = makePartyDetails(annotations = Map("a" -> "b"))
      val prWithoutAnnotations = makePartyDetails()
      testedMapper
        .toUpdate(prWithAnnotations, FieldMask(Seq("local_metadata.annotations")))
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = Some(Map("a" -> "b")))
      testedMapper
        .toUpdate(
          prWithoutAnnotations,
          FieldMask(Seq("local_metadata.annotations")),
        )
        .value shouldBe makePartyDetailsUpdate(annotationsUpdateO = Some(Map.empty))
    }
    "when inexact path match on metadata annotations field" in {
      val prWithAnnotations = makePartyDetails(annotations = Map("a" -> "b"))
      testedMapper
        .toUpdate(prWithAnnotations, FieldMask(Seq("local_metadata")))
        .value shouldBe makePartyDetailsUpdate(
        annotationsUpdateO = Some(Map("a" -> "b"))
      )
    }

    "the longest matching path is matched" in {
      val pr = makePartyDetails(
        annotations = Map("a" -> "b")
      )
      testedMapper
        .toUpdate(
          pr,
          FieldMask(
            Seq(
              "local_metadata",
              "local_metadata.annotations",
            )
          ),
        )
        .value shouldBe makePartyDetailsUpdate(
        annotationsUpdateO = Some(Map("a" -> "b"))
      )
      testedMapper
        .toUpdate(
          pr,
          FieldMask(
            Seq(
              "local_metadata",
              "local_metadata.annotations",
            )
          ),
        )
        .value shouldBe makePartyDetailsUpdate(
        annotationsUpdateO = Some(Map("a" -> "b"))
      )
      testedMapper
        .toUpdate(
          pr,
          FieldMask(
            Seq(
              "local_metadata",
              "local_metadata.annotations",
            )
          ),
        )
        .value shouldBe makePartyDetailsUpdate(
        annotationsUpdateO = Some(Map("a" -> "b"))
      )
    }

  }

  "produce an error when " - {
    val pd = makePartyDetails(annotations = Map("a" -> "b"))

    "field masks lists unknown field" in {
      testedMapper
        .toUpdate(pd, FieldMask(Seq("some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
      testedMapper
        .toUpdate(pd, FieldMask(Seq("local_metadata", "some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
    }
    "specifying resource version in the update mask" in {
      testedMapper
        .toUpdate(pd, FieldMask(Seq("local_metadata.resource_version")))
        .value shouldBe emptyUpdate
    }
    "specifying party in the update mask" in {
      testedMapper
        .toUpdate(pd, FieldMask(Seq("party")))
        .value shouldBe emptyUpdate
    }
    "empty field mask" in {
      testedMapper
        .toUpdate(pd, FieldMask(Seq.empty))
        .left
        .value shouldBe UpdatePathError.EmptyUpdateMask
    }
    "update path with invalid field path syntax" in {
      testedMapper
        .toUpdate(pd, FieldMask(Seq("..local_metadata")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(
        "..local_metadata"
      )
      testedMapper
        .toUpdate(pd, FieldMask(Seq(".local_metadata")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(
        ".local_metadata"
      )
    }
    "multiple update paths with the same field path" in {
      testedMapper
        .toUpdate(
          pd,
          FieldMask(
            Seq(
              "local_metadata.annotations",
              "local_metadata.annotations",
            )
          ),
        )
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath(
        "local_metadata.annotations"
      )
    }
  }
}
