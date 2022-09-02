// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain.ParticipantParty.PartyRecord
import com.daml.ledger.api.domain.ObjectMeta
import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate.{Merge, Replace}
import com.daml.ledger.participant.state.index.v2.{
  AnnotationsUpdate,
  ObjectMetaUpdate,
  PartyRecordUpdate,
}
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PartyRecordUpdateMapperSpec extends AnyFreeSpec with Matchers with EitherValues {

  private val party1 = Ref.Party.assertFromString("party")

  def makePartyRecord(
      party: Ref.Party = party1,
      annotations: Map[String, String] = Map.empty,
  ): PartyRecord = PartyRecord(
    party = party,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
  )

  def makePartyRecordUpdate(
      party: Ref.Party = party1,
      annotationsUpdateO: Option[AnnotationsUpdate] = None,
  ): PartyRecordUpdate = PartyRecordUpdate(
    party = party,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
  )

  val emptyUpdate: PartyRecordUpdate = makePartyRecordUpdate()

  private val testedMapper = PartyRecordUpdateMapper

  "map to party record updates" - {
    "basic mapping" in {
      val pr = makePartyRecord(annotations = Map("a" -> "b"))
      val expected =
        makePartyRecordUpdate(annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b"))))
      testedMapper
        .toUpdate(pr, FieldMask(Seq("party_details.metadata.annotations")))
        .value shouldBe expected
      testedMapper.toUpdate(pr, FieldMask(Seq("party_details.metadata"))).value shouldBe expected
      testedMapper.toUpdate(pr, FieldMask(Seq("party_details"))).value shouldBe expected
    }

    "produce an empty update when new values are all default and merge update semantics is used" in {
      val pr = makePartyRecord(annotations = Map.empty)
      testedMapper.toUpdate(pr, FieldMask(Seq("party_details"))).value.isNoUpdate shouldBe true
    }

    "test use of update modifiers" - {

      "when exact path match on the metadata annotations field" in {
        val prWithAnnotations = makePartyRecord(annotations = Map("a" -> "b"))
        val prWithoutAnnotations = makePartyRecord()
        testedMapper
          .toUpdate(prWithAnnotations, FieldMask(Seq("party_details.metadata.annotations!replace")))
          .value shouldBe makePartyRecordUpdate(annotationsUpdateO = Some(Replace(Map("a" -> "b"))))
        testedMapper
          .toUpdate(
            prWithoutAnnotations,
            FieldMask(Seq("party_details.metadata.annotations!replace")),
          )
          .value shouldBe makePartyRecordUpdate(annotationsUpdateO = Some(Replace(Map.empty)))
        testedMapper
          .toUpdate(prWithAnnotations, FieldMask(Seq("party_details.metadata.annotations!merge")))
          .value shouldBe makePartyRecordUpdate(annotationsUpdateO =
          Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(
            prWithoutAnnotations,
            FieldMask(Seq("party_details.metadata.annotations!merge")),
          )
          .left
          .value shouldBe UpdatePathError.MergeUpdateModifierOnEmptyMapField(
          "party_details.metadata.annotations!merge"
        )
        testedMapper
          .toUpdate(prWithAnnotations, FieldMask(Seq("party_details.metadata.annotations")))
          .value shouldBe makePartyRecordUpdate(annotationsUpdateO =
          Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(prWithoutAnnotations, FieldMask(Seq("party_details.metadata.annotations")))
          .value shouldBe makePartyRecordUpdate(annotationsUpdateO = Some(Replace(Map.empty)))
      }

      "when inexact path match on metadata annotations field" in {
        val userWithAnnotations = makePartyRecord(annotations = Map("a" -> "b"))
        val userWithoutAnnotations = makePartyRecord()
        testedMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("party_details!replace")))
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Replace(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("party_details!replace")))
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Replace(Map.empty))
        )
        testedMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("party_details!merge")))
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("party_details!merge")))
          .value shouldBe emptyUpdate
        testedMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("party_details")))
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("party_details")))
          .value shouldBe emptyUpdate
      }

      "the longest matching path is matched" in {
        val user = makePartyRecord(
          annotations = Map("a" -> "b")
        )
        testedMapper
          .toUpdate(
            user,
            FieldMask(
              Seq(
                "party_details!replace",
                "party_details.metadata!replace",
                "party_details.metadata.annotations!merge",
              )
            ),
          )
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(
            user,
            FieldMask(
              Seq(
                "party_details!replace",
                "party_details.metadata!replace",
                "party_details.metadata.annotations",
              )
            ),
          )
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        testedMapper
          .toUpdate(
            user,
            FieldMask(
              Seq(
                "party_details!merge",
                "party_details.metadata",
                "party_details.metadata.annotations!replace",
              )
            ),
          )
          .value shouldBe makePartyRecordUpdate(
          annotationsUpdateO = Some(Replace(Map("a" -> "b")))
        )
      }

      "when update modifier on a dummy field" in {
        val user = makePartyRecord(annotations = Map("a" -> "b"))
        testedMapper
          .toUpdate(user, FieldMask(Seq("party_details.dummy!replace")))
          .left
          .value shouldBe UpdatePathError.UnknownFieldPath("party_details.dummy!replace")
      }

      "raise an error when an unsupported modifier like syntax is used" in {
        val user = makePartyRecord(annotations = Map("a" -> "b"))
        testedMapper
          .toUpdate(user, FieldMask(Seq("party_details!badmodifier")))
          .left
          .value shouldBe UpdatePathError.UnknownUpdateModifier(
          "party_details!badmodifier"
        )
        testedMapper
          .toUpdate(user, FieldMask(Seq("party_details.metadata.annotations!alsobad")))
          .left
          .value shouldBe UpdatePathError.UnknownUpdateModifier(
          "party_details.metadata.annotations!alsobad"
        )
      }
    }
  }

  "produce an error when " - {
    val user = makePartyRecord(annotations = Map("a" -> "b"))

    "field masks lists unknown field" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq("some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
      testedMapper
        .toUpdate(user, FieldMask(Seq("party_details", "some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
      testedMapper
        .toUpdate(user, FieldMask(Seq("party_details", "party_details.some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("party_details.some_unknown_field")
    }
    "attempting to update resource version" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq("party_details.metadata.resource_version")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("party_details.metadata.resource_version")
    }
    "empty string update path" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq("")))
        .left
        .value shouldBe UpdatePathError.EmptyFieldPath("")
    }
    "empty string field path part of the field mask but non-empty update modifier" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq("!merge")))
        .left
        .value shouldBe UpdatePathError.EmptyFieldPath("!merge")
    }
    "empty field mask" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq.empty))
        .left
        .value shouldBe UpdatePathError.EmptyFieldMask
    }
    "update path with invalid field path syntax" in {
      testedMapper
        .toUpdate(user, FieldMask(Seq("party_details..primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("party_details..primary_party")
      testedMapper
        .toUpdate(user, FieldMask(Seq(".user.primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(".user.primary_party")
      testedMapper
        .toUpdate(user, FieldMask(Seq(".user!merge.primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownUpdateModifier(".user!merge.primary_party")
      testedMapper
        .toUpdate(user, FieldMask(Seq("party_details!merge.primary_party!merge")))
        .left
        .value shouldBe UpdatePathError.InvalidUpdatePathSyntax(
        "party_details!merge.primary_party!merge"
      )
    }
    "multiple update paths with the same field path" in {
      testedMapper
        .toUpdate(
          user,
          FieldMask(Seq("party_details.metadata!merge", "party_details.metadata!replace")),
        )
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("party_details.metadata!replace")
      testedMapper
        .toUpdate(
          user,
          FieldMask(
            Seq("party_details.metadata.annotations!merge", "party_details.metadata.annotations")
          ),
        )
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("party_details.metadata.annotations")
    }
  }
}
