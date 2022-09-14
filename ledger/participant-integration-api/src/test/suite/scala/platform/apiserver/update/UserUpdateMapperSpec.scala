// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

import com.daml.ledger.api.domain.{ObjectMeta, User}
import com.daml.ledger.participant.state.index.v2.{AnnotationsUpdate, ObjectMetaUpdate, UserUpdate}
import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate.{Merge, Replace}
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.EitherValues
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class UserUpdateMapperSpec extends AnyFreeSpec with Matchers with EitherValues {

  private val userId1: Ref.UserId = Ref.UserId.assertFromString("u1")
  private val party1 = Ref.Party.assertFromString("party")

  def makeUser(
      id: Ref.UserId = userId1,
      primaryParty: Option[Ref.Party] = None,
      isDeactivated: Boolean = false,
      annotations: Map[String, String] = Map.empty,
  ): User = User(
    id = id,
    primaryParty = primaryParty,
    isDeactivated = isDeactivated,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
  )

  def makeUserUpdate(
      id: Ref.UserId = userId1,
      primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
      isDeactivatedUpdateO: Option[Boolean] = None,
      annotationsUpdateO: Option[AnnotationsUpdate] = None,
  ): UserUpdate = UserUpdate(
    id = id,
    primaryPartyUpdateO = primaryPartyUpdateO,
    isDeactivatedUpdateO = isDeactivatedUpdateO,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
  )

  val emptyUserUpdate: UserUpdate = makeUserUpdate()

  "map to user updates" - {
    "basic mapping" - {
      val user = makeUser(
        primaryParty = None,
        isDeactivated = false,
        annotations = Map("a" -> "b"),
      )
      val expected = makeUserUpdate(
        primaryPartyUpdateO = Some(None),
        isDeactivatedUpdateO = Some(false),
        annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b"))),
      )
      "1) with all individual fields to update listed in the update mask" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user.is_deactivated", "user.primary_party", "user.metadata.annotations")),
          )
          .value shouldBe expected
      }
      "2) with metadata.annotations not listed explicitly" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user.is_deactivated", "user.primary_party", "user.metadata")),
          )
          .value shouldBe expected
      }
    }

    "map api request to update - merge user and reset is_deactivated" - {
      val user = makeUser(
        // non-default value
        primaryParty = Some(party1),
        // default value
        isDeactivated = false,
        // non-default value
        annotations = Map("a" -> "b"),
      )
      val expected = makeUserUpdate(
        primaryPartyUpdateO = Some(Some(party1)),
        isDeactivatedUpdateO = Some(false),
        annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b"))),
      )
      "1) minimal field mask" in {
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("user", "user.is_deactivated")))
          .value shouldBe expected
      }
      "2) not so minimal field mask" in {
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("user", "user.metadata", "user.is_deactivated")))
          .value shouldBe expected
      }
      "3) also not so minimal field mask" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user.primary_party", "user.metadata", "user.is_deactivated")),
          )
          .value shouldBe expected
      }
      "4) field mask with exact field paths" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user.primary_party", "user.metadata.annotations", "user.is_deactivated")),
          )
          .value shouldBe expected
      }
    }

    "produce an empty update when new values are all default and merge update semantics is used" in {
      val user = makeUser(
        primaryParty = None,
        isDeactivated = false,
        annotations = Map.empty,
      )
      UserUpdateMapper
        .toUpdate(
          user,
          FieldMask(
            Seq(
              "user"
            )
          ),
        )
        .value
        .isNoUpdate shouldBe true
    }

    "test use of update modifiers" - {
      "when exact path match on a primitive field" in {
        val userWithParty = makeUser(primaryParty = Some(party1))
        val userWithoutParty = makeUser()
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user.primary_party!replace")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user.primary_party!replace")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(None))
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user.primary_party!merge")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user.primary_party!merge")))
          .left
          .value shouldBe UpdatePathError.MergeUpdateModifierOnPrimitiveFieldWithDefaultValue(
          "user.primary_party!merge"
        )
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user.primary_party")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user.primary_party")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(None))
      }

      "when exact path match on the metadata annotations field" in {
        val userWithAnnotations = makeUser(annotations = Map("a" -> "b"))
        val userWithoutAnnotations = makeUser()
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user.metadata.annotations!replace")))
          .value shouldBe makeUserUpdate(annotationsUpdateO = Some(Replace(Map("a" -> "b"))))
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user.metadata.annotations!replace")))
          .value shouldBe makeUserUpdate(annotationsUpdateO = Some(Replace(Map.empty)))
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user.metadata.annotations!merge")))
          .value shouldBe makeUserUpdate(annotationsUpdateO =
          Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user.metadata.annotations!merge")))
          .left
          .value shouldBe UpdatePathError.MergeUpdateModifierOnEmptyMapField(
          "user.metadata.annotations!merge"
        )
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user.metadata.annotations")))
          .value shouldBe makeUserUpdate(annotationsUpdateO =
          Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user.metadata.annotations")))
          .value shouldBe makeUserUpdate(annotationsUpdateO = Some(Replace(Map.empty)))
      }

      "when inexact path match for a primitive field" in {
        val userWithParty = makeUser(primaryParty = Some(party1))
        val userWithoutParty = makeUser()
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user!replace")))
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(Some(party1)),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Replace(Map.empty)),
        )
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user!replace")))
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(None),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Replace(Map.empty)),
        )
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user!merge")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user!merge")))
          .value shouldBe emptyUserUpdate
        UserUpdateMapper
          .toUpdate(userWithParty, FieldMask(Seq("user")))
          .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
        UserUpdateMapper
          .toUpdate(userWithoutParty, FieldMask(Seq("user")))
          .value shouldBe emptyUserUpdate
      }

      "when inexact path match on metadata annotations field" in {
        val userWithAnnotations = makeUser(annotations = Map("a" -> "b"))
        val userWithoutAnnotations = makeUser()
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user!replace")))
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(None),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Replace(Map("a" -> "b"))),
        )
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user!replace")))
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(None),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Replace(Map.empty)),
        )
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user!merge")))
          .value shouldBe makeUserUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user!merge")))
          .value shouldBe emptyUserUpdate
        UserUpdateMapper
          .toUpdate(userWithAnnotations, FieldMask(Seq("user")))
          .value shouldBe makeUserUpdate(
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b")))
        )
        UserUpdateMapper
          .toUpdate(userWithoutAnnotations, FieldMask(Seq("user")))
          .value shouldBe emptyUserUpdate
      }

      "the longest matching path is matched" in {
        val user = makeUser(
          annotations = Map("a" -> "b"),
          primaryParty = Some(party1),
        )
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(
              Seq("user!replace", "user.metadata!replace", "user.metadata.annotations!merge")
            ),
          )
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(Some(party1)),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b"))),
        )
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user!replace", "user.metadata!replace", "user.metadata.annotations")),
          )
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(Some(party1)),
          isDeactivatedUpdateO = Some(false),
          annotationsUpdateO = Some(Merge.fromNonEmpty(Map("a" -> "b"))),
        )
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("user!merge", "user.metadata", "user.metadata.annotations!replace")),
          )
          .value shouldBe makeUserUpdate(
          primaryPartyUpdateO = Some(Some(party1)),
          annotationsUpdateO = Some(Replace(Map("a" -> "b"))),
        )
      }

      "when update modifier on a dummy field" in {
        val user = makeUser(primaryParty = Some(party1))
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("user.dummy!replace")))
          .left
          .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath("user.dummy!replace")
      }

      "raise an error when an unsupported modifier like syntax is used" in {
        val user = makeUser(primaryParty = Some(party1))
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("user!badmodifier")))
          .left
          .value shouldBe UpdatePathError.UnknownUpdateModifier(
          "user!badmodifier"
        )
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("user.metadata.annotations!alsobad")))
          .left
          .value shouldBe UpdatePathError.UnknownUpdateModifier(
          "user.metadata.annotations!alsobad"
        )
      }
    }
  }

  "produce an error when " - {
    val user = makeUser(primaryParty = Some(party1))

    "field masks lists unknown field" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath("some_unknown_field")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user", "some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath("some_unknown_field")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user", "user.some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath("user.some_unknown_field")
    }
    "attempting to update resource version" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user.metadata.resource_version")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath(
        "user.metadata.resource_version"
      )
    }
    "empty string update path" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("")))
        .left
        .value shouldBe UpdatePathError.InvalidUpdatePathSyntax("")
    }
    "empty string field path part of the field mask but non-empty update modifier" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("!merge")))
        .left
        .value shouldBe UpdatePathError.InvalidUpdatePathSyntax("!merge")
    }
    "empty field mask" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq.empty))
        .left
        .value shouldBe UpdatePathError.EmptyUpdateMask
    }
    "update path with invalid field path syntax" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user..primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath("user..primary_party")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq(".user.primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownOrUnmodifiableFieldPath(".user.primary_party")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq(".user!merge.primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownUpdateModifier(".user!merge.primary_party")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user!merge.primary_party!merge")))
        .left
        .value shouldBe UpdatePathError.InvalidUpdatePathSyntax(
        "user!merge.primary_party!merge"
      )
    }
    "multiple update paths with the same field path" in {
      UserUpdateMapper
        .toUpdate(
          user,
          FieldMask(Seq("user.primary_party!merge", "user.primary_party!replace")),
        )
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("user.primary_party!replace")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("user.primary_party!merge", "user.primary_party")))
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("user.primary_party")
    }
  }
}
