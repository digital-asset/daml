// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.update

import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderId, ObjectMeta, User}
import com.digitalasset.canton.ledger.localstore.api.{ObjectMetaUpdate, UserUpdate}
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
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
  ): User = User(
    id = id,
    primaryParty = primaryParty,
    isDeactivated = isDeactivated,
    metadata = ObjectMeta(
      resourceVersionO = None,
      annotations = annotations,
    ),
    identityProviderId = identityProviderId,
  )

  def makeUserUpdate(
      id: Ref.UserId = userId1,
      identityProviderId: IdentityProviderId = IdentityProviderId.Default,
      primaryPartyUpdateO: Option[Option[Ref.Party]] = None,
      isDeactivatedUpdateO: Option[Boolean] = None,
      annotationsUpdateO: Option[Map[String, String]] = None,
  ): UserUpdate = UserUpdate(
    id = id,
    primaryPartyUpdateO = primaryPartyUpdateO,
    isDeactivatedUpdateO = isDeactivatedUpdateO,
    metadataUpdate = ObjectMetaUpdate(
      resourceVersionO = None,
      annotationsUpdateO = annotationsUpdateO,
    ),
    identityProviderId = identityProviderId,
  )

  val emptyUserUpdate: UserUpdate = makeUserUpdate()

  "map to user updates" - {
    "basic mapping" - {
      val user = makeUser(
        primaryParty = None,
        isDeactivated = false,
        annotations = Map("a" -> "b"),
        identityProviderId = IdentityProviderId("abc123"),
      )
      val expected = makeUserUpdate(
        primaryPartyUpdateO = Some(None),
        isDeactivatedUpdateO = Some(false),
        annotationsUpdateO = Some(Map("a" -> "b")),
        identityProviderId = IdentityProviderId("abc123"),
      )
      "1) with all individual fields to update listed in the update mask" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(
              Seq("is_deactivated", "primary_party", "metadata.annotations")
            ),
          )
          .value shouldBe expected
      }
      "2) with metadata.annotations not listed explicitly" in {
        UserUpdateMapper
          .toUpdate(
            user,
            FieldMask(Seq("is_deactivated", "primary_party", "metadata")),
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
        annotationsUpdateO = Some(Map("a" -> "b")),
      )
      "field mask with non-exact field paths" in {
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("primary_party", "is_deactivated", "metadata")))
          .value shouldBe expected
      }
      "field mask with exact field paths" in {
        UserUpdateMapper
          .toUpdate(user, FieldMask(Seq("primary_party", "is_deactivated", "metadata.annotations")))
          .value shouldBe expected
      }
    }

    "when exact path match on a primitive field" in {
      val userWithParty = makeUser(primaryParty = Some(party1))
      val userWithoutParty = makeUser()
      UserUpdateMapper
        .toUpdate(userWithParty, FieldMask(Seq("primary_party")))
        .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(Some(party1)))
      UserUpdateMapper
        .toUpdate(userWithoutParty, FieldMask(Seq("primary_party")))
        .value shouldBe makeUserUpdate(primaryPartyUpdateO = Some(None))
    }

    "when exact path match on the metadata annotations field" in {
      val userWithAnnotations = makeUser(annotations = Map("a" -> "b"))
      val userWithoutAnnotations = makeUser()
      UserUpdateMapper
        .toUpdate(userWithAnnotations, FieldMask(Seq("metadata.annotations")))
        .value shouldBe makeUserUpdate(annotationsUpdateO = Some(Map("a" -> "b")))
      UserUpdateMapper
        .toUpdate(userWithoutAnnotations, FieldMask(Seq("metadata.annotations")))
        .value shouldBe makeUserUpdate(annotationsUpdateO = Some(Map.empty))
    }

    "when inexact path match on metadata annotations field" in {
      val userWithAnnotations = makeUser(annotations = Map("a" -> "b"))
      val userWithoutAnnotations = makeUser()
      UserUpdateMapper
        .toUpdate(userWithAnnotations, FieldMask(Seq("metadata")))
        .value shouldBe makeUserUpdate(
        annotationsUpdateO = Some(Map("a" -> "b"))
      )
      UserUpdateMapper
        .toUpdate(userWithoutAnnotations, FieldMask(Seq("metadata")))
        .value shouldBe emptyUserUpdate
    }

  }

  "produce an error when " - {
    val user = makeUser(primaryParty = Some(party1))

    "field masks lists unknown field" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("metadata", "some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("primary_party", "some_unknown_field")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath("some_unknown_field")
    }
    "specifying resource version in the update mask" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("metadata.resource_version")))
        .value shouldBe emptyUserUpdate
    }
    "specifying id in the update mask" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq("id")))
        .value shouldBe emptyUserUpdate
    }
    "empty field mask" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq.empty))
        .left
        .value shouldBe UpdatePathError.EmptyUpdateMask
    }
    "update path with invalid field path syntax" in {
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq(".primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(".primary_party")
      UserUpdateMapper
        .toUpdate(user, FieldMask(Seq(".user.primary_party")))
        .left
        .value shouldBe UpdatePathError.UnknownFieldPath(".user.primary_party")
    }
    "multiple update paths with the same field path" in {
      UserUpdateMapper
        .toUpdate(
          user,
          FieldMask(Seq("primary_party", "primary_party")),
        )
        .left
        .value shouldBe UpdatePathError.DuplicatedFieldPath("primary_party")
    }
  }
}
