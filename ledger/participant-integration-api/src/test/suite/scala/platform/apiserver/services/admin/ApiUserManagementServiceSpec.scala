// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{DamlContextualizedErrorLogger, ErrorsAssertions}
import com.daml.ledger.api.domain.ObjectMeta
import com.daml.lf.data.Ref.IdString
import com.daml.ledger.participant.state.index.v2.{ObjectMetaUpdate, UpdateMaskTrie_mut, UserUpdate}
import scalapb.FieldMaskUtil
import com.daml.ledger.api.v1.admin.user_management_service.{UpdateUserRequest, User}
import com.daml.ledger.participant.state.index.v2.AnnotationsUpdate.Replace
import com.daml.platform.apiserver.page_tokens.ListUsersPageTokenPayload
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO pbatko: Move to a separate test class
class ApiUserManagementServiceSpec2
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with ErrorsAssertions {

  it should "resetting nested message in shortcut notation" in {
    // Do reset "foo.bar" to it's default value (None) when it's exact path is specified
    val trie = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo", "bar"))
    )
    trie.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = None,
      defaultValue = None,
    ) shouldBe Some(None)

    // Do not reset "foo.bar" to it's default value (None) when it's exact path is not specified - case 1: a longer path is specified
    val trie2 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo", "bar", "baz"))
    )
    trie2.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = None,
      defaultValue = None,
    ) shouldBe None

    // Do not reset "foo.bar" to it's default value (None) when it's exact path is not specified - case 1: a shorter path is specified
    val trie3 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo"))
    )
    trie3.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = None,
      defaultValue = None,
    ) shouldBe None

    // Do update "foo.bar" to a non default value when it's exact path is specified
    val trie4 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo", "bar"))
    )
    trie4.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = Some("nonDefaultValue"),
      defaultValue = None,
    ) shouldBe Some(Some("nonDefaultValue"))

    // Do not update "foo.bar" to a non default value when it's exact path is not specified - case 1: a longer path is specified
    val trie5 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo", "bar", "baz"))
    )
    trie5.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = Some("nonDefaultValue"),
      defaultValue = None,
    ) shouldBe None

    // Do update "foo.bar" to a non default value when it's exact path is not specified - case 1: a shorter path is specified
    val trie6 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo"))
    )
    trie6.determineUpdate(
      updatePath = Seq("foo", "bar")
    )(
      newValueCandidate = Some("nonDefaultValue"),
      defaultValue = None,
    ) shouldBe Some(Some("nonDefaultValue"))
  }

  it should "construct correct trie" in {
    val trie0 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo"))
    )
    trie0 shouldBe new UpdateMaskTrie_mut(
      nodes = collection.mutable.SortedMap(
        "foo" -> new UpdateMaskTrie_mut(
          nodes = collection.mutable.SortedMap.empty,
          exists = true,
        )
      ),
      exists = false,
    )

    collection.mutable.SortedMap("a" -> "b") shouldBe collection.mutable.SortedMap("a" -> "b")

    val trie2 = UpdateMaskTrie_mut.fromPaths(
      Seq(Seq("foo", "bar", "baz"))
    )
    trie2 shouldBe new UpdateMaskTrie_mut(
      nodes = collection.mutable.SortedMap(
        "foo" -> new UpdateMaskTrie_mut(
          nodes = collection.mutable.SortedMap(
            "bar" -> new UpdateMaskTrie_mut(
              nodes = collection.mutable.SortedMap(
                "baz" -> new UpdateMaskTrie_mut(
                  nodes = collection.mutable.SortedMap.empty,
                  exists = true,
                )
              ),
              exists = false,
            )
          ),
          exists = false,
        )
      ),
      exists = false,
    )
  }

}
class ApiUserManagementServiceSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with ErrorsAssertions {

  private val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass)

  private val userId1: IdString.UserId = Ref.UserId.assertFromString("u1")
  private val partyId1 = Ref.Party.assertFromString("party")

  // TODO pbatko: Redundant tests
  it should "check valid field masks" in {
    FieldMaskUtil.isValid[UpdateUserRequest](
      FieldMask(
        paths = Seq(
          "user.is_deactivated",
          "user.metadata.annotations",
        )
      )
    ) shouldBe true
    FieldMaskUtil.isValid[UpdateUserRequest](
      FieldMask(
        paths = Seq(
          "user",
          "user.is_deactivated",
          "user.metadata.annotations",
        )
      )
    ) shouldBe true
  }

  it should "map api request to update" in {
    val user = com.daml.ledger.api.domain.User(
      id = userId1,
      primaryParty = None,
      isDeactivated = false,
      metadata = ObjectMeta(
        resourceVersionO = None,
        annotations = Map(
          "a" -> "b"
        ),
      ),
    )

    val actual_individualFields = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user.is_deactivated",
          "user.primary_party",
          "user.metadata.annotations",
        )
      ),
    )
    val actual_mergeMetadata = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user.is_deactivated",
          "user.primary_party",
          "user.metadata",
        )
      ),
    )

    actual_individualFields shouldBe UserUpdate(
      id = userId1,
      // NOTE: This field is absent in the update mask
      primaryPartyUpdateO = Some(None),
      // NOTE: This field is present in the update mask
      isDeactivatedUpdateO = Some(false),
      metadataUpdate = ObjectMetaUpdate(
        resourceVersionO = None,
        // NOTE: This nested field present in the update mask
        annotationsUpdateO = Some(
          Replace(
            Map(
              "a" -> "b"
            )
          )
        ),
      ),
    )
    actual_individualFields shouldBe actual_mergeMetadata

    val actual_mergeUser = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user"
        )
      ),
    )
    actual_mergeUser shouldBe UserUpdate(
      id = userId1,
      // NOTE: This field's exact path is absent from the update mask and it's value is default so we expect no updates
      primaryPartyUpdateO = None,
      // NOTE: This field's exact path is absent from the update mask and it's value is default so we expect no updates
      isDeactivatedUpdateO = None,
      metadataUpdate = ObjectMetaUpdate(
        resourceVersionO = None,
        // NOTE: This field's exact path is absent from the update mask but it's value is npn default so we expect an update
        annotationsUpdateO = Some(
          Replace(
            Map(
              "a" -> "b"
            )
          )
        ),
      ),
    )
  }

  it should "map api request to update - merge user and reset is_deactivated" in {
    val user = com.daml.ledger.api.domain.User(
      id = userId1,
      primaryParty = Some(partyId1),
      // NOTE: resetting this field and updating all other fields
      isDeactivated = false,
      metadata = ObjectMeta(
        resourceVersionO = None,
        annotations = Map(
          "a" -> "b"
        ),
      ),
    )

    val actual = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user",
          "user.is_deactivated",
        )
      ),
    )
    actual shouldBe UserUpdate(
      id = userId1,
      primaryPartyUpdateO = Some(Some(partyId1)),
      isDeactivatedUpdateO = Some(false),
      metadataUpdate = ObjectMetaUpdate(
        resourceVersionO = None,
        annotationsUpdateO = Some(
          Replace(
            Map(
              "a" -> "b"
            )
          )
        ),
      ),
    )

    val actualSame1 = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user",
          "user.metadata",
          "user.is_deactivated",
        )
      ),
    )
    val actualSame2 = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user.primary_party",
          "user.metadata",
          "user.is_deactivated",
        )
      ),
    )
    val actualSame3 = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq(
          "user.primary_party",
          "user.metadata.annotations",
          "user.is_deactivated",
        )
      ),
    )
    actualSame1 shouldBe actual
    actualSame2 shouldBe actual
    actualSame3 shouldBe actual
  }

  it should "map api request to update - all fields set but missing from the mask" in {

    val user = com.daml.ledger.api.domain.User(
      id = userId1,
      primaryParty = Some(partyId1),
      isDeactivated = true,
      metadata = ObjectMeta(
        resourceVersionO = None,
        annotations = Map(
          "a" -> "b"
        ),
      ),
    )

    val actual1 = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq()
      ),
    )
    val actual2 = UpdateMapper.toUserUpdate(
      user = user,
      updateMask = FieldMask.apply(
        Seq("some_other_field")
      ),
    )

    actual1 shouldBe UserUpdate(
      id = userId1,
      primaryPartyUpdateO = None,
      isDeactivatedUpdateO = None,
      metadataUpdate = ObjectMetaUpdate(
        resourceVersionO = None,
        annotationsUpdateO = None,
      ),
    )
    actual2 shouldBe actual1
  }

  // TODO pbatko: test trie in separate test class
  it should "implement a Trie with subtree extraction" in {

    val p0 = Seq(
      Seq("a1", "a2", "c3"),
      Seq("a1", "b2", "c3"),
      Seq("a1", "b2", "d3", "a4"),
      Seq("a1", "b2", "d3", "b4"),
      Seq("b1"),
    )
    val t = UpdateMaskTrie_mut.fromPaths(p0)

    t.toPaths shouldBe p0

    t.subtree(Seq("a1")).get.toPaths shouldBe Seq(
      Seq("a2", "c3"),
      Seq("b2", "c3"),
      Seq("b2", "d3", "a4"),
      Seq("b2", "d3", "b4"),
    )

    t.subtree(Seq.empty).get.toPaths shouldBe p0
    t.subtree(Seq("a1", "b2")).get.toPaths shouldBe Seq(
      Seq("c3"),
      Seq("d3", "a4"),
      Seq("d3", "b4"),
    )
    t.subtree(Seq("a1", "b2", "dummy")) shouldBe None
    t.subtree(Seq("dummy")) shouldBe None
    t.subtree(Seq("")) shouldBe None

    t.subtree(Seq("b1")).get.toPaths shouldBe Seq.empty
    t.subtree(Seq("a1", "b2", "c3")).get.toPaths shouldBe Seq.empty
    t.subtree(Seq("a1", "b2", "d3", "b4")).get.toPaths shouldBe Seq.empty
  }

  // TODO pbatko: redundant test?
  it should "handle field mask in update user calls" in {
    val userFieldNumber: String = UpdateUserRequest.javaDescriptor
      .findFieldByNumber(UpdateUserRequest.USER_FIELD_NUMBER)
      .getName
    val userFieldNumber2: String = UpdateUserRequest.scalaDescriptor
      .findFieldByNumber(UpdateUserRequest.USER_FIELD_NUMBER)
      .getOrElse(sys.error("uknwon field number"))
      .name

    def concat(names: String*): String = names.mkString(".")

    import com.daml.ledger.participant.state.index.v2.MyFieldMaskUtils.fieldNameForNumber

    val fieldMaskPaths = Seq(
      concat(
        fieldNameForNumber[UpdateUserRequest](UpdateUserRequest.USER_FIELD_NUMBER),
        fieldNameForNumber[User](User.IS_DEACTIVATED_FIELD_NUMBER),
      ),
      concat(
        fieldNameForNumber[UpdateUserRequest](UpdateUserRequest.USER_FIELD_NUMBER),
        fieldNameForNumber[User](User.METADATA_FIELD_NUMBER),
        fieldNameForNumber[com.daml.ledger.api.v1.admin.object_meta.ObjectMeta](
          com.daml.ledger.api.v1.admin.object_meta.ObjectMeta.ANNOTATIONS_FIELD_NUMBER
        ),
      ),
    )

    fieldMaskPaths shouldBe Seq(
      "user.is_deactivated",
      "user.metadata.annotations",
    )

    val fieldMask = FieldMask(
      paths = fieldMaskPaths
    )
    FieldMaskUtil.isValid[UpdateUserRequest](fieldMask) shouldBe true

    userFieldNumber shouldBe userFieldNumber2
  }

  it should "test users page token encoding and decoding" in {
    val id2 = Ref.UserId.assertFromString("user2")
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(Some(id2))
    actualNextPageToken shouldBe "CgV1c2VyMg=="
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken)(
      errorLogger
    ) shouldBe Right(
      Some(id2)
    )
  }

  it should "test users empty page token encoding and decoding" in {
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(None)
    actualNextPageToken shouldBe ("")
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken)(
      errorLogger
    ) shouldBe Right(None)
  }

  it should "return invalid argument error when token is not a base64" in {
    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken("not-a-base64-string!!")(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is base64 but not a valid protobuf" in {
    val notValidProtoBufBytes = "not a valid proto buf".getBytes()
    val badPageToken = new String(
      Base64.getEncoder.encode(notValidProtoBufBytes),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is valid base64 encoded protobuf but does not contain a valid user id string" in {
    val notValidUserId = "not a valid user id"
    Ref.UserId.fromString(notValidUserId).isLeft shouldBe true
    val payload = ListUsersPageTokenPayload(
      userIdLowerBoundExcl = notValidUserId
    )
    val badPageToken = new String(
      Base64.getEncoder.encode(payload.toByteArray),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }
}
