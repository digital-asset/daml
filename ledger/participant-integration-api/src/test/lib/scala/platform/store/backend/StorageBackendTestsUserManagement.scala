// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.SQLException
import java.util.UUID

import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsUserManagement
    extends Matchers
    with Inside
    with StorageBackendSpec
    with OptionValues {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (user management)"

  // Representative values for each kind of user right
  private val right1 = ParticipantAdmin
  private val right2 = CanActAs(Ref.Party.assertFromString("party_act_as_1"))
  private val right3 = CanReadAs(Ref.Party.assertFromString("party_read_as_1"))
  private val zeroMicros: Long = 0

  private def tested = backend.userManagement

  it should "handle created_at and granted_at attributes correctly" in {
    val user = newUniqueUser()
    val internalId = executeSql(tested.createUser(user, createdAt = 123))
    executeSql(tested.addUserRight(internalId, right1, grantedAt = 234))
    executeSql(tested.getUser(user.id)).map(_.createdAt) shouldBe Some(123)
    executeSql(tested.getUserRights(internalId)).headOption.map(_.grantedAt) shouldBe Some(234)
  }

  it should "count number of user rights per user" in {
    val userA = newUniqueUser()
    val userB = newUniqueUser()
    val idA: Int = executeSql(tested.createUser(userA, createdAt = zeroMicros))
    val idB: Int = executeSql(tested.createUser(userB, createdAt = zeroMicros))

    def countA: Int = executeSql(tested.countUserRights(idA))

    def countB: Int = executeSql(tested.countUserRights(idB))

    val _ = executeSql(tested.addUserRight(idB, UserRight.ParticipantAdmin, grantedAt = zeroMicros))
    val _ =
      executeSql(
        tested.addUserRight(
          idB,
          UserRight.CanActAs(Ref.Party.assertFromString("act1")),
          grantedAt = zeroMicros,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idB,
          UserRight.CanReadAs(Ref.Party.assertFromString("read1")),
          grantedAt = zeroMicros,
        )
      )
    countA shouldBe zeroMicros
    countB shouldBe 3
    val _ = executeSql(tested.addUserRight(idA, UserRight.ParticipantAdmin, grantedAt = zeroMicros))
    countA shouldBe 1
    countB shouldBe 3
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanActAs(Ref.Party.assertFromString("act1")),
          grantedAt = zeroMicros,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanActAs(Ref.Party.assertFromString("act2")),
          grantedAt = zeroMicros,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanReadAs(Ref.Party.assertFromString("read1")),
          grantedAt = zeroMicros,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanReadAs(Ref.Party.assertFromString("read2")),
          grantedAt = zeroMicros,
        )
      )
    countA shouldBe 5
    countB shouldBe 3
    val _ = executeSql(
      tested.deleteUserRight(idA, UserRight.CanActAs(Ref.Party.assertFromString("act2")))
    )
    countA shouldBe 4
    countB shouldBe 3
  }

  it should "use invalid party string to mark absence of party" in {
    intercept[IllegalArgumentException](
      Ref.Party.assertFromString("!")
    ).getMessage shouldBe "non expected character 0x21 in Daml-LF Party \"!\""
  }

  it should "create user (createUser)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val internalId1 = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    // Attempting to add a duplicate user
    assertThrows[SQLException](executeSql(tested.createUser(user1, createdAt = zeroMicros)))
    val internalId2 = executeSql(tested.createUser(user2, createdAt = zeroMicros))
    val _ =
      executeSql(tested.createUser(newUniqueUser(emptyPrimaryParty = true), createdAt = zeroMicros))
    internalId1 should not equal internalId2
  }

  it should "handle user ops (getUser, deleteUser)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val _ = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    val getExisting = executeSql(tested.getUser(user1.id))
    val deleteExisting = executeSql(tested.deleteUser(user1.id))
    val deleteNonexistent = executeSql(tested.deleteUser(user2.id))
    val getDeleted = executeSql(tested.getUser(user1.id))
    val getNonexistent = executeSql(tested.getUser(user2.id))
    getExisting.value.domainUser shouldBe user1
    deleteExisting shouldBe true
    deleteNonexistent shouldBe false
    getDeleted shouldBe None
    getNonexistent shouldBe None
  }

  it should "get all users (getUsers) ordered by id" in {
    val user1 = newUniqueUser(userId = "user_id_1")
    val user2 = newUniqueUser(userId = "user_id_2")
    val user3 = newUniqueUser(userId = "user_id_3")
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 10)) shouldBe empty
    val _ = executeSql(tested.createUser(user3, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 10)) shouldBe Seq(
      user1,
      user3,
    )
    val _ = executeSql(tested.createUser(user2, createdAt = zeroMicros))
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 10)) shouldBe Seq(
      user1,
      user2,
      user3,
    )
  }

  it should "get all users (getUsers) ordered by id using binary collation" in {
    val user1 = newUniqueUser(userId = "a")
    val user2 = newUniqueUser(userId = "a!")
    val user3 = newUniqueUser(userId = "b")
    val user4 = newUniqueUser(userId = "a_")
    val user5 = newUniqueUser(userId = "!a")
    val user6 = newUniqueUser(userId = "_a")
    val users = Seq(user1, user2, user3, user4, user5, user6)
    users.foreach(user => executeSql(tested.createUser(user, createdAt = zeroMicros)))
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 10))
      .map(_.id) shouldBe Seq("!a", "_a", "a", "a!", "a_", "b")
  }

  it should "get a page of users (getUsers) ordered by id" in {
    val user1 = newUniqueUser(userId = "user_id_1")
    val user2 = newUniqueUser(userId = "user_id_2")
    val user3 = newUniqueUser(userId = "user_id_3")
    // Note: user4 doesn't exist and won't be created
    val user5 = newUniqueUser(userId = "user_id_5")
    val user6 = newUniqueUser(userId = "user_id_6")
    val user7 = newUniqueUser(userId = "user_id_7")
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 10)) shouldBe empty
    // Creating users in a random order
    val _ = executeSql(tested.createUser(user5, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user7, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user3, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user6, createdAt = zeroMicros))
    val _ = executeSql(tested.createUser(user2, createdAt = zeroMicros))
    // Get first 2 elements
    executeSql(tested.getUsersOrderedById(fromExcl = None, maxResults = 2)) shouldBe Seq(
      user1,
      user2,
    )
    // Get 3 users after user1
    executeSql(tested.getUsersOrderedById(maxResults = 3, fromExcl = Some(user1.id))) shouldBe Seq(
      user2,
      user3,
      user5,
    )
    // Get up to 10000 users after user1
    executeSql(
      tested.getUsersOrderedById(maxResults = 10000, fromExcl = Some(user1.id))
    ) shouldBe Seq(
      user2,
      user3,
      user5,
      user6,
      user7,
    )
    // Get some users after a non-existing user id
    executeSql(
      tested.getUsersOrderedById(
        maxResults = 2,
        fromExcl = Some(Ref.UserId.assertFromString("user_id_4")),
      )
    ) shouldBe Seq(user5, user6)
    // Get no users when requesting with after set the last existing user
    executeSql(tested.getUsersOrderedById(maxResults = 2, fromExcl = Some(user7.id))) shouldBe empty
    // Get no users when requesting with after set beyond the last existing user
    executeSql(
      tested.getUsersOrderedById(
        maxResults = 2,
        fromExcl = Some(Ref.UserId.assertFromString("user_id_8")),
      )
    ) shouldBe empty
  }

  it should "handle adding rights to non-existent user" in {
    val nonExistentUserInternalId = 123
    val allUsers = executeSql(tested.getUsersOrderedById(maxResults = 10, fromExcl = None))
    val rightExists = executeSql(tested.userRightExists(nonExistentUserInternalId, right2))
    allUsers shouldBe empty
    rightExists shouldBe false
  }

  it should "handle adding duplicate rights" in {
    val user1 = newUniqueUser()
    val adminRight = ParticipantAdmin
    val readAsRight = CanReadAs(Ref.Party.assertFromString("party_read_as_1"))
    val actAsRight = CanActAs(Ref.Party.assertFromString("party_act_as_1"))
    val internalId = executeSql(tested.createUser(user = user1, createdAt = zeroMicros))
    executeSql(tested.addUserRight(internalId, adminRight, grantedAt = zeroMicros))
    // Attempting to add a duplicate user admin right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, adminRight, grantedAt = zeroMicros))
    )
    executeSql(tested.addUserRight(internalId, readAsRight, grantedAt = zeroMicros))
    // Attempting to add a duplicate user readAs right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, readAsRight, grantedAt = zeroMicros))
    )
    executeSql(tested.addUserRight(internalId, actAsRight, grantedAt = zeroMicros))
    // Attempting to add a duplicate user actAs right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, actAsRight, grantedAt = zeroMicros))
    )
  }

  it should "handle removing absent rights" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    val delete1 = executeSql(tested.deleteUserRight(internalId, right1))
    val delete2 = executeSql(tested.deleteUserRight(internalId, right2))
    val delete3 = executeSql(tested.deleteUserRight(internalId, right3))
    delete1 shouldBe false
    delete2 shouldBe false
    delete3 shouldBe false
  }

  it should "handle multiple rights (getUserRights, addUserRight, deleteUserRight)" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    val rights1 = executeSql(tested.getUserRights(internalId))
    executeSql(tested.addUserRight(internalId, right1, grantedAt = zeroMicros))
    executeSql(tested.addUserRight(internalId, right2, grantedAt = zeroMicros))
    executeSql(tested.addUserRight(internalId, right3, grantedAt = zeroMicros))
    val rights2 = executeSql(tested.getUserRights(internalId))
    val deleteRight2 = executeSql(tested.deleteUserRight(internalId, right2))
    val rights3 = executeSql(tested.getUserRights(internalId))
    val deleteRight3 = executeSql(tested.deleteUserRight(internalId, right3))
    val rights4 = executeSql(tested.getUserRights(internalId))
    rights1 shouldBe empty
    rights2.map(_.domainRight) should contain theSameElementsAs Seq(right1, right2, right3)
    deleteRight2 shouldBe true
    rights3.map(_.domainRight) should contain theSameElementsAs Seq(right1, right3)
    deleteRight3 shouldBe true
    rights4.map(_.domainRight) should contain theSameElementsAs Seq(right1)
  }

  it should "add and delete a single right (userRightExists, addUserRight, deleteUserRight, getUserRights)" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = zeroMicros))
    // no rights
    val rightExists0 = executeSql(tested.userRightExists(internalId, right1))
    val rights0 = executeSql(tested.getUserRights(internalId))
    // add one rights
    executeSql(tested.addUserRight(internalId, right1, grantedAt = zeroMicros))
    val rightExists1 = executeSql(tested.userRightExists(internalId, right1))
    val rights1 = executeSql(tested.getUserRights(internalId))
    // delete
    val deleteRight = executeSql(tested.deleteUserRight(internalId, right1))
    val rightExists2 = executeSql(tested.userRightExists(internalId, right1))
    val rights2 = executeSql(tested.getUserRights(internalId))
    // no rights
    rightExists0 shouldBe false
    rights0 shouldBe empty
    rightExists1 shouldBe true
    rights1.map(_.domainRight) should contain theSameElementsAs Seq(right1)
    // deleted right
    deleteRight shouldBe true
    rightExists2 shouldBe false
    rights2 shouldBe empty
  }

  private def newUniqueUser(
      emptyPrimaryParty: Boolean = false,
      userId: String = "",
  ): User = {
    val uuid = UUID.randomUUID.toString
    val primaryParty =
      if (emptyPrimaryParty)
        None
      else
        Some(Ref.Party.assertFromString(s"primary_party_${uuid}"))
    val userIdStr = if (userId != "") userId else s"user_id_${uuid}"
    User(
      id = Ref.UserId.assertFromString(userIdStr),
      primaryParty = primaryParty,
    )
  }

}
