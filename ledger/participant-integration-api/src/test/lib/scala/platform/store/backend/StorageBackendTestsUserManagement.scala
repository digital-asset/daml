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
import anorm.{SqlParser, SqlStringInterpolation}

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

  private def tested = backend.userManagement

  it should "handle created_at and granted_at attributes correctly" in {
    val user = newUniqueUser()
    val internalId = executeSql(tested.createUser(user, createdAt = 123))
    executeSql(tested.addUserRight(internalId, right1, grantedAt = 234))

    val actualCreatedAt = executeSql(
      SQL"""
       SELECT created_at
       FROM participant_users
       WHERE user_id = ${user.id: String}
       """.as(SqlParser.long("created_at").single)(_)
    )
    val actualGrantedAt = executeSql(
      SQL"""
       SELECT granted_at
       FROM participant_user_rights
       WHERE user_internal_id = ${internalId}
       """.as(SqlParser.long("granted_at").single)(_)
    )
    actualCreatedAt shouldBe 123
    actualGrantedAt shouldBe 234
  }

  it should "count number of user rights per user" in {
    val userA = newUniqueUser()
    val userB = newUniqueUser()
    val idA: Int = executeSql(tested.createUser(userA, createdAt = 0))
    val idB: Int = executeSql(tested.createUser(userB, createdAt = 0))

    def countA: Int = executeSql(tested.countUserRights(idA))

    def countB: Int = executeSql(tested.countUserRights(idB))

    val _ = executeSql(tested.addUserRight(idB, UserRight.ParticipantAdmin, grantedAt = 0))
    val _ =
      executeSql(
        tested.addUserRight(
          idB,
          UserRight.CanActAs(Ref.Party.assertFromString("act1")),
          grantedAt = 0,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idB,
          UserRight.CanReadAs(Ref.Party.assertFromString("read1")),
          grantedAt = 0,
        )
      )
    countA shouldBe 0
    countB shouldBe 3
    val _ = executeSql(tested.addUserRight(idA, UserRight.ParticipantAdmin, grantedAt = 0))
    countA shouldBe 1
    countB shouldBe 3
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanActAs(Ref.Party.assertFromString("act1")),
          grantedAt = 0,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanActAs(Ref.Party.assertFromString("act2")),
          grantedAt = 0,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanReadAs(Ref.Party.assertFromString("read1")),
          grantedAt = 0,
        )
      )
    val _ =
      executeSql(
        tested.addUserRight(
          idA,
          UserRight.CanReadAs(Ref.Party.assertFromString("read2")),
          grantedAt = 0,
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
    val internalId1 = executeSql(tested.createUser(user1, createdAt = 0))
    // Attempting to add a duplicate user
    assertThrows[SQLException](executeSql(tested.createUser(user1, createdAt = 0)))
    val internalId2 = executeSql(tested.createUser(user2, createdAt = 0))
    val _ = executeSql(tested.createUser(newUniqueUser(emptyPrimaryParty = true), createdAt = 0))
    internalId1 should not equal internalId2
  }

  it should "handle user ops (getUser, deleteUser)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val _ = executeSql(tested.createUser(user1, createdAt = 0))
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

  it should "get users (getUsers)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val emptyUsers = executeSql(tested.getUsers())
    val _ = executeSql(tested.createUser(user1, createdAt = 0))
    val _ = executeSql(tested.createUser(user2, createdAt = 0))
    val allUsers = executeSql(tested.getUsers())
    emptyUsers shouldBe empty
    allUsers should contain theSameElementsAs Seq(user1, user2)
  }

  it should "handle adding rights to non-existent user" in {
    val nonExistentUserInternalId = 123
    val allUsers = executeSql(tested.getUsers())
    val _ = executeSql(tested.userRightExists(nonExistentUserInternalId, right2))
    allUsers shouldBe empty
  }

  it should "handle adding duplicate rights" in {
    val user1 = newUniqueUser()
    val adminRight = ParticipantAdmin
    val readAsRight = CanReadAs(Ref.Party.assertFromString("party_read_as_1"))
    val actAsRight = CanActAs(Ref.Party.assertFromString("party_act_as_1"))
    val internalId = executeSql(tested.createUser(user = user1, createdAt = 0))
    val addOk1 = executeSql(tested.addUserRight(internalId, adminRight, grantedAt = 0))
    // Attempting to add a duplicate user admin right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, adminRight, grantedAt = 0))
    )
    val addOk2 = executeSql(tested.addUserRight(internalId, readAsRight, grantedAt = 0))
    // Attempting to add a duplicate user readAs right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, readAsRight, grantedAt = 0))
    )
    val addOk3 = executeSql(tested.addUserRight(internalId, actAsRight, grantedAt = 0))
    // Attempting to add a duplicate user actAs right
    assertThrows[SQLException](
      executeSql(tested.addUserRight(internalId, actAsRight, grantedAt = 0))
    )
    addOk1 shouldBe true
    addOk2 shouldBe true
    addOk3 shouldBe true
  }

  it should "handle removing absent rights" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = 0))
    val delete1 = executeSql(tested.deleteUserRight(internalId, right1))
    val delete2 = executeSql(tested.deleteUserRight(internalId, right2))
    val delete3 = executeSql(tested.deleteUserRight(internalId, right3))
    delete1 shouldBe false
    delete2 shouldBe false
    delete3 shouldBe false
  }

  it should "handle multiple rights (getUserRights, addUserRight, deleteUserRight)" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = 0))
    val rights1 = executeSql(tested.getUserRights(internalId))
    val addRight1 = executeSql(tested.addUserRight(internalId, right1, grantedAt = 0))
    val addRight2 = executeSql(tested.addUserRight(internalId, right2, grantedAt = 0))
    val addRight3 = executeSql(tested.addUserRight(internalId, right3, grantedAt = 0))
    val rights2 = executeSql(tested.getUserRights(internalId))
    val deleteRight2 = executeSql(tested.deleteUserRight(internalId, right2))
    val rights3 = executeSql(tested.getUserRights(internalId))
    val deleteRight3 = executeSql(tested.deleteUserRight(internalId, right3))
    val rights4 = executeSql(tested.getUserRights(internalId))
    rights1 shouldBe empty
    addRight1 shouldBe true
    addRight2 shouldBe true
    addRight3 shouldBe true
    rights2 should contain theSameElementsAs Seq(right1, right2, right3)
    deleteRight2 shouldBe true
    rights3 should contain theSameElementsAs Seq(right1, right3)
    deleteRight3 shouldBe true
    rights4 should contain theSameElementsAs Seq(right1)
  }

  it should "add and delete a single right (userRightExists, addUserRight, deleteUserRight, getUserRights)" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user1, createdAt = 0))
    // no rights
    val rightExists0 = executeSql(tested.userRightExists(internalId, right1))
    val rights0 = executeSql(tested.getUserRights(internalId))
    // add one rights
    val addRight = executeSql(tested.addUserRight(internalId, right1, grantedAt = 0))
    val rightExists1 = executeSql(tested.userRightExists(internalId, right1))
    val rights1 = executeSql(tested.getUserRights(internalId))
    // delete
    val deleteRight = executeSql(tested.deleteUserRight(internalId, right1))
    val rightExists2 = executeSql(tested.userRightExists(internalId, right1))
    val rights2 = executeSql(tested.getUserRights(internalId))
    // no rights
    rightExists0 shouldBe false
    rights0 shouldBe empty
    // added right
    addRight shouldBe true
    rightExists1 shouldBe true
    rights1 should contain theSameElementsAs Seq(right1)
    // deleted right
    deleteRight shouldBe true
    rightExists2 shouldBe false
    rights2 shouldBe empty
  }

  private def newUniqueUser(emptyPrimaryParty: Boolean = false): User = {
    val uuid = UUID.randomUUID.toString
    val primaryParty =
      if (emptyPrimaryParty)
        None
      else
        Some(Ref.Party.assertFromString(s"primary_party_${uuid}"))
    User(
      id = Ref.UserId.assertFromString(s"user_id_${uuid}"),
      primaryParty = primaryParty,
    )
  }

}
