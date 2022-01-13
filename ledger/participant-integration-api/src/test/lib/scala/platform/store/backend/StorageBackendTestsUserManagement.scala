// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.util.UUID

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.domain.User
import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.lf.data.Ref
import com.daml.platform.testing.LogCollectorAssertions
import org.scalatest.flatspec.{AnyFlatSpec}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import scala.concurrent.Future
import scala.language.implicitConversions

private[backend] trait StorageBackendTestsUserManagement
  extends Matchers
    with Inside
    with StorageBackendSpec
    with ErrorsAssertions
    with LogCollectorAssertions
    with OptionValues {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (user management)"

  // Representative values for each kind of right
  private val right1 = ParticipantAdmin
  private val right2 = CanActAs(Ref.Party.assertFromString("party_act_as_1"))
  private val right3 = CanReadAs(Ref.Party.assertFromString("party_read_as_1"))

  private def tested = backend.userManagement

  /** Allows for assertions with more information in the error messages. */
  implicit def futureAssertions[T](future: Future[T]): FutureAssertions[T] =
    new FutureAssertions[T](future)

  it should "create user (createUser)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val internalId1 = executeSql(tested.createUser(user1))
    val _ = intercept[Throwable](executeSql(tested.createUser(user1)))
//    val _ = intercept[org.postgresql.util.PSQLException](executeSql(tested.createUser(user1)))
//      .mustFail2[StatusRuntimeException]("creating duplicate user")
    val internalId2 = executeSql(tested.createUser(user2))
    internalId1 should not equal internalId2
    // TODO pbatko: Should be FAILED_PRECONDITION ? (or more precisey ALREADY_EXISTS)
    // Logged message:
    // 13:06:30.954 [daml.index.db.threadpool.connection.storagebackendpostgresspec-6] ERROR c.d.p.s.appendonlydao.DbDispatcher - INDEX_DB_SQL_NON_TRANSIENT_ERROR(4,0): Processing the request failed due to a non-transient database error: ERROR: duplicate key value violates unique constraint "participant_users_user_id_key"
    //  Detail: Key (user_id)=(user_id_1) already exists. , context: {metric: "db", err-context: "{location=DatabaseSelfServiceError.scala:56}"}
    //org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "participant_users_user_id_key"
    //  Detail: Key (user_id)=(user_id_1) already exists.
//    assertInternalError(error)
  }

  it should "handle user ops (getUser, deleteUser)" in {
    val user1 = newUniqueUser()
    val user2 = newUniqueUser()
    val _ = executeSql(tested.createUser(user1))
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
    val _ = executeSql(tested.createUser(user1))
    val _ = executeSql(tested.createUser(user2))
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
    val internalId = executeSql(tested.createUser(user = user1))
    val addOk1 = executeSql(tested.addUserRight(internalId, adminRight))
    val _ = intercept[Throwable](executeSql(tested.addUserRight(internalId, adminRight)))
//      .mustFail2[StatusRuntimeException]("adding a duplicate admin right")
    val addOk2 = executeSql(tested.addUserRight(internalId, readAsRight))
    val _ = intercept[Throwable](executeSql(tested.addUserRight(internalId, readAsRight)))
//      .mustFail2[StatusRuntimeException]("adding a duplicate read as right")
    val addOk3 = executeSql(tested.addUserRight(internalId, actAsRight))
    val _ = intercept[Throwable](executeSql(tested.addUserRight(internalId, actAsRight)))
//      .mustFail2[StatusRuntimeException]("adding a duplicate act as right")
    addOk1 shouldBe true
//    assertInternalError(addError1)
    addOk2 shouldBe true
//    assertInternalError(addError2)
    addOk3 shouldBe true
//    assertInternalError(addError3)
  }

  it should "handle removing absent rights" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user = user1))
    val delete1 = executeSql(tested.deleteUserRight(internalId, right1))
    val delete2 = executeSql(tested.deleteUserRight(internalId, right2))
    val delete3 = executeSql(tested.deleteUserRight(internalId, right3))
    delete1 shouldBe false
    delete2 shouldBe false
    delete3 shouldBe false
  }

  it should "handle multiple rights (getUserRights, addUserRight, deleteUserRight)" in {
    val user1 = newUniqueUser()
    val internalId = executeSql(tested.createUser(user = user1))
    val rights1 = executeSql(tested.getUserRights(internalId))
    val addRight1 = executeSql(tested.addUserRight(internalId, right1))
    val addRight2 = executeSql(tested.addUserRight(internalId, right2))
    val addRight3 = executeSql(tested.addUserRight(internalId, right3))
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
    val internalId = executeSql(tested.createUser(user = user1))
    // no rights
    val rightExists0 = executeSql(tested.userRightExists(internalId, right1))
    val rights0 = executeSql(tested.getUserRights(internalId))
    // add one rights
    val addRight = executeSql(tested.addUserRight(internalId, right1))
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

  private def newUniqueUser(): User = {
    val uuid = UUID.randomUUID.toString
    User(
      id = Ref.UserId.assertFromString(s"user_id_${uuid}"),
      primaryParty = Some(Ref.Party.assertFromString(s"primary_party_${uuid}")),
    )
  }
}
