// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.UserRight.{CanActAs, CanReadAs, ParticipantAdmin}
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.lf.data.Ref
import org.scalatest.Inside
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO participant user management: Refactor and make sure all is covered
private[backend] trait StorageBackendTestsUserManagement
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (user management)"

  it should "check if rights exist" in {
    val tested = backend.userManagement
    val user = User(
      id = Ref.UserId.assertFromString("user_id_123"),
      primaryParty = Some(Ref.Party.assertFromString("primary_party_123")),
    )
    val nonExistentUserInternalId = 123
    for {
      user_id <- executeSql(tested.createUser(user = user, createdAt = 0))
      rightExists1 <- executeSql(
        tested.userRightExists(internalId = user_id, right = ParticipantAdmin)
      )
      rightExists2 <- executeSql(
        tested.userRightExists(
          internalId = user_id,
          right = CanActAs(Ref.Party.assertFromString("party_act_as_1")),
        )
      )

      rightExists3 <- executeSql(
        tested.userRightExists(
          internalId = nonExistentUserInternalId,
          right = CanActAs(Ref.Party.assertFromString("party_act_as_1")),
        )
      )
      rightAdded1 <- executeSql(
        tested.addUserRight(internalId = user_id, right = ParticipantAdmin, grantedAt = 0)
      )
      rightAdded2 <- executeSql(
        tested.addUserRight(
          internalId = user_id,
          right = CanActAs(Ref.Party.assertFromString("party_act_as_1")),
          grantedAt = 0,
        )
      )
      rightExists1b <- executeSql(
        tested.userRightExists(internalId = user_id, right = ParticipantAdmin)
      )
      rightExists2b <- executeSql(
        tested.userRightExists(
          internalId = user_id,
          right = CanActAs(Ref.Party.assertFromString("party_act_as_1")),
        )
      )
    } yield {
      user_id shouldBe 1
      rightExists1 shouldBe false
      rightExists2 shouldBe false
      rightExists3 shouldBe false
      rightAdded1 shouldBe true
      rightAdded2 shouldBe true
      rightExists1b shouldBe true
      rightExists2b shouldBe true

    }
  }
  it should "create user with rights" in {
    val user = User(
      id = Ref.UserId.assertFromString("user_id_123"),
      primaryParty = Some(Ref.Party.assertFromString("primary_party_123")),
    )
    val rights: Seq[UserRight] = Seq(
      ParticipantAdmin,
      CanActAs(Ref.Party.assertFromString("party_act_as_1")),
      CanActAs(Ref.Party.assertFromString("party_act_as_2")),
      CanReadAs(Ref.Party.assertFromString("party_read_as_1")),
    )
    val rightsToAdd: Seq[UserRight] = Seq(
      CanActAs(Ref.Party.assertFromString("party_act_as_3")),
      CanReadAs(Ref.Party.assertFromString("party_read_as_2")),
    )

    val tested = backend.userManagement

    for {
      user_id <- executeSql(tested.createUser(user = user, createdAt = 0))
      right1 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rights(0), grantedAt = 0)
      )
      right2 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rights(1), grantedAt = 0)
      )
      right3 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rights(2), grantedAt = 0)
      )
      right4 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rights(3), grantedAt = 0)
      )

      addedUser <- executeSql(tested.getUser(id = user.id))
      addedUserRights <- executeSql(tested.getUserRights(internalId = user_id))

      addedRight1 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rightsToAdd(0), grantedAt = 0)
      )
      addedRight2 <- executeSql(
        tested.addUserRight(internalId = user_id, right = rightsToAdd(1), grantedAt = 0)
      )
      allUserRights <- executeSql(tested.getUserRights(internalId = user_id))

      _ <- executeSql(tested.deleteUser(id = user.id))
      deletedUser <- executeSql(tested.getUser(id = user.id))
      deletedRights <- executeSql(tested.getUserRights(internalId = user_id))
    } yield {
      user_id shouldBe 2
      right1 shouldBe true
      right2 shouldBe true
      right3 shouldBe true
      right4 shouldBe true

      addedUser shouldBe defined
      addedUser.get.internalId shouldBe 2
      addedUser.get.domainUser shouldBe user

      addedUserRights shouldBe rights.toSet

      addedRight1 shouldBe true
      addedRight2 shouldBe true

      allUserRights shouldBe (rightsToAdd.toSet ++ rights.toSet)

      deletedUser shouldBe None
      deletedRights shouldBe Set.empty

    }
  }

}
