// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.{ObjectMeta, User, UserRight}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.UserManagementStore.{UserInfo, UserNotFound, UsersPage}
import com.daml.platform.localstore.api.{ObjectMetaUpdate, UserUpdate}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.freespec.AsyncFreeSpec

//TODO DPP-1299 Include IdentityProviderAdmin
class CachedUserManagementStoreSpec
    extends AsyncFreeSpec
    with UserStoreTests
    with MockitoSugar
    with ArgumentMatchersSugar {

  override def newStore() = new CachedUserManagementStore(
    new InMemoryUserManagementStore(createAdmin = false),
    expiryAfterWriteInSeconds = 1,
    maximumCacheSize = 10,
    Metrics.ForTesting,
  )

  private val user = User(
    id = Ref.UserId.assertFromString("user_id1"),
    primaryParty = Some(Ref.Party.assertFromString("primary_party1")),
    false,
    ObjectMeta.empty,
  )
  private val createdUser1 = User(
    id = Ref.UserId.assertFromString("user_id1"),
    primaryParty = Some(Ref.Party.assertFromString("primary_party1")),
    false,
    ObjectMeta(
      resourceVersionO = Some(0),
      annotations = Map.empty,
    ),
  )

  private val right1 = UserRight.CanActAs(Ref.Party.assertFromString("party_id1"))
  private val right2 = UserRight.ParticipantAdmin
  private val right3 = UserRight.CanActAs(Ref.Party.assertFromString("party_id2"))
  private val rights = Set(right1, right2)
  private val userInfo = UserInfo(user, rights)
  private val createdUserInfo = UserInfo(createdUser1, rights)
  private val identityProviderId: Ref.IdentityProviderId = Ref.IdentityProviderId.Default

  "test user-not-found cache result gets invalidated after user creation" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)
    for {
      getYetNonExistent <- tested.getUserInfo(userInfo.user.id, identityProviderId)
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get <- tested.getUserInfo(user.id, identityProviderId)
    } yield {
      getYetNonExistent shouldBe Left(UserNotFound(createdUserInfo.user.id))
      get shouldBe Right(createdUserInfo)
    }
  }

  "test cache population" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id, identityProviderId)
      get2 <- tested.getUserInfo(user.id, identityProviderId)
      getUser <- tested.getUser(user.id, identityProviderId)
      listRights <- tested.listUserRights(user.id, identityProviderId)
    } yield {
      verify(delegate, times(1)).createUser(userInfo.user, userInfo.rights)
      verify(delegate, times(1)).getUserInfo(userInfo.user.id, identityProviderId)
      verifyNoMoreInteractions(delegate)
      get1 shouldBe Right(createdUserInfo)
      get2 shouldBe Right(createdUserInfo)
      getUser shouldBe Right(createdUserInfo.user)
      listRights shouldBe Right(createdUserInfo.rights)
    }
  }

  "test cache invalidation after every write method" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    val userInfo = UserInfo(user, rights)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id, identityProviderId)
      _ <- tested.grantRights(user.id, Set(right1), identityProviderId)
      get2 <- tested.getUserInfo(user.id, identityProviderId)
      _ <- tested.revokeRights(user.id, Set(right3), identityProviderId)
      get3 <- tested.getUserInfo(user.id, identityProviderId)
      _ <- tested.updateUser(
        UserUpdate(
          id = user.id,
          primaryPartyUpdateO = Some(Some(Ref.Party.assertFromString("newPp"))),
          metadataUpdate = ObjectMetaUpdate.empty,
        )
      )
      get4 <- tested.getUserInfo(user.id, identityProviderId)
      _ <- tested.deleteUser(user.id, identityProviderId)
      get5 <- tested.getUserInfo(user.id, identityProviderId)

    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, userInfo.rights)
      order.verify(delegate, times(1)).getUserInfo(user.id, identityProviderId)
      order
        .verify(delegate, times(1))
        .grantRights(eqTo(user.id), any[Set[UserRight]], eqTo(identityProviderId))(
          any[LoggingContext]
        )
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, identityProviderId)
      order
        .verify(delegate, times(1))
        .revokeRights(eqTo(user.id), any[Set[UserRight]], eqTo(identityProviderId))(
          any[LoggingContext]
        )
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, identityProviderId)
      order.verify(delegate, times(1)).updateUser(any[UserUpdate])(any[LoggingContext])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, identityProviderId)
      order.verify(delegate, times(1)).deleteUser(userInfo.user.id, identityProviderId)
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, identityProviderId)
      order.verifyNoMoreInteractions()
      get1 shouldBe Right(createdUserInfo)
      get2 shouldBe Right(createdUserInfo)
      get3 shouldBe Right(createdUserInfo)
      get4.value.user.primaryParty shouldBe Some(Ref.Party.assertFromString("newPp"))
      get5 shouldBe Left(UserNotFound(createdUser1.id))
    }
  }

  "listing all users should not be cached" in {
    val delegate = spy(new InMemoryUserManagementStore(createAdmin = false))
    val tested = createTested(delegate)

    for {
      res0 <- tested.createUser(user, rights)
      res1 <- tested.listUsers(
        fromExcl = None,
        maxResults = 100,
        identityProviderId = identityProviderId,
      )
      res2 <- tested.listUsers(
        fromExcl = None,
        maxResults = 100,
        identityProviderId = identityProviderId,
      )
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, rights)
      order
        .verify(delegate, times(2))
        .listUsers(fromExcl = None, maxResults = 100, identityProviderId = identityProviderId)
      order.verifyNoMoreInteractions()
      res0 shouldBe Right(createdUser1)
      res1 shouldBe Right(UsersPage(Seq(createdUser1)))
      res2 shouldBe Right(UsersPage(Seq(createdUser1)))
    }
  }

  "cache entries expire after a set time" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    for {
      create1 <- tested.createUser(user, rights)
      get1 <- tested.getUserInfo(user.id, identityProviderId)
      get2 <- tested.getUserInfo(user.id, identityProviderId)
      get3 <- {
        Thread.sleep(2000); tested.getUserInfo(user.id, identityProviderId)
      }
    } yield {
      val order = inOrder(delegate)
      order
        .verify(delegate, times(1))
        .createUser(any[User], any[Set[UserRight]])(any[LoggingContext])
      order
        .verify(delegate, times(2))
        .getUserInfo(any[Ref.UserId], eqTo(identityProviderId))(any[LoggingContext])
      order.verifyNoMoreInteractions()
      create1 shouldBe Right(createdUser1)
      get1 shouldBe Right(createdUserInfo)
      get2 shouldBe Right(createdUserInfo)
      get3 shouldBe Right(createdUserInfo)
    }
  }

  private def createTested(delegate: InMemoryUserManagementStore): CachedUserManagementStore = {
    new CachedUserManagementStore(
      delegate,
      expiryAfterWriteInSeconds = 1,
      maximumCacheSize = 10,
      Metrics.ForTesting,
    )
  }
}
