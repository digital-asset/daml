// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.daml.lf.data.Ref
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  ObjectMeta,
  User,
  UserRight,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore.{
  UserInfo,
  UserNotFound,
  UsersPage,
}
import com.digitalasset.canton.ledger.localstore.api.{
  ObjectMetaUpdate,
  UserManagementStore,
  UserUpdate,
}
import com.digitalasset.canton.ledger.localstore.{
  CachedUserManagementStore,
  InMemoryUserManagementStore,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class CachedUserManagementStoreSpec
    extends AsyncFreeSpec
    with UserStoreTests
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {

  override def newStore(): UserManagementStore =
    createTested(new InMemoryUserManagementStore(createAdmin = false, loggerFactory))

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] =
    Future.unit

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
  private val filter: IdentityProviderId = IdentityProviderId.Default
  private val idpId = IdentityProviderId.Default

  "test user-not-found cache result gets invalidated after user creation" in {
    val delegate = spy(new InMemoryUserManagementStore(loggerFactory = loggerFactory))
    val tested = createTested(delegate)
    for {
      getYetNonExistent <- tested.getUserInfo(userInfo.user.id, idpId)
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get <- tested.getUserInfo(user.id, idpId)
    } yield {
      getYetNonExistent shouldBe Left(UserNotFound(createdUserInfo.user.id))
      get shouldBe Right(createdUserInfo)
    }
  }

  "test cache population" in {
    val delegate = spy(new InMemoryUserManagementStore(loggerFactory = loggerFactory))
    val tested = createTested(delegate)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id, idpId)
      get2 <- tested.getUserInfo(user.id, idpId)
      getUser <- tested.getUser(user.id, idpId)
      listRights <- tested.listUserRights(user.id, idpId)
    } yield {
      verify(delegate, times(1)).createUser(userInfo.user, userInfo.rights)
      verify(delegate, times(1)).getUserInfo(userInfo.user.id, idpId)
      verifyNoMoreInteractions(delegate)
      get1 shouldBe Right(createdUserInfo)
      get2 shouldBe Right(createdUserInfo)
      getUser shouldBe Right(createdUserInfo.user)
      listRights shouldBe Right(createdUserInfo.rights)
    }
  }

  "test cache invalidation after every write method" in {
    val delegate = spy(new InMemoryUserManagementStore(loggerFactory = loggerFactory))
    val tested = createTested(delegate)

    val userInfo = UserInfo(user, rights)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id, idpId)
      _ <- tested.grantRights(user.id, Set(right1), idpId)
      get2 <- tested.getUserInfo(user.id, idpId)
      _ <- tested.revokeRights(user.id, Set(right3), idpId)
      get3 <- tested.getUserInfo(user.id, idpId)
      _ <- tested.updateUser(
        UserUpdate(
          id = user.id,
          identityProviderId = IdentityProviderId.Default,
          primaryPartyUpdateO = Some(Some(Ref.Party.assertFromString("newPp"))),
          metadataUpdate = ObjectMetaUpdate.empty,
        )
      )
      get4 <- tested.getUserInfo(user.id, idpId)
      _ <- tested.deleteUser(user.id, idpId)
      get5 <- tested.getUserInfo(user.id, idpId)

    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, userInfo.rights)
      order.verify(delegate, times(1)).getUserInfo(user.id, idpId)
      order
        .verify(delegate, times(1))
        .grantRights(eqTo(user.id), any[Set[UserRight]], eqTo(idpId))(any[LoggingContextWithTrace])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, idpId)
      order
        .verify(delegate, times(1))
        .revokeRights(eqTo(user.id), any[Set[UserRight]], eqTo(idpId))(any[LoggingContextWithTrace])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, idpId)
      order.verify(delegate, times(1)).updateUser(any[UserUpdate])(any[LoggingContextWithTrace])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, idpId)
      order.verify(delegate, times(1)).deleteUser(userInfo.user.id, idpId)
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id, idpId)
      order.verifyNoMoreInteractions()
      get1 shouldBe Right(createdUserInfo)
      get2 shouldBe Right(createdUserInfo)
      get3 shouldBe Right(createdUserInfo)
      get4.value.user.primaryParty shouldBe Some(Ref.Party.assertFromString("newPp"))
      get5 shouldBe Left(UserNotFound(createdUser1.id))
    }
  }

  "listing all users should not be cached" in {
    val delegate = spy(
      new InMemoryUserManagementStore(
        createAdmin = false,
        loggerFactory = loggerFactory,
      )
    )
    val tested = createTested(delegate)

    for {
      res0 <- tested.createUser(user, rights)
      res1 <- tested.listUsers(
        fromExcl = None,
        maxResults = 100,
        identityProviderId = filter,
      )
      res2 <- tested.listUsers(
        fromExcl = None,
        maxResults = 100,
        identityProviderId = filter,
      )
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, rights)
      order
        .verify(delegate, times(2))
        .listUsers(fromExcl = None, maxResults = 100, identityProviderId = filter)
      order.verifyNoMoreInteractions()
      res0 shouldBe Right(createdUser1)
      res1 shouldBe Right(UsersPage(Seq(createdUser1)))
      res2 shouldBe Right(UsersPage(Seq(createdUser1)))
    }
  }

  "cache entries expire after a set time" in {
    val delegate = spy(new InMemoryUserManagementStore(loggerFactory = loggerFactory))
    val tested = createTested(delegate)

    for {
      create1 <- tested.createUser(user, rights)
      get1 <- tested.getUserInfo(user.id, idpId)
      get2 <- tested.getUserInfo(user.id, idpId)
      get3 <- {
        Threading.sleep(2000); tested.getUserInfo(user.id, idpId)
      }
    } yield {
      val order = inOrder(delegate)
      order
        .verify(delegate, times(1))
        .createUser(any[User], any[Set[UserRight]])(any[LoggingContextWithTrace])
      order
        .verify(delegate, times(2))
        .getUserInfo(any[Ref.UserId], eqTo(idpId))(any[LoggingContextWithTrace])
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
      LedgerApiServerMetrics.ForTesting,
      loggerFactory,
    )
  }
}
