// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.{
  UserInfo,
  UserNotFound,
  UsersPage,
}
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.platform.usermanagement.UserManagementStoreSpecBase
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class CachedUserManagementStoreSpec
    extends AsyncFreeSpec
    with UserManagementStoreSpecBase
    with TestResourceContext
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val user = User(
    id = Ref.UserId.assertFromString("user_id1"),
    primaryParty = Some(Ref.Party.assertFromString("primary_party1")),
  )
  private val right1 = UserRight.CanActAs(Ref.Party.assertFromString("party_id1"))
  private val right2 = UserRight.ParticipantAdmin
  private val right3 = UserRight.CanActAs(Ref.Party.assertFromString("party_id2"))
  private val rights = Set(right1, right2)
  private val userInfo = UserInfo(user, rights)

  "test user-not-found cache result gets invalidated after user creation" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)
    for {
      getYetNonExistent <- tested.getUserInfo(userInfo.user.id)
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get <- tested.getUserInfo(user.id)
    } yield {
      getYetNonExistent shouldBe Left(UserNotFound(userInfo.user.id))
      get shouldBe Right(userInfo)
    }
  }

  "test cache population" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id)
      get2 <- tested.getUserInfo(user.id)
      getUser <- tested.getUser(user.id)
      listRights <- tested.listUserRights(user.id)
    } yield {
      verify(delegate, times(1)).createUser(userInfo.user, userInfo.rights)
      verify(delegate, times(1)).getUserInfo(userInfo.user.id)
      verifyNoMoreInteractions(delegate)
      get1 shouldBe Right(userInfo)
      get2 shouldBe Right(userInfo)
      getUser shouldBe Right(userInfo.user)
      listRights shouldBe Right(userInfo.rights)
    }
  }

  "test cache invalidation after every write method" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    val userInfo = UserInfo(user, rights)

    for {
      _ <- tested.createUser(userInfo.user, userInfo.rights)
      get1 <- tested.getUserInfo(user.id)
      _ <- tested.grantRights(user.id, Set(right1))
      get2 <- tested.getUserInfo(user.id)
      _ <- tested.revokeRights(user.id, Set(right3))
      get3 <- tested.getUserInfo(user.id)
      _ <- tested.deleteUser(user.id)
      get4 <- tested.getUserInfo(user.id)
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, userInfo.rights)
      order.verify(delegate, times(1)).getUserInfo(user.id)
      order
        .verify(delegate, times(1))
        .grantRights(eqTo(user.id), any[Set[UserRight]])(any[LoggingContext])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id)
      order
        .verify(delegate, times(1))
        .revokeRights(eqTo(user.id), any[Set[UserRight]])(any[LoggingContext])
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id)
      order.verify(delegate, times(1)).deleteUser(userInfo.user.id)
      order.verify(delegate, times(1)).getUserInfo(userInfo.user.id)
      order.verifyNoMoreInteractions()
      get1 shouldBe Right(userInfo)
      get2 shouldBe Right(userInfo)
      get3 shouldBe Right(userInfo)
      get4 shouldBe Left(UserNotFound(user.id))
    }
  }

  "listing all users should not be cached" in {
    val delegate = spy(new InMemoryUserManagementStore(createAdmin = false))
    val tested = createTested(delegate)

    for {
      res0 <- tested.createUser(user, rights)
      res1 <- tested.listUsers(fromExcl = None, maxResults = 100)
      res2 <- tested.listUsers(fromExcl = None, maxResults = 100)
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(user, rights)
      order.verify(delegate, times(2)).listUsers(fromExcl = None, maxResults = 100)
      order.verifyNoMoreInteractions()
      res0 shouldBe Right(())
      res1 shouldBe Right(UsersPage(Seq(user)))
      res2 shouldBe Right(UsersPage(Seq(user)))
    }
  }

  "cache entries expire after a set time" in {
    val delegate = spy(new InMemoryUserManagementStore())
    val tested = createTested(delegate)

    for {
      create1 <- tested.createUser(user, rights)
      get1 <- tested.getUserInfo(user.id)
      get2 <- tested.getUserInfo(user.id)
      get3 <- {
        Thread.sleep(2000); tested.getUserInfo(user.id)
      }
    } yield {
      val order = inOrder(delegate)
      order
        .verify(delegate, times(1))
        .createUser(any[User], any[Set[UserRight]])(any[LoggingContext])
      order.verify(delegate, times(2)).getUserInfo(any[Ref.UserId])(any[LoggingContext])
      order.verifyNoMoreInteractions()
      create1 shouldBe Right(())
      get1 shouldBe Right(userInfo)
      get2 shouldBe Right(userInfo)
      get3 shouldBe Right(userInfo)
    }
  }

  private def createTested(delegate: InMemoryUserManagementStore): CachedUserManagementStore = {
    new CachedUserManagementStore(
      delegate,
      expiryAfterWriteInSeconds = 1,
      maximumCacheSize = 10,
      new Metrics(new MetricRegistry),
    )
  }

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    f(createTested(new InMemoryUserManagementStore(createAdmin = false)))
  }
}
