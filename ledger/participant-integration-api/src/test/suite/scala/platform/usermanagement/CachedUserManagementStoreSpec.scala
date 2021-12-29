// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore.UserNotFound
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.data.Ref
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

// TODO participant user management: Maybe using a custom dummy (or a spy on InMemory version) rather than Mockito.mock would result in a simpler test
class CachedUserManagementStoreSpec
    extends AsyncFreeSpec
    with TestResourceContext
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private val user = User(
    id = Ref.UserId.assertFromString("user_id1"),
    primaryParty = Some(Ref.Party.assertFromString("primary_party1")),
  )
  private val emptyUserRights = Set.empty[UserRight]
  private val right1 = UserRight.CanActAs(Ref.Party.assertFromString("party_id1"))
  private val right2 = UserRight.ParticipantAdmin
  private val right3 = UserRight.CanActAs(Ref.Party.assertFromString("party_id2"))

  "cache users accesses with on write invalidation (createUser, getUser, deleteUser)" in {
    val delegate = mock[UserManagementStore]
    val tested = new CachedUserManagementStore(delegate, expiryAfterWriteInSeconds = 10)

    when(delegate.getUser(eqTo(user.id)))
      .thenReturn(
        Future(Left(UserNotFound(user.id))),
        Future(Right(user)),
        Future(Left(UserNotFound(user.id))),
      )

    when(delegate.createUser(eqTo(user), eqTo(emptyUserRights)))
      .thenReturn(Future.successful(Right(())))

    when(delegate.deleteUser(eqTo(user.id)))
      .thenReturn(Future.successful(Right(())))

    for {
      res0 <- tested.getUser(user.id)
      res1 <- tested.createUser(user, emptyUserRights)
      res2 <- tested.getUser(user.id)
      res3 <- tested.getUser(user.id)
      res4 <- tested.deleteUser(user.id)
      res5 <- tested.getUser(user.id)
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).getUser(eqTo(user.id))
      order.verify(delegate, times(1)).createUser(eqTo(user), eqTo(emptyUserRights))
      order.verify(delegate, times(1)).getUser(eqTo(user.id))
      order.verify(delegate, times(1)).deleteUser(eqTo(user.id))
      order.verify(delegate, times(1)).getUser(eqTo(user.id))
      order.verifyNoMoreInteractions()
      res0 shouldBe Left(UserNotFound(user.id))
      res1 shouldBe Right(())
      res2 shouldBe Right(user)
      res3 shouldBe Right(user)
      res4 shouldBe Right(())
      res5 shouldBe Left(UserNotFound(user.id))
    }
  }

  "listing all users should bypass cache (listUsers)" in {
    val delegate = mock[UserManagementStore]
    val tested = new CachedUserManagementStore(delegate, expiryAfterWriteInSeconds = 10)

    when(delegate.createUser(eqTo(user), eqTo(emptyUserRights)))
      .thenReturn(Future.successful(Right(())))

    when(delegate.listUsers())
      .thenReturn(Future.successful(Right(Seq.empty[User])))

    for {
      res0 <- tested.createUser(user, emptyUserRights)
      res1 <- tested.listUsers()
      res2 <- tested.listUsers()
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(eqTo(user), eqTo(emptyUserRights))
      order.verify(delegate, times(2)).listUsers()
      order.verifyNoMoreInteractions()
      res0 shouldBe Right(())
      res1 shouldBe Right(Seq.empty[User])
      res2 shouldBe Right(Seq.empty[User])
    }
  }

  "cache users rights accesses with on write invalidation (grantRights, revokeRights, listUserRights)" in {
    val delegate = mock[UserManagementStore]
    val tested = new CachedUserManagementStore(delegate, expiryAfterWriteInSeconds = 10)

    when(delegate.createUser(eqTo(user), eqTo(emptyUserRights)))
      .thenReturn(Future.successful(Right(())))

    when(delegate.listUserRights(eqTo(user.id)))
      .thenReturn(Future.successful(Right(Set.empty[UserRight])))

    when(delegate.getUser(eqTo(user.id)))
      .thenReturn(Future.successful(Left(UserNotFound(user.id))))

    when(delegate.deleteUser(eqTo(user.id)))
      .thenReturn(Future.successful(Right(())))

    when(delegate.grantRights(eqTo(user.id), eqTo(Set(right1, right2))))
      .thenReturn(Future.successful(Right(Set(right1, right2))))

    when(delegate.grantRights(eqTo(user.id), eqTo(Set(right2, right3))))
      .thenReturn(Future.successful(Right(Set(right3))))

    when(delegate.revokeRights(eqTo(user.id), eqTo(Set(right1, right3))))
      .thenReturn(Future.successful(Right(Set(right1, right3))))

    when(delegate.revokeRights(eqTo(user.id), eqTo(Set(right1, right2))))
      .thenReturn(Future.successful(Right(Set(right2))))

    for {
      res1 <- tested.createUser(user = user, rights = Set.empty[UserRight])
      list1 <- tested.listUserRights(id = user.id)
      list2 <- tested.listUserRights(id = user.id)
      grant1 <- tested.grantRights(id = user.id, rights = Set(right1, right2))
      list3 <- tested.listUserRights(id = user.id)
      grant2 <- tested.grantRights(id = user.id, rights = Set(right2, right3))
      list4 <- tested.listUserRights(id = user.id)
      revoke1 <- tested.revokeRights(id = user.id, rights = Set(right1, right3))
      list5 <- tested.listUserRights(id = user.id)
      revoke2 <- tested.revokeRights(id = user.id, rights = Set(right1, right2))
      list6 <- tested.listUserRights(id = user.id)

    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(eqTo(user), eqTo(emptyUserRights))
      order.verify(delegate, times(1)).listUserRights(eqTo(user.id))
      order.verify(delegate, times(2)).grantRights(any[Ref.UserId], any[Set[UserRight]])
      order.verify(delegate, times(2)).revokeRights(any[Ref.UserId], any[Set[UserRight]])
      order.verifyNoMoreInteractions()

      res1 shouldBe Right(())
      list1 shouldBe Right(Set.empty[UserRight])
      list2 shouldBe Right(Set.empty[UserRight])
      grant1 shouldBe Right(Set(right1, right2))
      list3 shouldBe Right(Set(right1, right2))
      grant2 shouldBe Right(Set(right3))
      list4 shouldBe Right(Set(right1, right2, right3))
      revoke1 shouldBe Right(Set(right1, right3))
      list5 shouldBe Right(Set(right2))
      revoke2 shouldBe Right(Set(right2))
      list6 shouldBe Right(Set.empty[UserRight])
    }
  }

  "cache entries expire after a set time" in {

    val delegate = spy(new InMemoryUserManagementStore())
    val tested = new CachedUserManagementStore(delegate, expiryAfterWriteInSeconds = 1)

    for {
      create1 <- tested.createUser(user, Set(right1))
      // Calls that populate the cache
      get1 <- tested.getUser(user.id)
      rights1 <- tested.listUserRights(user.id)
      // Calls that get values from cache
      get2 <- tested.getUser(user.id)
      rights2 <- tested.listUserRights(user.id)
      // Calls that find cache being empty
      // TODO participant user management: Check if the sleep time is appropriate
      get3 <- { Thread.sleep(2000); tested.getUser(user.id) }
      rights3 <- tested.listUserRights(user.id)
    } yield {
      val order = inOrder(delegate)
      order.verify(delegate, times(1)).createUser(any[User], any[Set[UserRight]])
      order.verify(delegate, times(1)).getUser(any[Ref.UserId])
      order.verify(delegate, times(1)).listUserRights(any[Ref.UserId])
      order.verify(delegate, times(1)).getUser(any[Ref.UserId])
      order.verify(delegate, times(1)).listUserRights(any[Ref.UserId])
      order.verifyNoMoreInteractions()
      create1 shouldBe Right(())
      get1 shouldBe Right(user)
      rights1 shouldBe Right(Set(right1))
      get2 shouldBe Right(user)
      rights2 shouldBe Right(Set(right1))
      get3 shouldBe Right(user)
      rights3 shouldBe Right(Set(right1))
    }
  }
}
