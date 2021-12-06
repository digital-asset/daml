// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.UserManagement._
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.ledger.participant.state.index.v2.UserManagementService._

import scala.concurrent.Future
import scala.collection.mutable

class InMemoryUserManagementService extends UserManagementService {
  import InMemoryUserManagementService._

  override def createUser(user: User, rights: Set[UserRight]): Future[Result[Unit]] = Future.successful {
    putIfAbsent(UserInfo(user, rights)) match {
      case Some(_) => Left(UserExists(user.id))
      case None => Right(())
    }
  }

  override def getUser(id: String): Future[Result[User]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.user)
      case None => Left(UserNotFound(id))
    }
  }

  override def deleteUser(id: String): Future[Result[Unit]] = Future.successful {
    dropExisting(id) match {
      case Some(_) => Right(())
      case None => Left(UserNotFound(id))
    }
  }

  override def grantRights(id: String, granted: Set[UserRight]): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) =>
        val newlyGranted = granted.diff(userInfo.rights) // faster than filter
        // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
        assert(replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights ++ newlyGranted)))
        Right(newlyGranted)
      case None =>
        Left(UserNotFound(id))
    }
  }

  override def revokeRights(id: String, revoked: Set[UserRight]): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) =>
        val effectivelyRevoked = revoked.intersect(userInfo.rights) // faster than filter
        // we're not doing concurrent updates -- assert as backstop and a reminder to handle the collision case in the future
        assert(replaceInfo(userInfo, userInfo.copy(rights = userInfo.rights -- effectivelyRevoked)))
        Right(effectivelyRevoked)
      case None =>
        Left(UserNotFound(id))
    }
  }

  override def listUserRights(id: String): Future[Result[Set[UserRight]]] = Future.successful {
    lookup(id) match {
      case Some(userInfo) => Right(userInfo.rights)
      case None => Left(UserNotFound(id))
    }
  }

  /**
   * TODO: support pagination
   *
   * (From https://cloud.google.com/apis/design/design_patterns#list_pagination)
   *
   * To support pagination (returning list results in pages) in a List method, the API shall:
   *   - define a string field page_token in the List method's request message
   *     The client uses this field to request a specific page of the list results.
   *   - define an int32 field page_size in the List method's request message.
   *     Clients use this field to specify the maximum number of results to be returned by the server.
   *     The server may further constrain the maximum number of results returned in a single page.
   *     If the page_size is 0, the server will decide the number of results to be returned.
   *   - define a string field next_page_token in the List method's response message.
   *     This field represents the pagination token to retrieve the next page of results.
   *     If the value is "", it means no further results for the request.
   *     To retrieve the next page of results, client shall pass the value of response's
   *     next_page_token in the subsequent List method call (in the request message's page_token field):
   *
   *  Page token contents should be a url-safe base64 encoded protocol buffer.
   *  This allows the contents to evolve without compatibility issues.
   *  If the page token contains potentially sensitive information, that information should be encrypted.
   *
   *  When clients pass in query parameters in addition to a page token, the service must fail the request
   *  if the query parameters are not consistent with the page token.
   *
   *  Services must prevent tampering with page tokens from exposing unintended data
   *  through one of the following methods:
   *   - require query parameters to be respecified on follow up requests.
   *   - only reference server-side session state in the page token.
   *   - encrypt and sign the query parameters in the page token and revalidate and reauthorize these parameters on every call.
   */
  def listUsers(/*pageSize: Int, pageToken: String*/): Future[Result[Users]] = Future.successful {
    Right(state.values.map(_.user).toSeq)
  }

  // Underlying mutable map to keep track of UserInfo state.
  // Structured so we can use a ConcurrentHashMap (to more closely mimic a real implementation, where performance is key).
  // We synchronize on a private object (the mutable map), not the service (which could cause deadlocks).
  // (No need to mark state as volatile -- rely on synchronized to establish the JMM's happens-before relation.)
  private val state: mutable.Map[String, UserInfo] = mutable.Map(AdminUser.toStateEntry)
  private def lookup(id: String) = state.synchronized { state.get(id) }
  private def dropExisting(id: String) = state.synchronized { state.get(id).map(_ => state -= id) }
  private def putIfAbsent(info: UserInfo) = state.synchronized {
    val old = state.get(info.user.id)

    if (old.isEmpty) state.update(info.user.id, info)

    old
  }
  private def replaceInfo(oldInfo: UserInfo, newInfo: UserInfo) = state.synchronized {
    assert(oldInfo.user.id == newInfo.user.id, s"Replace info from if ${oldInfo.user.id} to ${newInfo.user.id} -> ${newInfo.rights}")
    state.get(oldInfo.user.id) match {
      case Some(`oldInfo`) => state.update(newInfo.user.id, newInfo); true
      case _ => false
    }
  }
}

object InMemoryUserManagementService {
  case class UserInfo(user: User, rights: Set[UserRight]) {
    def toStateEntry: (String, UserInfo) = user.id -> this
  }
  private val AdminUser = UserInfo(
    user = User("participant_admin", None),
    rights = Set(UserRight.ParticipantAdmin),
  )
}
