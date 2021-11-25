// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.api.UserManagement
import com.daml.ledger.participant.state.index.v2.UserManagementService
import com.daml.lf.data.Ref

import scala.concurrent.Future

object UserManagementServiceStub extends UserManagementService {
  override def createUser(
      user: UserManagement.User,
      rights: Set[UserManagement.Right],
  ): Future[Boolean] =
    Future.successful(true)

  override def getUser(id: String): Future[UserManagement.User] =
    Future.successful(UserManagement.User(id, Ref.Party.assertFromString(s"party-for-$id")))

  override def deleteUser(id: String): Future[Unit] =
    Future.unit

  override def grantRights(
      id: String,
      rights: Set[UserManagement.Right],
  ): Future[Set[UserManagement.Right]] =
    Future.successful(rights)

  override def revokeRights(
      id: String,
      rights: Set[UserManagement.Right],
  ): Future[Set[UserManagement.Right]] =
    Future.successful(rights)

  override def listUserRights(id: String): Future[Set[UserManagement.Right]] =
    Future.successful(
      Set(
        UserManagement.Right.CanReadAs(Ref.Party.assertFromString("reader-party")),
        UserManagement.Right.CanActAs(Ref.Party.assertFromString("acting-party")),
        UserManagement.Right.ParticipantAdmin,
      )
    )
}
