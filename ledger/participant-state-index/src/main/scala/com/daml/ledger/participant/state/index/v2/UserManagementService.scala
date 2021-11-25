// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import scala.concurrent.Future

trait UserManagementService {
  import com.daml.ledger.api.UserManagement._

  def createUser(user: User, rights: Set[Right]): Future[Boolean]

  def getUser(id: String): Future[User]

  def deleteUser(id: String): Future[Unit]

  def grantRights(id: String, rights: Set[Right]): Future[Set[Right]]

  def revokeRights(id: String, rights: Set[Right]): Future[Set[Right]]

  def listUserRights(id: String): Future[Set[Right]]
}
