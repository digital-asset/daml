// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

// TODO (i12057): complete unit testing and move to the right place sitting side-by-side with the implementation

//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
//import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpec

//import scala.concurrent.duration._

final class InMemoryUserManagementStoreSpec
    extends AnyWordSpec
    with Matchers
//    with Eventually
//    with BeforeAndAfterAll
    {
//  override implicit val patienceConfig: PatienceConfig =
//    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(50, Millis)))

  // tests for
  //   deleteUser
  //   getUser
  //   createUser
  "in-memory user management should" should {
    "allow creating a fresh user" in {}

    "disallow re-creating an existing user" in {}

    "find a freshly created user" in {}

    "not find a non-existent user" in {}

  }

  // tests for:
  //    listUserRights
  //    revokeRights
  //    grantRights
  "in-memory user rights management should" should {}

//  override def afterAll(): Unit = {
//  }
}
