// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.test.dev.DevTests
import com.google.protobuf.ByteString
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

import scala.concurrent.{ExecutionContext, Future}

final class DevModeIT extends LedgerTestSuite {

  // We have to load the package for dev test on demand
  // as legitimate ledgers may reject loading such packages
  private[this] val packageResourcePath = "/ledger/test-common/dev-tests.dar"
  private[this] val pkgId = ApiTypes.TemplateId.unwrap(DevTests.TemplateWithMap.id).packageId

  private def loadPackage(ledger: ParticipantTestContext)(
      implicit ec: ExecutionContext): Future[Unit] = {
    val testPackage = Future {
      getClass.getResourceAsStream(packageResourcePath)
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete { bs =>
      testPackage.map(_.close())
      if (bs.toOption.forall(_.isEmpty))
        fail(s"Unable to load test package resource at $packageResourcePath")
    }
    bytes.flatMap(ledger.uploadDarFile)
  }

  private def loadPackageIfNecessary(ledger: ParticipantTestContext)(
      implicit ec: ExecutionContext): Future[Unit] =
    for {
      knowPackages <- ledger.listKnownPackages()
      _ <- if (knowPackages.exists(_.packageId == pkgId)) Future(()) else loadPackage(ledger)
    } yield ()

  test(
    "DevModeUpload",
    "Upload of dev package should succeed",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        _ <- loadPackage(ledger)
        knowPackages <- ledger.listKnownPackages()
      } yield
        if (!knowPackages.exists(_.packageId == pkgId))
          fail("fail to load the dev package")
  })

  test(
    "DevModeValue",
    "Create a template with Dev value",
    allocate(SingleParty),
  )(implicit ec => {
    case Participants(Participant(ledger, party)) =>
      for {
        _ <- loadPackageIfNecessary(ledger)
        _ <- ledger.create(
          party,
          new DevTests.TemplateWithMap(party, Primitive.GenMap(1L -> "a", 2L -> "b", 3L -> "c"))
        )
      } yield ()
  })

}
