// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.auth.client

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.engine.trigger.AuthServiceClient
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.ports.{LockedFreePort, Port}
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

object AuthServiceFixture {

  def withAuthServiceClient[A](testName: String)(testFn: AuthServiceClient => Future[A])(
      implicit system: ActorSystem,
      mat: Materializer,
      ec: ExecutionContext): Future[A] = {
    val adminLedgerId = LedgerId(s"admin-ledger-$testName")
    val adminLedgerF = for {
      ledger <- Future(
        new SandboxServer(
          sandbox.DefaultConfig.copy(
            port = Port.Dynamic,
            ledgerIdMode = LedgerIdMode.Static(adminLedgerId),
          ),
          mat))
      ledgerPort <- ledger.portF
    } yield (ledger, ledgerPort)

    val authServiceBinaryLoc: String = {
      val isWindows = sys.props("os.name").toLowerCase.contains("windows")
      val extension = if (isWindows) ".exe" else ""
      BazelRunfiles.rlocation("triggers/service/ref-ledger-authentication-binary" + extension)
    }

    val host = InetAddress.getLoopbackAddress

    val authServiceInstanceF: Future[(Process, Port)] = for {
      lockedPort <- Future(LockedFreePort.find())
      (_, ledgerPort) <- adminLedgerF
      ledgerUri = Uri.from(scheme = "http", host = host.getHostAddress, port = ledgerPort.value)
      process <- Future {
        Process(
          Seq(authServiceBinaryLoc),
          None,
          ("DABL_AUTHENTICATION_SERVICE_ADDRESS", host.getHostAddress),
          ("DABL_AUTHENTICATION_SERVICE_PORT", lockedPort.port.toString),
          ("DABL_AUTHENTICATION_SERVICE_LEDGER_URL", ledgerUri.toString),
          ("DABL_AUTHENTICATION_SERVICE_TEST_MODE", "true") // Needed for initial authorize call with basic credentials
        ).run()
      }
      // Wait for the auth service instance to be ready to accept connections.
      _ <- RetryStrategy.constant(attempts = 10, waitTime = 4.seconds)((_, _) =>
        Future(lockedPort.testAndUnlock(host)))
    } yield (process, lockedPort.port)

    val testF: Future[A] = for {
      (_, authServicePort) <- authServiceInstanceF
      authServiceBaseUri = Uri.from(
        scheme = "http",
        host = host.getHostAddress,
        port = authServicePort.value)
      authServiceClient = AuthServiceClient(authServiceBaseUri)
      result <- testFn(authServiceClient)
    } yield result

    testF.onComplete { _ =>
      authServiceInstanceF.foreach(_._1.destroy)
      adminLedgerF.foreach(_._1.close())
    }

    testF
  }

}
