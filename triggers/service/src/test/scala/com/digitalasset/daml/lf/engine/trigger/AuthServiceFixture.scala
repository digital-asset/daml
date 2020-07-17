package com.daml.lf.engine.trigger

import java.net.{InetAddress, ServerSocket, Socket}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.domain.LedgerId
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process

object AuthServiceFixture {

  private def findFreePort(): Port = {
    val socket = new ServerSocket(Port(0).value)
    try {
      Port(socket.getLocalPort)
    } finally {
      socket.close()
    }
  }

  def withAuthServiceClient[A](testName: String)(testFn: AuthServiceClient => Future[A])(
      implicit system: ActorSystem,
      mat: Materializer,
      ec: ExecutionContext): Future[A] = {
    val adminLedgerId = LedgerId("admin-ledger")
    val adminLedgerF = for {
      ledger <- Future(
        new SandboxServer(
          SandboxConfig.default.copy(
            port = Port.Dynamic,
            timeProviderType = Some(TimeProviderType.Static),
            ledgerIdMode = LedgerIdMode.Static(adminLedgerId),
          ),
          mat))
      ledgerPort <- ledger.portF.map(_.value)
    } yield (ledger, ledgerPort)

    val authServiceBinaryLoc: String = {
      val isWindows = sys.props("os.name").toLowerCase.contains("windows")
      val extension = if (isWindows) ".exe" else ""
      BazelRunfiles.rlocation("triggers/service/ref-ledger-authentication-binary" + extension)
    }

    val host = InetAddress.getLoopbackAddress

    val authServiceInstanceF: Future[(Process, Port)] = for {
      port <- Future { findFreePort() }
      (_, ledgerPort) <- adminLedgerF
      ledgerUrl = "http://" + host.getHostAddress + ":" + ledgerPort.toString
      process <- Future {
        Process(
          Seq(authServiceBinaryLoc),
          None,
          ("DABL_AUTHENTICATION_SERVICE_ADDRESS", host.getHostAddress),
          ("DABL_AUTHENTICATION_SERVICE_PORT", port.toString),
          ("DABL_AUTHENTICATION_SERVICE_LEDGER_URL", ledgerUrl)
        ).run()
      }
    } yield (process, port)

    // Wait for the auth service instance to be ready to accept connections.
    RetryStrategy.constant(attempts = 3, waitTime = 2.seconds) { (_, _) =>
      for {
        (_, port) <- authServiceInstanceF
        channel <- Future(new Socket(host, port.value))
      } yield channel.close()
    }

    val testF: Future[A] = for {
      (_, authServicePort) <- authServiceInstanceF
      authServiceClient = AuthServiceClient(host, authServicePort)
      result <- testFn(authServiceClient)
    } yield result

    testF.onComplete { _ =>
      authServiceInstanceF.foreach(_._1.destroy)
      adminLedgerF.foreach(_._1.close())
    }

    testF
  }

}
