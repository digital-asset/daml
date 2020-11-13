// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.testing.utils.OwnedResource
import com.daml.ledger.api.testing.utils.{Resource => TestResource}
import com.daml.ledger.resources.ResourceContext
import com.daml.ledger.resources.ResourceOwner
import com.daml.ledger.resources.Resource
import com.daml.timer.RetryStrategy
import org.scalatest.Suite

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.sys.process._

trait OCSPResponderFixture extends AkkaBeforeAndAfterAll { this: Suite =>

  private val ec: ExecutionContext = system.dispatcher

  private val RESPONDER_HOST: String = "127.0.0.1"
  private val RESPONDER_PORT: Int = 2560

  def indexPath: String
  def caCertPath: String
  def ocspKeyPath: String
  def ocspCertPath: String
  def clientCertPath: String

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    responderResource.setup()
  }

  override protected def afterAll(): Unit = {
    responderResource.close()
    super.afterAll()
  }

  private val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("windows")

  private val opensslExecutable: String =
    if (!isWindows) BazelRunfiles.rlocation("external/openssl_dev_env/bin/openssl")
    else BazelRunfiles.rlocation("external/openssl_dev_env/usr/bin/openssl.exe")

  lazy val responderResource: TestResource[Any] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    new OwnedResource[ResourceContext, Process](
      owner = responderResourceOwner,
      acquisitionTimeout = 10.seconds,
      releaseTimeout = 5.seconds
    )
  }

  private def responderResourceOwner: ResourceOwner[Process] =
    new ResourceOwner[Process] {
      
      override def acquire()(implicit context: ResourceContext): Resource[Process] = {
        def start(): Future[Process] =
          for {
            process <- startResponderProcess()
            _ <- verifyResponderReady()
          } yield process

        def stop(responderProcess: Process): Future[Unit] =
          Future {
            responderProcess.destroy()
          }.map(_ => ())

        Resource(start())(stop)
      }
    }

  private def startResponderProcess()(implicit ec: ExecutionContext): Future[Process] =
    Future(Process(ocspServerCommand).run())

  private def verifyResponderReady()(implicit ec: ExecutionContext): Future[String] =
    RetryStrategy.constant(attempts = 3, waitTime = 5.seconds) { (_, _) =>
      Future(Process(testOCSPRequestCommand).!!)
    }

  private def ocspServerCommand = List(
    opensslExecutable,
    "ocsp",
    "-port",
    RESPONDER_PORT.toString,
    "-text",
    "-index",
    indexPath,
    "-CA",
    caCertPath,
    "-rkey",
    ocspKeyPath,
    "-rsigner",
    ocspCertPath
  )

  private def testOCSPRequestCommand = List(
    opensslExecutable,
    "ocsp",
    "-CAfile",
    caCertPath,
    "-url",
    s"http://$RESPONDER_HOST:$RESPONDER_PORT",
    "-resp_text",
    "-issuer",
    caCertPath,
    "-cert",
    clientCertPath
  )

}
