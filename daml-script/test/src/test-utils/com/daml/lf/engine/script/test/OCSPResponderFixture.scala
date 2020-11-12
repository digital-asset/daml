package com.daml.lf.engine.script.test

import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.Suite
import scala.concurrent.duration._

import scala.concurrent.Future

trait OCSPResponderFixture extends AkkaBeforeAndAfterAll { this: Suite =>

  private val RESPONDER_START_TIMEOUT: Duration = 5.seconds

  def ocspCommandPath: String
  def indexPath: String
  def caCertPath: String
  def ocspKeyPath: String
  def ocspCertPath: String
  def clientCertPath: String

  override def beforeAll(): Unit = {
    super.beforeAll()
    Future(scala.sys.process.Process(ocspServerCommand).run())(system.dispatcher)
    waitForResponderToStart()
    sendTestRequest()
  }

  private def waitForResponderToStart(): Unit =
    Thread.sleep(RESPONDER_START_TIMEOUT.toMillis)

  private def sendTestRequest(): Unit = {
    scala.sys.process.Process(testOCSPRequestCommand).!!
    ()
  }

  private def ocspServerCommand = List(
    ocspCommandPath,
    "ocsp",
    "-port",
    "2560",
    "-text",
    "-index", indexPath,
    "-CA", caCertPath,
    "-rkey", ocspKeyPath,
    "-rsigner", ocspCertPath
  )

  private def testOCSPRequestCommand = List(
    ocspCommandPath,
    "ocsp",
    "-CAfile",
    caCertPath,
    "-url",
    "http://127.0.0.1:2560",
    "-resp_text",
    "-issuer",
    caCertPath,
    "-cert",
    clientCertPath
  )

}
