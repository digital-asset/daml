// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Success, Failure}
import scalaz.\/-
import scalaz.syntax.traverse._
import spray.json._

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.iface.EnvironmentInterface
import com.digitalasset.daml.lf.iface.reader.InterfaceReader
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}

import com.digitalasset.daml.lf.engine.script._

class JsonTestRunner(
    val participantParams: Participants[ApiParameters],
    val dar: Dar[(PackageId, Package)],
    val token: String,
) {
  def genericTest[A](
      // test name
      name: String,
      // Identifier of the script value
      scriptId: Identifier,
      inputValue: Option[JsValue],
      assertResult: SValue => Either[String, Unit],
      expectedLog: Option[Seq[String]] = None
  ) = {

    LogCollector.clear()

    println(s"---\n$name:")

    implicit val system: ActorSystem = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val materializer: Materializer = Materializer(system)
    implicit val ec: ExecutionContext = system.dispatcher
    val applicationId = ApplicationId("Script Runner")

    val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
    val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
    val clientsF = Runner.jsonClients(participantParams, token, envIface)

    val testFlow: Future[Unit] = for {
      clients <- clientsF
      result <- Runner.run(dar, scriptId, inputValue, clients, applicationId, TimeProvider.UTC)
      _ <- expectedLog match {
        case None => Future.unit
        case Some(expectedLogs) =>
          val logMsgs = LogCollector.events.map(_.getMessage)
          if (expectedLogs.equals(logMsgs)) {
            Future.unit
          } else {
            Future.failed(new RuntimeException(s"Expected logs $expectedLogs but got $logMsgs"))
          }
      }
      _ <- assertResult(result) match {
        case Left(err) =>
          Future.failed(new RuntimeException(s"Assertion on script result failed: $err"))
        case Right(()) => Future.unit
      }
    } yield ()
    testFlow.onComplete({
      case Success(_) => {
        Http().shutdownAllConnectionPools().flatMap { case () => system.terminate() }
        println(s"Test $name succeeded")
      }
      case Failure(err) => {
        println(s"Test $name failed: $err")
        sys.exit(1)
      }
    })
    Await.result(testFlow, Duration.Inf)
  }
}
