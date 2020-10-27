// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.PureCompiledPackages
import com.daml.lf.archive.{Dar, DarReader, Decode}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast.Package
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.traverse._
import spray.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success}

object TestMain extends StrictLogging {

  // We run tests sequentially for now. While tests that
  // only access per-party state and access only state of freshly allocated parties
  // can in principal be run in parallel that runs into resource limits at some point
  // and doesn’t work for tests that access things like listKnownParties.
  // Once we have a mechanism to mark tests as exclusive and control the concurrency
  // limit we can think about running tests in parallel again.
  def sequentialTraverse[A, B](seq: Seq[A])(f: A => Future[B])(
      implicit ec: ExecutionContext): Future[Seq[B]] =
    seq.foldLeft(Future.successful(Seq.empty[B])) {
      case (acc, nxt) => acc.flatMap(bs => f(nxt).map(b => bs :+ b))
    }

  def main(args: Array[String]): Unit = {

    TestConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        val system: ActorSystem = ActorSystem("ScriptTest")
        implicit val sequencer: ExecutionSequencerFactory =
          new AkkaExecutionSequencerPool("ScriptTestPool")(system)
        implicit val materializer: Materializer = Materializer(system)
        implicit val executionContext: ExecutionContext = system.dispatcher
        implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

        val (participantParams, participantCleanup) = config.participantConfig match {
          case Some(file) =>
            val source = Source.fromFile(file)
            val fileContent = try {
              source.mkString
            } finally {
              source.close
            }
            val jsVal = fileContent.parseJson
            import ParticipantsJsonProtocol._
            (jsVal.convertTo[Participants[ApiParameters]], () => Future.unit)
          case None =>
            val (apiParameters, cleanup) = if (config.ledgerHost.isEmpty) {
              val timeProviderType = config.timeMode match {
                case ScriptTimeMode.Static => TimeProviderType.Static
                case ScriptTimeMode.WallClock => TimeProviderType.WallClock
              }
              val sandboxConfig = SandboxConfig.defaultConfig.copy(
                port = Port.Dynamic,
                timeProviderType = Some(timeProviderType),
              )
              val sandboxResource = SandboxServer.owner(sandboxConfig).acquire()
              val sandboxPort =
                Await.result(sandboxResource.asFuture.flatMap(_.portF).map(_.value), Duration.Inf)
              (ApiParameters("localhost", sandboxPort, None, None), () => sandboxResource.release())
            } else {
              (
                ApiParameters(config.ledgerHost.get, config.ledgerPort.get, None, None),
                () => Future.unit,
              )
            }
            (
              Participants(
                default_participant = Some(apiParameters),
                participants = Map.empty,
                party_participants = Map.empty),
              cleanup,
            )
        }

        val darMap = dar.all.toMap
        val compiledPackages = PureCompiledPackages(darMap).right.get
        val testScripts = dar.main._2.modules.flatMap {
          case (moduleName, module) =>
            module.definitions.collect(Function.unlift {
              case (name, _) =>
                val id = Identifier(dar.main._1, QualifiedName(moduleName, name))
                Script.fromIdentifier(compiledPackages, id) match {
                  // We exclude generated identifiers starting with `$`.
                  case Right(script: Script.Action) if !name.dottedName.startsWith("$") =>
                    Some((id, script))
                  case _ => None
                }
            })
        }

        val flow: Future[Boolean] = for {
          clients <- Runner.connect(
            participantParams,
            TlsConfiguration(false, None, None, None),
            config.maxInboundMessageSize,
          )
          _ <- clients.getParticipant(None) match {
            case Left(err) => throw new RuntimeException(err)
            case Right(client) =>
              client.grpcClient.packageManagementClient
                .uploadDarFile(ByteString.readFrom(new FileInputStream(config.darPath)))
          }
          success = new AtomicBoolean(true)
          _ <- sequentialTraverse(testScripts.toList) {
            case (id, script) =>
              val runner =
                new Runner(compiledPackages, script, config.timeMode)
              val testRun: Future[Unit] = runner.runWithClients(clients)._2.map(_ => ())
              // Print test result and remember failure.
              testRun.onComplete {
                case Failure(exception) =>
                  success.set(false)
                  println(s"${id.qualifiedName} FAILURE ($exception)")
                case Success(_) =>
                  println(s"${id.qualifiedName} SUCCESS")
              }
              // Do not abort in case of failure, but complete all test runs.
              testRun.recover {
                case _ => ()
              }
          }
        } yield success.get()

        flow.onComplete { _ =>
          Await.result(participantCleanup(), Duration.Inf)
          system.terminate()
        }

        if (!Await.result(flow, Duration.Inf)) {
          sys.exit(1)
        }
    }
  }
}
