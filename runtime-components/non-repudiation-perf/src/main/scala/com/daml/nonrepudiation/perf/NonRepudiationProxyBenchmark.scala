// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.perf

import java.util.UUID

import cats.effect.{ContextShift, IO}
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceBlockingStub
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.nonrepudiation.AlgorithmString
import com.daml.nonrepudiation.postgresql.Tables
import com.daml.nonrepudiation.resources.HikariTransactorResourceOwner
import com.daml.resources.Resource
import com.daml.resources.grpc.{GrpcResourceOwnerFactories => Resources}
import com.daml.testing.postgresql.{PostgresAround, PostgresDatabase}
import com.google.protobuf.duration.Duration
import doobie.util.log.LogHandler
import org.openjdk.jmh.annotations._
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

@State(Scope.Benchmark)
class NonRepudiationProxyBenchmark extends PostgresAround {

  import NonRepudiationProxyBenchmark._

  @Param(Array("100000"))
  var commandPayloadSize: Int = _

  @Param(Array("false"))
  var useNetworkStack: Boolean = _

  private var stubResource: Resource[ExecutionContext, CommandSubmissionServiceBlockingStub] = _
  private var stub: CommandSubmissionServiceBlockingStub = _
  private var database: PostgresDatabase = _
  private var payload: String = _

  @Benchmark
  def run(): Unit = {
    // Generating commands adds very little noise to substantial benchmarks
    val command = generateCommand(payload)
    val _ = stub.submit(command)
  }

  @Setup
  def setup(): Unit = {
    // The global fork-join work-stealing pool should be good enough to be used for both
    // handling resource callbacks and mock command submission calls
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    implicit val shift: ContextShift[IO] = IO.contextShift(executionContext)
    implicit val logHandler: LogHandler = Slf4jLogHandler(classOf[NonRepudiationProxyBenchmark])
    connectToPostgresqlServer()
    database = createNewRandomDatabase()

    val generator = new CertAndKeyGen(AlgorithmString.RSA, AlgorithmString.SHA256withRSA)
    generator.generate(2048)
    val key = generator.getPrivateKey
    val certificate = generator.getSelfCertificate(
      new X500Name("CN=Non-Repudiation Test,O=Digital Asset,L=Zurich,C=CH"),
      1.hour.toSeconds,
    )

    val stubOwner =
      for {
        transactor <- ownTransactor(database.url, maxPoolSize = 10)
        db = Tables.initialize(transactor)
        _ = db.certificates.put(certificate)
        stub <- StubOwner(
          useNetworkStack = useNetworkStack,
          key = key,
          certificate = certificate,
          certificates = db.certificates,
          signedPayloads = db.signedPayloads,
          serviceExecutionContext = executionContext,
        )
      } yield stub

    stubResource = stubOwner.acquire()
    stub = Await.result(stubResource.asFuture, atMost = 10.seconds)
    payload = "e" * commandPayloadSize
  }

  @TearDown
  def tearDown(): Unit = {
    Await.ready(stubResource.release(), atMost = 10.seconds)
    dropDatabase(database)
    disconnectFromPostgresqlServer()
  }

}

object NonRepudiationProxyBenchmark {

  private def ownTransactor(jdbcUrl: String, maxPoolSize: Int)(implicit cs: ContextShift[IO]) =
    HikariTransactorResourceOwner(Resources)(jdbcUrl, maxPoolSize)

  private val LedgerId = UUID.randomUUID.toString
  private val WorkflowId = UUID.randomUUID.toString
  private val ApplicationId = UUID.randomUUID.toString
  private val Party = UUID.randomUUID.toString

  private def generateCommand(payload: String): SubmitRequest =
    SubmitRequest(
      commands = Some(
        Commands(
          ledgerId = LedgerId,
          workflowId = WorkflowId,
          applicationId = ApplicationId,
          commandId = UUID.randomUUID.toString,
          party = Party,
          commands = Seq(
            Command(
              Command.Command.Create(
                CreateCommand(
                  templateId = None,
                  createArguments = Some(
                    Record(
                      recordId = None,
                      fields = Seq(
                        RecordField(
                          label = "field",
                          value = Some(Value(Value.Sum.Text(payload))),
                        )
                      ),
                    )
                  ),
                )
              )
            )
          ),
          deduplicationTime = Some(Duration(seconds = 1.day.toSeconds)),
          minLedgerTimeRel = Some(Duration(seconds = 1.minute.toSeconds)),
        )
      ),
      traceContext = None,
    )

}
