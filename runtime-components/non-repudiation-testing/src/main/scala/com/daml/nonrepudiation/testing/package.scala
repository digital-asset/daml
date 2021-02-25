// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.util.UUID

import cats.effect.{ContextShift, IO}
import com.daml.doobie.logging.Slf4jLogHandler
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.nonrepudiation.postgresql.Tables
import com.daml.nonrepudiation.resources.HikariTransactorResourceOwner
import com.daml.resources.AbstractResourceOwner
import com.google.protobuf.duration.Duration
import doobie.util.log.LogHandler
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

package object testing {

  def generateKeyAndCertificate(): (PrivateKey, X509Certificate) = {
    val generator = new CertAndKeyGen(AlgorithmString.RSA, AlgorithmString.SHA256withRSA)
    generator.generate(2048)
    val key = generator.getPrivateKey
    val certificate = generator.getSelfCertificate(
      new X500Name("CN=Non-Repudiation Test,O=Digital Asset,L=Zurich,C=CH"),
      1.hour.toSeconds,
    )
    (key, certificate)
  }

  def initializeDatabase(
      jdbcUrl: String,
      maxPoolSize: Int,
  ): AbstractResourceOwner[ResourceContext, Tables] = {
    implicit val shift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
    implicit val logHandler: LogHandler = Slf4jLogHandler(getClass)
    HikariTransactorResourceOwner(ResourceOwner)(jdbcUrl, maxPoolSize).map(Tables.initialize)
  }

  private val LedgerId = UUID.randomUUID.toString
  private val WorkflowId = UUID.randomUUID.toString
  private val ApplicationId = UUID.randomUUID.toString
  private val Party = UUID.randomUUID.toString

  def generateCommand(payload: String): SubmitRequest =
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
