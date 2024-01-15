// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PrivateKey
import java.security.cert.X509Certificate
import java.util.UUID

import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod.DeduplicationTime
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.google.protobuf.duration.Duration
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

import scala.annotation.nowarn
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

  private val LedgerId = UUID.randomUUID.toString
  private val WorkflowId = UUID.randomUUID.toString
  private val ApplicationId = UUID.randomUUID.toString
  private val Party = UUID.randomUUID.toString

  @nowarn("msg=deprecated")
  def generateCommand(
      payload: String = "hello, world",
      commandId: String = UUID.randomUUID.toString,
  ): SubmitRequest =
    SubmitRequest(
      commands = Some(
        Commands(
          ledgerId = LedgerId,
          workflowId = WorkflowId,
          applicationId = ApplicationId,
          commandId = commandId,
          party = Party,
          commands = Seq(
            Command(
              Command.Command.Create(
                CreateCommand(
                  templateId = Some(Identifier("Package", "Module", "Entity")),
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
          deduplicationPeriod = DeduplicationTime(Duration(seconds = 1.day.toSeconds)),
          minLedgerTimeRel = Some(Duration(seconds = 1.minute.toSeconds)),
        )
      )
    )

}
