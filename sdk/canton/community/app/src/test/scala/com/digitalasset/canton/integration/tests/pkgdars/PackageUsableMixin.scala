// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pkgdars

import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.damltests.java.conflicttest.Many
import com.digitalasset.canton.integration.BaseIntegrationTest
import com.digitalasset.canton.topology.SynchronizerId
import org.scalatest.Assertion

import scala.jdk.CollectionConverters.*

trait PackageUsableMixin {
  this: BaseIntegrationTest =>

  protected def submitCommand(
      submittingParticipant: ParticipantReference,
      observerParticipant: ParticipantReference,
      synchronizerId: SynchronizerId,
  ): Unit = {
    val submitter = submittingParticipant.id.adminParty
    val observer = observerParticipant.id.adminParty
    val cmd = new Many(
      submitter.toProtoPrimitive,
      List(observer.toProtoPrimitive).asJava,
    ).create.commands.asScala.toSeq

    submittingParticipant.ledger_api.javaapi.commands.submit(
      Seq(submitter),
      cmd,
      Some(synchronizerId),
    )
  }

  protected def assertPackageUsable(
      submittingParticipant: ParticipantReference,
      observerParticipant: ParticipantReference,
      synchronizerId: SynchronizerId,
  ): Assertion =
    eventually() {
      withClue("Submit a command referencing the main package") {
        noException shouldBe thrownBy {
          submitCommand(submittingParticipant, observerParticipant, synchronizerId)
        }
      }
    }
  protected def archiveContract(submittingParticipant: ParticipantReference): Unit = {
    val contracts = submittingParticipant.ledger_api.javaapi.state.acs
      .filter(Many.COMPANION)(submittingParticipant.id.adminParty)
    val cmds = contracts.map(_.id.exerciseArchive().commands.loneElement)
    submittingParticipant.ledger_api.javaapi.commands
      .submit(Seq(submittingParticipant.id.adminParty), cmds)
  }

}
