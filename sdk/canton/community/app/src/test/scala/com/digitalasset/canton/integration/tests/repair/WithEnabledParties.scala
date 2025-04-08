// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.topology.PartyId

import java.util.concurrent.atomic.AtomicLong

private[repair] object WithEnabledParties {

  private val seq = new AtomicLong(Long.MinValue)

  private def suffix = f"${seq.getAndIncrement()}%08x"

  def apply[A](
      p: (LocalParticipantReference, Seq[String]),
      ps: (LocalParticipantReference, Seq[String])*
  )(test: PartialFunction[Seq[PartyId], A]): A = {

    val enabledParties =
      for {
        (participant, mnemonics) <- p +: ps
        mnemonic <- mnemonics
      } yield {
        participant.parties.enable(name = s"$mnemonic-${participant.name}-$suffix")
      }

    test
      .lift(enabledParties)
      .getOrElse(
        org.scalatest.Assertions.fail(
          "The function passed to `withEnabledParties` must be defined exactly for every enabled party"
        )
      )
  }

}
