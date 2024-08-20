// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.admin.api.client.commands.{
  MediatorAdminCommands,
  ParticipantAdminCommands,
  SequencerAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.CommunityCantonStatus
import com.digitalasset.canton.environment.CommunityEnvironment
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.nowarn

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class CommunityHealthDumpGenerator(
    override val environment: CommunityEnvironment,
    override val grpcAdminCommandRunner: GrpcAdminCommandRunner,
) extends HealthDumpGenerator[CommunityCantonStatus] {
  override protected implicit val statusEncoder: Encoder[CommunityCantonStatus] = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*
    deriveEncoder[CommunityCantonStatus]
  }

  override def status(): CommunityCantonStatus =
    CommunityCantonStatus.getStatus(
      statusMap(
        environment.config.sequencersByString,
        SequencerAdminCommands.Health.SequencerStatusCommand(),
      ),
      statusMap(
        environment.config.mediatorsByString,
        MediatorAdminCommands.Health.MediatorStatusCommand(),
      ),
      statusMap(
        environment.config.participantsByString,
        ParticipantAdminCommands.Health.ParticipantStatusCommand(),
      ),
    )
}
