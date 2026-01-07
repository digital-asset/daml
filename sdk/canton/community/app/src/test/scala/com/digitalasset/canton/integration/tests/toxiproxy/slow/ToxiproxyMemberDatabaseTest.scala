// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToPostgres,
  ProxyConfig,
  SequencerToPostgres,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyDatabase
import org.scalatest.Ignore

/** Currently tests the participant-database connection under various network conditions. The
  * test-setup used is:
  *   - Two participants, participant 1 and participant2 connected to a single synchronizer da
  *   - Participant1 connects to its postgres database via a proxy controlled by toxiproxy
  *   - Participant2 connects to its postgres database directly
  */
// TODO(#16571): The test currently fails due to broken retry logic in DbStorageSingle and
//  too verbose and quick disconnect/passive switch in DbStorageMulti (even after a small hickup).
//  Need to make DbStorageMulti more lenient to db network failures, e.g. give the check some grace period
//  or only disconnect/shutdown if the error persists through at least 2 checks.
@Ignore
class ToxiproxyParticipantPostgres extends ToxiproxyDatabase {
  val postgres = new UsePostgres(loggerFactory)

  registerPlugin(postgres)
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiProxy)

  override def proxyConf: ParticipantToPostgres =
    ParticipantToPostgres("p1-to-postgres", defaultParticipant)
  override def component: String = "participant"

  override def connectionName: String = "participant - postgres connection"
}

/** Tests the response to sequencer-postgres connection failure.
  */
@Ignore // TODO(#16571): See comment above
class ToxiproxySequencerPostgres extends ToxiproxyDatabase {
  val postgres = new UsePostgres(loggerFactory)

  override def component: String = "sequencer"

  registerPlugin(postgres)
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiProxy)

  override def proxyConf: ProxyConfig =
    SequencerToPostgres("sequencer1-to-postgres", "sequencer1", dbTimeout = 5000L)
  override def connectionName: String = "sequencer - postgres connection"
}
