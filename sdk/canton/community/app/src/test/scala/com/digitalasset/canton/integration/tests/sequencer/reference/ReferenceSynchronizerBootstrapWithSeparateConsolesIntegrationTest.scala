// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.sequencer.SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest

class ReferenceSynchronizerBootstrapWithSeparateConsolesIntegrationTest
    extends SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
