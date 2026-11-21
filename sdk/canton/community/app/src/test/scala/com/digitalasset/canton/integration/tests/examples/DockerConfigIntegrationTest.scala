// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.`ExampleIntegrationTest`.dockerImagesPath

sealed abstract class DockerConfigIntegrationTest
    extends ExampleIntegrationTest(
      dockerImagesPath / "canton-base" / "storage.conf",
      dockerImagesPath / "canton-mediator" / "app.conf",
      dockerImagesPath / "canton-sequencer" / "app.conf",
      dockerImagesPath / "canton-participant" / "app.conf",
      dockerImagesPath / "integration-tests" / "overrides.conf",
    )
    with CommunityIntegrationTest {
  "run docker synchronizer bootstrap successfully" in { env =>
    import env.*
    runScript(dockerImagesPath / "integration-tests" / "integration-bootstrap.sc")(environment)

  }
}

final class DockerConfigIntegrationTestPostgres extends DockerConfigIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
