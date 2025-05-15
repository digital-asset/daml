// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

import better.files.File
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.ConsoleMacros.utils

import scala.sys.process.Process

/** Test _some_ exemples from the release artifact. In particular the ones requiring external
  * scripts to be run on the participant.
  */
abstract class ReleaseExamplesIntegrationTest extends ReleaseArtifactIntegrationTestUtils {
  "The release examples" should {
    "successfully initialize participant with offline root namespace key" in { processLogger =>
      val offlineExampleDir = File(s"$cantonDir/examples/10-offline-root-namespace-init")
      val cantonBinRel = offlineExampleDir.relativize(File(cantonBin))
      Process(
        s"$cantonBinRel run --config external-init-example.conf bootstrap.canton",
        cwd = offlineExampleDir.toJava,
      ).!(processLogger) shouldBe 0
      checkOutput(
        processLogger,
        shouldContain = Seq(
          "participant initialization completed successfully"
        ),
      )
    }

    "successfully run the interactive topology example" in { processLogger =>
      val interactiveTopologyDir = File(s"$cantonDir/examples/08-interactive-submission")
      val cantonBinRel = interactiveTopologyDir.relativize(File(cantonBin))

      var cantonProcess: Option[Process] = None
      val portsFile = interactiveTopologyDir / "canton_ports.json"
      portsFile.deleteOnExit(swallowIOExceptions = true)

      try {
        cantonProcess = Some(
          Process(
            s"$cantonBinRel --no-tty --config interactive-submission.conf --bootstrap bootstrap.canton",
            cwd = interactiveTopologyDir.toJava,
          ).run(processLogger)
        )
        utils.retry_until_true(NonNegativeDuration.ofSeconds(40))(portsFile.exists)

        Process(
          "./interactive_topology_example.sh",
          cwd = interactiveTopologyDir.toJava,
        ).!(processLogger) shouldBe 0
      } finally {
        cantonProcess.foreach(_.destroy())
      }
    }
  }
}

class CommunityReleaseExamplesIntegrationTest
    extends ReleaseExamplesIntegrationTest
    with CommunityReleaseTest
