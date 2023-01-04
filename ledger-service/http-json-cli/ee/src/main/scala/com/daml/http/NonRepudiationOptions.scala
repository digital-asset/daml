// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Paths

trait NonRepudiationOptions { this: scopt.OptionParser[JsonApiCli] =>

  opt[String]("non-repudiation-certificate-path")
    .action((path, config) =>
      config
        .copy(nonRepudiation = config.nonRepudiation.copy(certificateFile = Some(Paths.get(path))))
    )
    .text(NonRepudiationOptions.helpText)

  opt[String]("non-repudiation-private-key-path")
    .action((path, config) =>
      config
        .copy(nonRepudiation = config.nonRepudiation.copy(privateKeyFile = Some(Paths.get(path))))
    )
    .text(NonRepudiationOptions.helpText)

  opt[String]("non-repudiation-private-key-algorithm")
    .action((algorithm, config) =>
      config
        .copy(nonRepudiation = config.nonRepudiation.copy(privateKeyAlgorithm = Some(algorithm)))
    )
    .text(NonRepudiationOptions.helpText)

}

object NonRepudiationOptions {

  private val helpText: String =
    """EARLY ACCESS FEATURE
      |--non-repudiation-certificate-path, --non-repudiation-private-key-path and --non-repudiation-private-key-algorithm
      |must be passed together. All commands issued by the HTTP JSON API will be signed with the private key and the X.509
      |certificate at the provided paths. This is relevant exclusively if you are using the non-repudiation middleware.""".stripMargin

}
