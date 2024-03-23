// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.util.jar.{Attributes, Manifest}

object DarManifestWriter {
  private val supportedFormat = "daml-lf"

  def encode(sdkVersion: String, dar: Dar[String]): Manifest = {
    val manifest = new Manifest()
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0")
    manifest.getMainAttributes().put(new Attributes.Name("Format"), supportedFormat)
    manifest
      .getMainAttributes()
      .put(new Attributes.Name("Dalfs"), dar.all.mkString(", "))
    manifest.getMainAttributes().put(new Attributes.Name("Main-Dalf"), dar.main)
    // We need an sdk version for damlc to accept the package.
    manifest.getMainAttributes().put(new Attributes.Name("Sdk-Version"), sdkVersion)
    manifest
  }
}
