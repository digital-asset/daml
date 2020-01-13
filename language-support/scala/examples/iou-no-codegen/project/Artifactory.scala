// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import sbt._

object Artifactory {

  val daResolvers: Seq[MavenRepository] = Seq(
    Resolver.bintrayRepo("digitalassetsdk", "DigitalAssetSDK"),
    Resolver.mavenLocal
  )
}
