// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import sbt._

object Artifactory {

  val daResolvers: Seq[MavenRepository] = Seq(
    Resolver.bintrayRepo("digitalassetsdk", "DigitalAssetSDK"),
    Resolver.mavenLocal
  )
}
