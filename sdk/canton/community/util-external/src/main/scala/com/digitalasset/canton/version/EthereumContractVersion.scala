// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.util.VersionUtil

/** This class represents a revision of the Sequencer.sol contract. */
final case class EthereumContractVersion(
    major: Int,
    minor: Int,
    patch: Int,
    optSuffix: Option[String] = None,
)

object EthereumContractVersion {

  def create(rawVersion: String): Either[String, EthereumContractVersion] =
    VersionUtil.create(rawVersion, EthereumContractVersion.getClass.getSimpleName).map {
      case (major, minor, patch, optSuffix) =>
        new EthereumContractVersion(major, minor, patch, optSuffix)
    }

  def tryCreate(rawVersion: String): EthereumContractVersion =
    create(rawVersion).fold(sys.error, identity)

  lazy val v1_0_0: EthereumContractVersion = EthereumContractVersion(1, 0, 0)
  lazy val v1_0_1: EthereumContractVersion = EthereumContractVersion(1, 0, 1)

  lazy val allKnownVersions = List(v1_0_0, v1_0_1)

  lazy val latest: EthereumContractVersion = v1_0_1

  lazy val versionInTests: EthereumContractVersion = latest
}
