// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import scala.collection.breakOut

abstract class LfVersions[V](protected val maxVersion: V, previousVersions: List[V])(
    protoValue: V => String) {

  val acceptedVersions: List[V] = previousVersions :+ maxVersion

  private val acceptedVersionsMap: Map[String, V] =
    acceptedVersions.map(v => (protoValue(v), v))(breakOut)

  def isAcceptedVersion(version: String): Option[V] = acceptedVersionsMap.get(version)

}
