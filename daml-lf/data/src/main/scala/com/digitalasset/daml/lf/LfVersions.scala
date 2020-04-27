// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scalaz.NonEmptyList

import scala.collection.breakOut

abstract class LfVersions[V](versionsAscending: NonEmptyList[V])(protoValue: V => String) {

  protected val maxVersion: V = versionsAscending.last

  val acceptedVersions: List[V] = versionsAscending.list.toList

  private val acceptedVersionsMap: Map[String, V] =
    acceptedVersions.map(v => (protoValue(v), v))(breakOut)

  def isAcceptedVersion(version: String): Option[V] = acceptedVersionsMap.get(version)

}
