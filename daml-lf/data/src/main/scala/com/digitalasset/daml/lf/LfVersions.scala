// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scalaz.NonEmptyList

abstract class LfVersions[V](versionsAscending: NonEmptyList[V])(protoValue: V => String) {

  protected val maxVersion: V = versionsAscending.last

  private[lf] val acceptedVersions: List[V] = versionsAscending.list.toList

  private val acceptedVersionsMap: Map[String, V] =
    acceptedVersions.iterator.map(v => (protoValue(v), v)).toMap

  private[lf] def isAcceptedVersion(version: String): Option[V] = acceptedVersionsMap.get(version)

}
