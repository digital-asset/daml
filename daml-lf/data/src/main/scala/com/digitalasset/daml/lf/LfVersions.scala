// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

abstract class LfVersions[V](versionsAscending: List[V])(protoValue: V => String) {

  final protected val maxVersion: V = versionsAscending.last

  final private[lf] val acceptedVersions: List[V] = versionsAscending.toList

  private[this] val acceptedVersionsMap: Map[String, V] =
    acceptedVersions.iterator.map(v => (protoValue(v), v)).toMap

  private[lf] def isAcceptedVersion(version: String): Option[V] = acceptedVersionsMap.get(version)

  final protected def mkOrdering: Ordering[V] =
    scala.Ordering.by(versionsAscending.zipWithIndex.toMap)

}
