// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.javaapi
import com.digitalasset.canton.config.*
import com.digitalasset.canton.damltestsdev.java.da as DA

import scala.jdk.CollectionConverters.*

/** A collection of small utilities for tests that have no obvious home */
object TestUtils {

  def hasPersistence(cfg: StorageConfig): Boolean = cfg match {
    case _: StorageConfig.Memory => false
    case _: DbConfig.Postgres => true
    case config: DbConfig.H2 =>
      // Check whether we're using file-based or in-memory storage
      config.config.getString("url").contains("file")
    case otherConfig =>
      throw new IllegalArgumentException(
        s"Could not automatically determine whether test uses persistence from" +
          s" storage config $otherConfig. You probably need to update this logic."
      )
  }

  def damlSet[A](scalaSet: Set[A]): DA.set.types.Set[A] =
    new DA.set.types.Set(
      scalaSet.map((_, javaapi.data.Unit.getInstance())).toMap.asJava
    )
}
