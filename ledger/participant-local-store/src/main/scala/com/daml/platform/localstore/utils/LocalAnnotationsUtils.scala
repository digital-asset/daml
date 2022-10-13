// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.utils

object LocalAnnotationsUtils {

  def calculateUpdatedAnnotations(
      newValue: Map[String, String],
      existing: Map[String, String],
  ): Map[String, String] =
    existing.concat(newValue).view.filter { case (_, value) => value != "" }.toMap

}
