// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.targets

sealed abstract class Target
final case class PostgreSQLTarget(
    connectUrl: String,
    user: String,
    password: String,
    outputFormat: String,
    schemaPerPackage: Boolean,
    mergeIdentical: Boolean,
    stripPrefix: Option[String]
) extends Target
final case object TextPrintTarget extends Target
final case class PrettyPrintTarget(width: Int, height: Int) extends Target
