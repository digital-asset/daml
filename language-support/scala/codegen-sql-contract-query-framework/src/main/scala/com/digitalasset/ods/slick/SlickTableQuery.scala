// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ods.slick

trait SlickTableQuery { self: SlickProfile =>
  def tableNameSuffix: String
  def odsId: String
  final def tableName: String = s"${odsId}_$tableNameSuffix"
  def schema: self.profile.SchemaDescription
}
