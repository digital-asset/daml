// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

case class TemplatePartiesFilter(relation: FilterRelation, wildcardParties: Set[Party]) {
  def allFilterParties: Set[Party] = relation.values.flatten.toSet ++ wildcardParties
}
