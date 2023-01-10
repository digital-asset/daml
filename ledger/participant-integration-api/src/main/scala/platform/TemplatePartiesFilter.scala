// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

case class TemplatePartiesFilter(relation: FilterRelation, wildcardParties: Set[Party]) {
  val allFilterParties: Set[Party] = relation.values.flatten.toSet ++ wildcardParties
}
