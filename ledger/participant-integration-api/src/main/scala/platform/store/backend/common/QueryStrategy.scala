// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.interning.StringInterning

trait QueryStrategy {

  /** An expression resulting to a boolean, to check equality between two SQL expressions
    *
    * @return plain SQL which fits the query template
    */
  def columnEqualityBoolean(column: String, value: String): String =
    s"""$column = $value"""

  /** This populates the following part of the query:
    *   SELECT ... WHERE ... ORDER BY ... [THIS PART]
    *
    * @param limit optional limit
    * @return the composable SQL
    */
  def limitClause(limit: Option[Int]): CompositeSql =
    limit
      .map(to => cSQL"fetch next $to rows only")
      .getOrElse(cSQL"")

  /** An expression resulting to a boolean to check whether:
    *   - the party set defined by columnName and
    *   - the party set defined by parties
    * have at least one element in common (eg their intersection is non empty).
    *
    * @param columnName the SQL table definition which holds the set of parties
    * @param parties set of parties
    * @return the composable SQL
    */
  def arrayIntersectionNonEmptyClause(
      columnName: String,
      parties: Set[Ref.Party],
      stringInterning: StringInterning,
  ): CompositeSql

  /** Would be used in column selectors in GROUP BY situations to see whether a boolean column had true
    * Example: getting all groups and see wheter they have someone who had covid:
    *   SELECT group_name, booleanOrAggregationFunction(has_covid) GROUP BY group_name;
    *
    * @return the function name
    */
  def booleanOrAggregationFunction: String = "bool_or"

  /** Predicate which tests if the element referenced by the `elementColumnName`
    * is in the array from column `arrayColumnName`
    */
  def arrayContains(arrayColumnName: String, elementColumnName: String): String

  /** Boolean predicate */
  def isTrue(booleanColumnName: String): String
}
