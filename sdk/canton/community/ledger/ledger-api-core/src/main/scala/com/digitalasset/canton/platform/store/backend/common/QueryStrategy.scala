// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}

import java.sql.Connection

object QueryStrategy {

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

  /** Would be used in column selectors in GROUP BY situations to see whether a boolean column had true
    * Example: getting all groups and see wheter they have someone who had covid:
    * SELECT group_name, booleanOrAggregationFunction(has_covid) GROUP BY group_name;
    *
    * @return the function name
    */
  def booleanOrAggregationFunction: String = "bool_or"

  /** Select a singleton element from some column based on max value of another column
    *
    * @param singletonColumn column whose value should be returned when the orderingColumn hits max
    * @param orderingColumn  column used for sorting the input rows
    * @return an sql clause to be composed into the sql query
    */
  def lastByProxyAggregateFuction(singletonColumn: String, orderingColumn: String): String =
    s"(array_agg($singletonColumn ORDER BY $orderingColumn DESC))[1]"

  /** Constant boolean to be used in a SELECT clause */
  def constBooleanSelect(value: Boolean): String =
    if (value) "true" else "false"

  /** Constant boolean to be used in a WHERE clause */
  def constBooleanWhere(value: Boolean): String =
    if (value) "true" else "false"

  /** Expression for `(offset <= endInclusive)`
    *
    * The offset column must only contain valid offsets (no NULL, no Offset.beforeBegin)
    */
  def offsetIsSmallerOrEqual(nonNullableColumn: String, endInclusive: Offset): CompositeSql = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    // Note: special casing Offset.beforeBegin makes the resulting query simpler:
    if (endInclusive == Offset.beforeBegin) {
      cSQL"#${constBooleanWhere(false)}"
    } else {
      cSQL"#$nonNullableColumn <= $endInclusive"
    }
  }

  /** Expression for `(offset > startExclusive)`
    *
    * The offset column must only contain valid offsets (no NULL, no Offset.beforeBegin)
    */
  def offsetIsGreater(nonNullableColumn: String, startExclusive: Offset): CompositeSql = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    // Note: casing Offset.beforeBegin makes the resulting query simpler:
    if (startExclusive == Offset.beforeBegin) {
      cSQL"#${constBooleanWhere(true)}"
    } else {
      cSQL"#$nonNullableColumn > $startExclusive"
    }
  }

  /** Expression for `(startExclusive < offset <= endExclusive)`
    *
    * The offset column must only contain valid offsets (no NULL, no Offset.beforeBegin)
    */
  def offsetIsBetween(
      nonNullableColumn: String,
      startExclusive: Offset,
      endInclusive: Offset,
  ): CompositeSql = {
    import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement
    // Note: special casing Offset.beforeBegin make the resulting query simpler:
    if (endInclusive == Offset.beforeBegin) {
      cSQL"#${constBooleanWhere(false)}"
    } else if (startExclusive == Offset.beforeBegin) {
      cSQL"#$nonNullableColumn <= $endInclusive"
    } else {
      cSQL"(#$nonNullableColumn > $startExclusive and #$nonNullableColumn <= $endInclusive)"
    }
  }
}

trait QueryStrategy {

  /** Predicate which tests if the element referenced by the `elementColumnName`
    * is in the array from column `arrayColumnName`
    */
  def arrayContains(arrayColumnName: String, elementColumnName: String): String

  /** ANY SQL clause generation for a number of Long values
    */
  def anyOf(longs: Iterable[Long]): CompositeSql = {
    val longArray: Array[java.lang.Long] =
      longs.view.map(Long.box).toArray
    cSQL"= ANY($longArray)"
  }

  /** ANY SQL clause generation for a number of Long values
    */
  def anyOfStrings(strings: Iterable[String]): CompositeSql = {
    val stringArray: Array[String] =
      strings.toArray
    cSQL"= ANY($stringArray)"
  }

  def analyzeTable(tableName: String): CompositeSql

  def forceSynchronousCommitForCurrentTransactionForPostgreSQL(connection: Connection): Unit = ()
}
