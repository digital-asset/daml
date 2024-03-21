// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

/** Collects identifiers used by the benchtool in a single place.
  */
class Names {

  val identifierSuffix = f"${System.nanoTime}%x"
  val benchtoolApplicationId = "benchtool"
  val benchtoolUserId: String = benchtoolApplicationId
  val workflowId = s"$benchtoolApplicationId-$identifierSuffix"
  val signatoryPartyName = s"signatory-$identifierSuffix"

  def observerPartyNames(numberOfObservers: Int, uniqueParties: Boolean): Seq[String] =
    partyNames("Obs", numberOfObservers, uniqueParties)

  def divulgeePartyNames(numberOfDivulgees: Int, uniqueParties: Boolean): Seq[String] =
    partyNames("Div", numberOfDivulgees, uniqueParties)

  def extraSubmitterPartyNames(numberOfExtraSubmitters: Int, uniqueParties: Boolean): Seq[String] =
    partyNames("Sub", numberOfExtraSubmitters, uniqueParties)

  def commandId(index: Int): String = s"command-$index-$identifierSuffix"

  def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

  private def partyNames(
      baseName: String,
      numberOfParties: Int,
      uniqueParties: Boolean,
  ): Seq[String] =
    (0 until numberOfParties).map(i => partyName(baseName, i, uniqueParties))

  private def partyName(baseName: String, index: Int, uniqueParties: Boolean): String =
    s"$baseName-$index" + (if (uniqueParties) identifierSuffix else "")

}
