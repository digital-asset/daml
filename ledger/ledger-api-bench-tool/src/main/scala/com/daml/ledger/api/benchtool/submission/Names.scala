// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

/** Collects identifiers used by the benchtool in a single place.
  */
class Names {

  import Names.{
    SignatoryPrefix,
    PartyPrefixSeparatorChar,
    ObserverPrefix,
    DivulgeePrefix,
    ExtraSubmitterPrefix,
  }

  val identifierSuffix = f"${System.nanoTime}%x"
  val benchtoolApplicationId = "benchtool"
  val benchtoolUserId: String = benchtoolApplicationId
  val workflowId = s"$benchtoolApplicationId-$identifierSuffix"
  val signatoryPartyName = s"$SignatoryPrefix$PartyPrefixSeparatorChar$identifierSuffix"

  def observerPartyNames(numberOfObservers: Int, uniqueParties: Boolean): Seq[String] =
    partyNames(ObserverPrefix, numberOfObservers, uniqueParties)

  def divulgeePartyNames(numberOfDivulgees: Int, uniqueParties: Boolean): Seq[String] =
    partyNames(DivulgeePrefix, numberOfDivulgees, uniqueParties)

  def extraSubmitterPartyNames(numberOfExtraSubmitters: Int, uniqueParties: Boolean): Seq[String] =
    partyNames(
      ExtraSubmitterPrefix,
      numberOfExtraSubmitters,
      uniqueParties,
      padPartyIndexWithZeroes = true,
    )

  def commandId(index: Int): String = s"command-$index-$identifierSuffix"

  def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

  def partyNames(
      prefix: String,
      numberOfParties: Int,
      uniqueParties: Boolean,
      padPartyIndexWithZeroes: Boolean = false,
  ): Seq[String] = {
    numberOfParties.toString.length
    (0 until numberOfParties).map(i => partyName(prefix, i, uniqueParties))
  }

  private def partyName(baseName: String, index: Int, uniqueParties: Boolean): String =
    s"$baseName$PartyPrefixSeparatorChar$index" + (if (uniqueParties) identifierSuffix else "")

}

object Names {
  protected val PartyPrefixSeparatorChar: Char = '-'
  val SignatoryPrefix = "signatory"
  val ObserverPrefix = "Obs"
  val DivulgeePrefix = "Div"
  val ExtraSubmitterPrefix = "Sub"

  def parsePartyNamePrefix(partyName: String): String = {
    partyName.split(Names.PartyPrefixSeparatorChar)(0)
  }

}
