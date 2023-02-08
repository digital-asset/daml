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
      padPartyIndexWithLeadingZeroes = true,
    )

  def partySetPartyName(prefix: String, numberOfParties: Int, uniqueParties: Boolean): Seq[String] =
    partyNames(
      prefix = prefix,
      numberOfParties = numberOfParties,
      uniqueParties = uniqueParties,
      // Padding the party names with leading zeroes makes it more convenient to construct requests based on a party prefix.
      // For example, if we have 1000 parties in a party set, we can use prefix 'Party-1' to match precisely the parties {Party-100, Party-101, .., Party-199}
      padPartyIndexWithLeadingZeroes = true,
    )

  def commandId(index: Int): String = s"command-$index-$identifierSuffix"

  def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

  private def partyNames(
      prefix: String,
      numberOfParties: Int,
      uniqueParties: Boolean,
      padPartyIndexWithLeadingZeroes: Boolean = false,
  ): Seq[String] = {
    val largestIndex = numberOfParties - 1
    val paddingTargetLength = largestIndex.toString.length
    def indexToString(i: Int): String =
      if (padPartyIndexWithLeadingZeroes) {
        padLeftWithZeroes(i, paddingTargetLength)
      } else {
        i.toString
      }
    (0 until numberOfParties).map(i => partyName(prefix, indexToString(i), uniqueParties))
  }

  private def padLeftWithZeroes(i: Int, len: Int): String = {
    val iText = i.toString
    "0" * (len - iText.length) + iText
  }

  private def partyName(baseName: String, index: String, uniqueParties: Boolean): String =
    s"$baseName$PartyPrefixSeparatorChar$index" + (if (uniqueParties) identifierSuffix else "")

}

object Names {
  protected val PartyPrefixSeparatorChar: Char = '-'
  val SignatoryPrefix = "signatory"
  val ObserverPrefix = "Obs"
  val DivulgeePrefix = "Div"
  val ExtraSubmitterPrefix = "Sub"

  /** @return main prefix of a party which is the prefix up to the first '-' character
    */
  def parsePartyNameMainPrefix(partyName: String): String = {
    partyName.split(Names.PartyPrefixSeparatorChar)(0)
  }

}
