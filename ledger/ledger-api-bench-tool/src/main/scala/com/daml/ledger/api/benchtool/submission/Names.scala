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

  def observerPartyName(index: Int, uniqueParties: Boolean): String = {
    if (uniqueParties) s"Obs-$index-$identifierSuffix"
    else s"Obs-$index"
  }

  def observerPartyNames(numberOfObservers: Int, uniqueParties: Boolean): Seq[String] =
    (0 until numberOfObservers).map(i => observerPartyName(i, uniqueParties))

  def commandId(index: Int): String = s"command-$index-$identifierSuffix"

  def darId(index: Int) = s"submission-dars-$index-$identifierSuffix"

}
