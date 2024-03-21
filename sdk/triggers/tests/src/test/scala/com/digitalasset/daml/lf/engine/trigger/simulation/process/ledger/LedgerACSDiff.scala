// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package ledger

import com.daml.lf.data.Ref.Identifier
import com.daml.lf.engine.trigger.simulation.process.report.ACSReporting

import scala.collection.concurrent.TrieMap

object LedgerACSDiff {
  def apply(
      triggerACSView: Map[String, Identifier],
      ledgerACSView: TrieMap[String, Identifier],
  ): ACSReporting.ACSDiff = {
    val common =
      ledgerACSView.toSet
        .intersect(triggerACSView.toSet)
        .groupBy(_._2)
    val additions = ledgerACSView.toSet
      .diff(triggerACSView.toSet)
      .groupBy(_._2)
    val deletions = triggerACSView.toSet
      .diff(ledgerACSView.toSet)
      .groupBy(_._2)
    val templates = triggerACSView.values.toSet ++ ledgerACSView.values.toSet
    val diff = templates.map { templateId =>
      (
        templateId,
        ACSReporting.ACSTemplateDiff(
          additions.getOrElse(templateId, Set.empty).size,
          deletions.getOrElse(templateId, Set.empty).size,
          common.getOrElse(templateId, Set.empty).size,
        ),
      )
    }

    ACSReporting.ACSDiff(diff.toMap)
  }
}
