// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

object NodeToCommand {

  /** a node incoming without use of internal nodeIds */
  type TranslatableNode = GenNode.WithTxValue[_, AbsoluteContractId]

  import Error._

  /**
    * Translates a single node to a command for reinterpretation
    *  the caller is expected to provide
    *
    *  @param node a node with any nodeId type that only contains AbsoluteContractIds,
    *    so that node ids are not referred internally
    *  @param ledgerEffectiveTime instant of the claimed submission
    *  @param workflowReference string identifier for errors reported to the caller and logging
    *  @return a commands structure that given can be submitted for (re)interpretation
    */
  def apply(
      node: TranslatableNode,
      ledgerEffectiveTime: Time.Timestamp,
      workflowReference: String): Either[Error, Commands] = {
    val cmd: Either[Error, Command] = node match {
      case _: NodeFetch[_] =>
        Left(Error(s"Fetch node cannot be translated to command in: $workflowReference"))
      case _: NodeLookupByKey[_, _] =>
        Left(Error(s"LookupByKey node cannot be translated to a command in: $workflowReference"))
      case c: NodeCreate.WithTxValue[AbsoluteContractId] =>
        val templateId = c.coinst.template
        val value = c.coinst.arg

        Right(CreateCommand(templateId, value))

      case e: NodeExercises.WithTxValue[_, AbsoluteContractId] =>
        val templateId = e.templateId
        val contractId = e.targetCoid.coid
        val argument = e.chosenValue
        // we take the first acting party to be the submitter,
        // this assumption needs to be validated
        // if the commands reinterpretation yields the same acting parties
        // than it can be accepted by any of them
        e.actingParties.headOption
          .errorIfEmpty(Error(
            s"Exercise node cannot be translated, no acting party in exercise node ($templateId, $contractId, ${e.choiceId}) ; in: $workflowReference"))
          .map(submitter =>
            ExerciseCommand(templateId, contractId, e.choiceId, submitter, argument))
    }

    cmd.map(p => Commands(ImmArray(p), ledgerEffectiveTime, workflowReference))
  }
}
