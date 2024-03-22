// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.config

import com.daml.ledger.api.benchtool.util.SimpleFileReader

import java.io.File
import scala.util.{Failure, Success}

object ConfigMaker {

  def make(args: Array[String]): Either[ConfigurationError, Config] = {
    def parseCli: Either[ConfigurationError, Config] = Cli.config(args) match {
      case None => Left(ConfigurationError("Invalid CLI arguments."))
      case Some(config) =>
        Right(config)
    }

    def parseWorkflowConfig(workflowConfigFile: File): Either[ConfigurationError, WorkflowConfig] =
      SimpleFileReader.readFile(workflowConfigFile)(WorkflowConfigParser.parse) match {
        case Failure(ex) =>
          Left(ConfigurationError(s"Workflow config reading error: ${ex.getLocalizedMessage}"))
        case Success(result) =>
          result.left
            .map(parserError =>
              ConfigurationError(s"Workflow config parsing error: ${parserError.details}")
            )
      }

    for {
      config <- parseCli
      workflowConfig <- config.workflowConfigFile match {
        case None => Right(config.workflow)
        case Some(workflowConfigFile) => parseWorkflowConfig(workflowConfigFile)
      }
    } yield {
      // Workflow defined in the YAML file takes precedence over CLI params
      config.copy(workflow = workflowConfig)
    }
  }

  case class ConfigurationError(details: String)

}
