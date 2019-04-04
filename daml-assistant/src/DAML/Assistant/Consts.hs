-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DAML.Assistant.Consts
  ( damlConfigName
  , projectConfigName
  , sdkConfigName
  , damlPathEnvVar
  , projectPathEnvVar
  , sdkPathEnvVar
  , sdkVersionEnvVar
  , damlEnvVars
  ) where

import DAML.Project.Config

-- | File name of config file in DAML_HOME (~/.daml).
damlConfigName :: FilePath
damlConfigName = "config.yaml"

-- | File name of config file in DAML_PROJECT (the project path).
projectConfigName :: FilePath
projectConfigName = "da.yaml"

-- | File name of config file in DAML_SDK (the sdk path)
sdkConfigName :: FilePath
sdkConfigName = "config.yaml"
