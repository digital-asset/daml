-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Commands for managing SDK templates
module DA.Sdk.Cli.Template.Consts
    ( builtInAddonTemplateNames
    , builtInProjectTemplateNames

    , templateScriptsDirectory
    ) where

import           Data.Text
import           Filesystem.Path.CurrentOS

builtInAddonTemplateNames :: [Text]
builtInAddonTemplateNames = ["dazl-starter", "nanobot-starter",
                             "paas-user-guide", "app-arch-guide",
                             "daml-lf-archive-protos", "ledger-api-protos"]

builtInProjectTemplateNames :: [Text]
builtInProjectTemplateNames = [ "example-bond-trading"
                              , "example-collateral"
                              , "example-repo-market"
                              , "example-upgrade"
                              , "example-ping-pong-grpc-java"
                              , "example-ping-pong-reactive-components-java"
                              , "example-ping-pong-reactive-java"
                              , "getting-started"
                              , "quickstart-java"
                              , "tutorial-nodejs" ]

-- | The directory within templates that contains activation and test scripts and resources.
templateScriptsDirectory :: FilePath
templateScriptsDirectory = "_template"
