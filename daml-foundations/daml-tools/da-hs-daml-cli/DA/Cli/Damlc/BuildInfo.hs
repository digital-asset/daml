-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Cli.Damlc.BuildInfo
  ( buildInfo
  ) where

import qualified Text.PrettyPrint.ANSI.Leijen as PP
import Data.Monoid ((<>))
import SdkVersion

buildInfo :: PP.Doc
buildInfo = PP.vcat
    [ "Compiler Version: " <> PP.text componentVersion
    , "SDK Version: " <> PP.text sdkVersion
    ]
