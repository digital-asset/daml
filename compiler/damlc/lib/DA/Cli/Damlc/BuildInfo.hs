-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Cli.Damlc.BuildInfo
  ( buildInfo
  ) where

import qualified Text.PrettyPrint.ANSI.Leijen as PP
import SdkVersion.Class (SdkVersioned, sdkVersion)

buildInfo :: SdkVersioned => PP.Doc
buildInfo = "SDK Version: " <> PP.text sdkVersion
