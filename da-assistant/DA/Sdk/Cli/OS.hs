-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Cli.OS
    ( OS(..)
    , currentOS
    ) where

import           Data.Text   (Text, pack)
import           System.Info (os)

data OS
    = Linux
    | MacOS
    | Windows
    | Unknown Text

currentOS :: OS
currentOS = matchOS os
  where
    matchOS "darwin"    = MacOS
    matchOS "linux"     = Linux
    matchOS "mingw32"   = Windows
    matchOS designation = Unknown (pack designation)
