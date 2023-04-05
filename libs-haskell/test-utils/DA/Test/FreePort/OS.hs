-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf #-}

module DA.Test.FreePort.OS (OS (..), os) where

import System.Info.Extra (isWindows, isMac)

data OS
  = Windows
  | Linux
  | MacOS

-- TODO: Linux being a catchall is risky, consider alternate check
os :: OS
os = if
  | isWindows -> Windows
  | isMac -> MacOS
  | otherwise -> Linux

