-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.App.Nim.Logging (Logger,noLog,tagLog,colourLog,plainLog,colourWrap) where

import System.Console.ANSI(
    setSGRCode, Color(..), SGR(SetColor), ConsoleLayer(Foreground), ColorIntensity(Vivid),)

type Logger = String -> IO ()

noLog :: Logger
noLog _ = return ()

plainLog :: Logger
plainLog = putStrLn

colourWrap :: Color -> String -> String
colourWrap col s =
  setSGRCode [SetColor Foreground Vivid col] <> s <>
  setSGRCode [SetColor Foreground Vivid White]

colourLog :: Color -> Logger -> Logger
colourLog col log s = log (colourWrap col s)

tagLog :: String -> Logger -> Logger
tagLog tag log s = log (tag <> s)
