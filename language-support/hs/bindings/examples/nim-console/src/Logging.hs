-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module Logging (Mes,noMes,tagMes,colourMes,plainMes,colourWrap) where

import System.Console.ANSI(
    setSGRCode, Color(..), SGR(SetColor), ConsoleLayer(Foreground), ColorIntensity(Vivid),)

type Mes = String -> IO ()

noMes :: Mes
noMes _ = return ()

plainMes :: Mes
plainMes = putStrLn

colourWrap :: Color -> String -> String
colourWrap col s =
  setSGRCode [SetColor Foreground Vivid col] <> s <>
  setSGRCode [SetColor Foreground Vivid White]

colourMes :: Color -> Mes -> Mes
colourMes col mes s = mes (colourWrap col s)

tagMes :: String -> Mes -> Mes
tagMes tag mes s = mes (tag <> s)
