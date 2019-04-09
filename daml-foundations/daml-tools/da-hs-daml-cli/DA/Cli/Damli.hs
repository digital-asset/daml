-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Main entry-point of DAML-interpreter.
module DA.Cli.Damli
  ( run
  ) where


import Control.Monad.Except
import DA.Cli.Damli.Base
import DA.Cli.Damli.BuildInfo
import DA.Cli.Damli.Command.LF
import Options.Applicative
import qualified Text.PrettyPrint.ANSI.Leijen         as PP

options :: Parser Command
options =
    flag' (PP.putDoc buildInfo) (short 'v' <> long "version" <> help "Show version information")
    <|>
    subparser cmdRoundtripLF1

parserInfo :: ParserInfo Command
parserInfo =
  info (helper <*> options)
    (  fullDesc
    <> progDesc "Invoke the DAML interpreter. Use -h for help."
    <> headerDoc (Just buildInfo)
    )

run :: IO ()
run = do
    join $ execParser parserInfo
