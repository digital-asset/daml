-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This exposes a tweaked "Option.Applicative" parser with an undocumented lax
--   mode which discards unused flags
--   We use feature flags in the daml IDE and this stops the compiler from
--   falling over whenever it receives a flag it doesn't recognize.
module DA.Cli.Args (execParserLax) where

import qualified Options.Applicative as Op
import Options.Applicative hiding (customExecParser, execParser)
import Options.Applicative.Internal
import Options.Applicative.BashCompletion
import Options.Applicative.Common
import System.Environment
import System.IO

-- | Runs the argument parser in a normal way unless the first argument is "lax"
--   In this case it discards invalid flags and continues
execParserLax :: Op.ParserInfo a -> IO a
execParserLax = customExecParser Op.defaultPrefs

customExecParser :: Op.ParserPrefs -> Op.ParserInfo a -> IO a
customExecParser pprefs pinfo = do
  args <- getArgs
  filteredArgs <-
        case args of
          "lax":xs -> removeUnknownFlags pprefs pinfo xs
          xs -> pure xs
  Op.handleParseResult $ Op.execParserPure pprefs pinfo filteredArgs

-- | If the parser fails on an unrecognized flags remove those flags
--   Worst case O(N^2)
removeUnknownFlags
  :: Op.ParserPrefs   -- ^ Global preferences for this parser
  -> Op.ParserInfo a  -- ^ Description of the program to run
  -> [String]         -- ^ Program arguments
  -> IO [String]
removeUnknownFlags pprefs pinfo args =
  noBadFlags wrk args
  where
    pinfo' = pinfo
      { Op.infoParser = (Left <$> bashCompletionParser pinfo pprefs)
                    <|> (Right <$> infoParser pinfo) }
    wrk as =
      fst $ runP (runParserInfo pinfo' as) pprefs

noBadFlags :: ([String] -> Either ParseError a)
           -> [String]
           -> IO [String]
noBadFlags f args = case f args of
    Left (UnexpectedError arg@('-':_) _) -> do
      hPutStrLn stderr $
        " [WARN] argument '"<>arg<>"' has been discarded due to the 'lax' command"
      noBadFlags f $ filter (arg /=) args
    _ -> pure args
