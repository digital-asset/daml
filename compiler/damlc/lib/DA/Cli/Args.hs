-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This exposes a tweaked "Option.Applicative" parser with an undocumented lax
--   mode which discards unused flags
--   We use feature flags in the daml IDE and this stops the compiler from
--   falling over whenever it receives a flag it doesn't recognize.
module DA.Cli.Args (lax) where

import Options.Applicative hiding (customExecParser, execParser)
import Options.Applicative qualified as Op
import Options.Applicative.BashCompletion
import Options.Applicative.Common
import Options.Applicative.Internal

-- | Runs the argument parser in a normal way unless the first argument is "lax"
--   In this case it discards invalid flags and continues
lax :: Op.ParserInfo a -> [String] -> ([String], ParserResult a)
lax pinfo args =
  let (errMsgs, filteredArgs) =
        case args of
          "lax":xs -> removeUnknownFlags Op.defaultPrefs pinfo xs
          xs -> ([], xs)
   in (errMsgs, Op.execParserPure Op.defaultPrefs pinfo filteredArgs)

-- | If the parser fails on an unrecognized flags remove those flags
--   Worst case O(N^2)
removeUnknownFlags
  :: Op.ParserPrefs   -- ^ Global preferences for this parser
  -> Op.ParserInfo a  -- ^ Description of the program to run
  -> [String]         -- ^ Program arguments
  -> ([String], [String])
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
           -> ([String], [String])
noBadFlags f args = case f args of
    Left (UnexpectedError arg@('-':_) _) ->
      let errMsg = " [WARN] argument '"
                   <>arg<>
                   "' has been discarded due to the 'lax' command"
          (errs, valid) = noBadFlags f $ filter (arg /=) args
      in (errMsg:errs, valid)
    _ -> ([], args)
