-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- A little program to help prove the use of hlint with DAML.

{-# OPTIONS_GHC -Wno-missing-fields #-}

module Main (main) where

import "ghc-lib-parser" HsSyn
import "ghc-lib-parser" Config
import "ghc-lib-parser" DynFlags
import "ghc-lib-parser" Platform
import "ghc-lib-parser" StringBuffer
import "ghc-lib-parser" Fingerprint
import "ghc-lib-parser" Lexer
import "ghc-lib-parser" ErrUtils
import qualified "ghc-lib-parser" Parser
import "ghc-lib-parser" FastString
import "ghc-lib-parser" Outputable
import "ghc-lib-parser" SrcLoc
import "ghc-lib-parser" Panic
import "ghc-lib-parser" HscTypes
import "ghc-lib-parser" HeaderInfo
import "ghc-lib-parser" ApiAnnotation
import "ghc-lib-parser" GHC.LanguageExtensions.Type

import Control.Monad
import Control.Monad.Extra
import System.FilePath
import System.Environment
import System.IO.Extra
import qualified Data.Map as Map
import Data.List
import DA.Bazel.Runfiles

import Language.Haskell.HLint4

{- To test, create a file e.g. ~/Test.daml with contents
   ```
   daml 1.2
   module Main where

   main : IO ()
   main = print $ 3 + 4
   ```
   then, bazel run //compiler/hlint-testing:hlint-test ~/Test.daml.

   For the above module, we expect output like,
   ```
   [/Users/shaynefletcher/Test.daml:1:1: Ignore: Use module export list
    Found:
      module Main where
    Perhaps:
      module Main (
              module Main
          ) where
    Note: an explicit list is usally better
    ]
    ```
-}

-- Calculate the HLint data directory.
getHlintDataDir :: IO FilePath
getHlintDataDir = do
  locateRunfiles $ mainWorkspace </> "compiler/hlint-testing/data"

hlintSettings :: IO (ParseFlags, [Classify], Hint)
hlintSettings = do
  hlintDataDir <- getHlintDataDir
  putStrLn $ "Data dir is " ++ hlintDataDir
  (fixities, classify, hints) <-
    findSettings (readSettingsFile (Just hlintDataDir)) Nothing
  return (parseFlagsAddFixities fixities defaultParseFlags, classify, hints)

analyzeModule :: Located (HsModule GhcPs) -> ApiAnns -> IO ()
analyzeModule modu anns = do
  (_, classify, hint) <- hlintSettings
  print $ applyHints classify hint [createModuleEx anns modu]

main :: IO ()
main = do
  args <- getArgs
  case args of
    [file] -> do
      s <- readFile' file
      flags <-
        parsePragmasIntoDynFlags
          (defaultDynFlags fakeSettings fakeLlvmConfig) file s
      whenJust flags $ \flags ->
         case parse file (flags `gopt_set` Opt_KeepRawTokenStream)s of
            PFailed _ loc err ->
              putStrLn (showSDoc flags (pprLocErrMsg (mkPlainErrMsg flags loc err)))
            POk s m -> do
              let (wrns, errs) = getMessages s flags
              report flags wrns
              report flags errs
              when (null errs) $ analyzeModule m (harvestAnns s)
    _ -> fail "Exactly one file argument required"
  where
    report flags msgs =
      sequence_
        [ putStrLn $ showSDoc flags msg
        | msg <- pprErrMsgBagWithLoc msgs
        ]
    harvestAnns pst =
      ( Map.fromListWith (++) $ annotations pst
      , Map.fromList ((noSrcSpan, comment_q pst) : annotations_comments pst)
      )

parse :: String -> DynFlags -> String -> ParseResult (Located (HsModule GhcPs))
parse filename flags str =
  unP Parser.parseModule parseState
  where
    location = mkRealSrcLoc (mkFastString filename) 1 1
    buffer = stringToStringBuffer str
    parseState = mkPState flags buffer location

parsePragmasIntoDynFlags :: DynFlags -> FilePath -> String -> IO (Maybe DynFlags)
parsePragmasIntoDynFlags flags filepath str =
  catchErrors $ do
    let opts = getOptions flags (stringToStringBuffer str) filepath
    (flags, _, _) <- parseDynamicFilePragma flags opts
    return $ Just (foldl' xopt_set flags xExtensionsSet)
  where
    catchErrors :: IO (Maybe DynFlags) -> IO (Maybe DynFlags)
    catchErrors act = handleGhcException reportErr
                        (handleSourceError reportErr act)
    reportErr e = do putStrLn $ "error : " ++ show e; return Nothing

xExtensionsSet :: [Extension]
xExtensionsSet =
  [ RecordPuns, RecordWildCards, LambdaCase, TupleSections, BlockArguments, ViewPatterns
  , NumericUnderscores
  , DuplicateRecordFields, DisambiguateRecordFields
  , ScopedTypeVariables, ExplicitForAll
  , DataKinds, KindSignatures, RankNTypes, TypeApplications
  , ConstraintKinds
  , MultiParamTypeClasses, FlexibleInstances, GeneralizedNewtypeDeriving, TypeSynonymInstances
  , DefaultSignatures, StandaloneDeriving, FunctionalDependencies, DeriveFunctor
  , RebindableSyntax, OverloadedStrings
  , Strict, StrictData
  , MonadComprehensions
  , PackageImports
  , DamlSyntax
  ]

fakeSettings :: Settings
fakeSettings = Settings
  { sTargetPlatform=platform
  , sPlatformConstants=platformConstants
  , sProjectVersion=cProjectVersion
  , sProgramName="ghc"
  , sOpt_P_fingerprint=fingerprint0
  }
  where
    platform =
      Platform{platformWordSize=8
              , platformOS=OSUnknown
              , platformUnregisterised=True}
    platformConstants =
      PlatformConstants{pc_DYNAMIC_BY_DEFAULT=False,pc_WORD_SIZE=8}

fakeLlvmConfig :: (LlvmTargets, LlvmPasses)
fakeLlvmConfig = ([], [])
