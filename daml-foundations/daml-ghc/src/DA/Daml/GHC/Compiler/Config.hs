-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Set up the GHC monad in a way that works for us
module DA.Daml.GHC.Compiler.Config(setupDamlGHC) where

import Development.IDE.UtilGHC
import qualified CmdLineParser as Cmd (warnMsg)
import           DynFlags (parseDynamicFilePragma)
import           GHC                         hiding (convertLit)
import           GHC.LanguageExtensions.Type
import           GhcMonad
import           GhcPlugins                  as GHC hiding (fst3, (<>))
import           Panic (throwGhcExceptionIO)

import           Control.Monad
import           Data.List

----------------------------------------------------------------------
-- GHC setup

-- | Language options enabled in the DAML-1.2 compilation
xExtensionsSet :: [Extension]
xExtensionsSet =
  [ -- syntactic convenience
    RecordPuns, RecordWildCards, LambdaCase, TupleSections, BlockArguments, ViewPatterns,
    NumericUnderscores
    -- records
  , DuplicateRecordFields, DisambiguateRecordFields
    -- types and kinds
  , ScopedTypeVariables, ExplicitForAll
  , DataKinds, KindSignatures, RankNTypes, TypeApplications
  , ConstraintKinds
    -- type classes
  , MultiParamTypeClasses, FlexibleInstances, GeneralizedNewtypeDeriving, TypeSynonymInstances
  , DefaultSignatures, StandaloneDeriving, FunctionalDependencies, DeriveFunctor
    -- replacing primitives
  , RebindableSyntax, OverloadedStrings
    -- strictness
  , Strict, StrictData
    -- avoiding letrec in list comp (see DEL-3841)
  , MonadComprehensions
    -- package imports
  , PackageImports
    -- our changes
  , NewColonConvention
  , DamlVersionRequired
  , WithRecordSyntax
  , DamlTemplate
  , ImportQualifiedPost
  ]


-- | Language settings _disabled_ ($-XNo...$) in the DAML-1.2 compilation
xExtensionsUnset :: [Extension]
xExtensionsUnset = [ ]

-- | Flags set for DAML-1.2 compilation
xFlagsSet :: [ GeneralFlag ]
xFlagsSet = [
   Opt_Haddock
 , Opt_Ticky
 ]

-- | Warning options set for DAML compilation. Note that these can be modified
--   (per file) by the user via file headers '{-# OPTIONS -fwarn-... #-} and
--   '{-# OPTIONS -no-warn-... #-}'.
wOptsSet :: [ WarningFlag ]
wOptsSet =
  [ Opt_WarnUnusedImports
  , Opt_WarnPrepositiveQualifiedModule
  , Opt_WarnOverlappingPatterns
  , Opt_WarnIncompletePatterns
  ]

-- | Warning options set for DAML compilation, which become errors.
wOptsSetFatal :: [ WarningFlag ]
wOptsSetFatal =
  [ Opt_WarnMissingFields
  ]

-- | Warning options unset for DAML compilation. Note that these can be modified
--   (per file) by the user via file headers '{-# OPTIONS -fwarn-... #-} and
--   '{-# OPTIONS -no-warn-... #-}'.
wOptsUnset :: [ WarningFlag ]
wOptsUnset =
  [ Opt_WarnMissingMonadFailInstances -- failable pattern plus RebindableSyntax raises this error
  , Opt_WarnOverflowedLiterals -- this does not play well with -ticky and the error message is misleading
  ]


adjustDynFlags :: [FilePath] -> PackageDynFlags -> Maybe String -> DynFlags -> DynFlags
adjustDynFlags paths packageState mbPackageName dflags
  = setImports paths
  $ setPackageDynFlags packageState
  $ setThisInstalledUnitId (maybe mainUnitId stringToUnitId mbPackageName)
  -- once we have package imports working, we want to import the base package and set this to
  -- the default instead of always compiling in the context of ghc-prim.
  $ apply wopt_set wOptsSet
  $ apply wopt_unset wOptsUnset
  $ apply wopt_set_fatal wOptsSetFatal
  $ apply xopt_set xExtensionsSet
  $ apply xopt_unset xExtensionsUnset
  $ apply gopt_set xFlagsSet
  dflags{
    mainModIs = mkModule primUnitId (mkModuleName "NotAnExistingName"), -- avoid DEL-6770
    debugLevel = 1,
    ghcLink = NoLink, hscTarget = HscNothing -- avoid generating .o or .hi files
    {-, dumpFlags = Opt_D_ppr_debug `EnumSet.insert` dumpFlags dflags -- turn on debug output from GHC-}
  }
  where apply f xs d = foldl' f d xs


setThisInstalledUnitId :: UnitId -> DynFlags -> DynFlags
setThisInstalledUnitId unitId dflags =
  dflags {thisInstalledUnitId = toInstalledUnitId unitId}

setImports :: [FilePath] -> DynFlags -> DynFlags
setImports paths dflags = dflags { importPaths = paths }



-- | Configures the @DynFlags@ for this session to DAML-1.2
--  compilation:
--     * Installs a custom log action;
--     * Sets up the package databases;
--     * Sets the import paths to the given list of 'FilePath'.
--     * if present, parses and applies custom options for GHC
--       (may fail if the custom options are inconsistent with std DAML ones)
setupDamlGHC :: GhcMonad m => [FilePath] -> Maybe String -> PackageDynFlags -> [String] -> m ()
setupDamlGHC importPaths mbPackageName packageState [] =
  modifyDynFlags $ adjustDynFlags importPaths packageState mbPackageName
-- if custom options are given, add them after the standard DAML flag setup
setupDamlGHC importPaths mbPackageName packageState customOpts = do
  setupDamlGHC importPaths mbPackageName packageState []
  damlDFlags <- getSessionDynFlags
  (dflags', leftover, warns) <- parseDynamicFilePragma damlDFlags $ map noLoc customOpts

  let leftoverError = CmdLineError $
        (unlines . ("Unable to parse custom flags:":) . map unLoc) leftover
  unless (null leftover) $ liftIO $ throwGhcExceptionIO leftoverError

  unless (null warns) $
    liftIO $ putStrLn $ unlines $ "Warnings:" : map (unLoc . Cmd.warnMsg) warns

  modifySession $ \h ->
    h { hsc_dflags = dflags', hsc_IC = (hsc_IC h) {ic_dflags = dflags' } }
