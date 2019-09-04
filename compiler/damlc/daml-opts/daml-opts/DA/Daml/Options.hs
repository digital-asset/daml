-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Set up the GHC monad in a way that works for us
module DA.Daml.Options
    ( toCompileOpts
    , generatePackageState
    , PackageDynFlags(..)
    ) where

import Control.Monad
import qualified CmdLineParser as Cmd (warnMsg)
import Data.Bifunctor
import Data.IORef
import Data.List
import DynFlags (parseDynamicFilePragma)
import qualified EnumSet
import GHC                         hiding (convertLit)
import GHC.LanguageExtensions.Type
import GhcMonad
import GhcPlugins as GHC hiding (fst3, (<>))
import HscMain
import Panic (throwGhcExceptionIO)
import System.Directory
import System.FilePath

import DA.Daml.Options.Types
import DA.Daml.Preprocessor
import Development.IDE.GHC.Util
import qualified Development.IDE.Types.Options as HieCore

-- | Convert to hie-core’s IdeOptions type.
toCompileOpts :: Options -> HieCore.IdeReportProgress -> HieCore.IdeOptions
toCompileOpts options@Options{..} reportProgress =
    HieCore.IdeOptions
      { optPreprocessor = if optIsGenerated then noPreprocessor else damlPreprocessor optMbPackageName
      , optGhcSession = do
            env <- liftIO $ runGhcFast $ do
                setupDamlGHC options
                GHC.getSession
            pkg <- liftIO $ generatePackageState optPackageDbs optHideAllPkgs $ map (second toRenaming) optPackageImports
            dflags <- liftIO $ checkDFlags $ setPackageDynFlags pkg $ hsc_dflags env
            return env{hsc_dflags = dflags}
      , optPkgLocationOpts = HieCore.IdePkgLocationOptions
          { optLocateHieFile = locateInPkgDb "hie"
          , optLocateSrcFile = locateInPkgDb "daml"
          }
      , optExtensions = ["daml"]
      , optThreads = optThreads
      , optShakeProfiling = optShakeProfiling
      , optReportProgress = reportProgress
      , optLanguageSyntax = "daml"
      , optNewColonConvention = True
      }
  where
    toRenaming aliases = ModRenaming False [(GHC.mkModuleName mod, GHC.mkModuleName alias) | (mod, alias) <- aliases]
    locateInPkgDb :: String -> PackageConfig -> GHC.Module -> IO (Maybe FilePath)
    locateInPkgDb ext pkgConfig mod
      | (importDir : _) <- importDirs pkgConfig = do
            -- We only produce package configs with exactly one importDir.
            let path = importDir </> moduleNameSlashes (GHC.moduleName mod) <.> ext
            exists <- doesFileExist path
            pure $ if exists
                then Just path
                else Nothing
      | otherwise = pure Nothing

-- | The subset of @DynFlags@ computed by package initialization.
data PackageDynFlags = PackageDynFlags
    { pdfPkgDatabase :: !(Maybe [(FilePath, [PackageConfig])])
    , pdfPkgState :: !PackageState
    , pdfThisUnitIdInsts :: !(Maybe [(GHC.ModuleName, GHC.Module)])
    }

setPackageDynFlags :: PackageDynFlags -> DynFlags -> DynFlags
setPackageDynFlags PackageDynFlags{..} dflags = dflags
    { pkgDatabase = pdfPkgDatabase
    , pkgState = pdfPkgState
    , thisUnitIdInsts_ = pdfThisUnitIdInsts
    }

getPackageDynFlags :: DynFlags -> PackageDynFlags
getPackageDynFlags DynFlags{..} = PackageDynFlags
    { pdfPkgDatabase = pkgDatabase
    , pdfPkgState = pkgState
    , pdfThisUnitIdInsts = thisUnitIdInsts_
    }

generatePackageState :: [FilePath] -> Bool -> [(String, ModRenaming)] -> IO PackageDynFlags
generatePackageState paths hideAllPkgs pkgImports = do
  let dflags = setPackageImports hideAllPkgs pkgImports $ setPackageDbs paths fakeDynFlags
  (newDynFlags, _) <- initPackages dflags
  pure $ getPackageDynFlags newDynFlags


setPackageDbs :: [FilePath] -> DynFlags -> DynFlags
setPackageDbs paths dflags =
  dflags
    { packageDBFlags =
        [PackageDB $ PkgConfFile $ path </> "package.conf.d" | path <- paths] ++ [NoGlobalPackageDB, ClearPackageDBs]
    , pkgDatabase = if null paths then Just [] else Nothing
      -- if we don't load any packages set the package database to empty and loaded.
    , settings = (settings dflags)
        {sTopDir = case paths of p:_ -> p; _ -> error "No package db path available but used $topdir"
        , sSystemPackageConfig = case paths of p:_ -> p; _ -> error "No package db path available but used system package config"
        }
    }

setPackageImports :: Bool -> [(String, ModRenaming)] -> DynFlags -> DynFlags
setPackageImports hideAllPkgs pkgImports dflags = dflags {
    packageFlags = packageFlags dflags ++
        [ExposePackage pkgName (UnitIdArg $ stringToUnitId pkgName) renaming
        | (pkgName, renaming) <- pkgImports
        ]
    , generalFlags = if hideAllPkgs
                      then Opt_HideAllPackages `EnumSet.insert` generalFlags dflags
                      else generalFlags dflags
    }

-- | Like 'runGhc' but much faster (400x), with less IO and no file dependency
runGhcFast :: GHC.Ghc a -> IO a
-- copied from GHC with the nasty bits dropped
runGhcFast act = do
  ref <- newIORef (error "empty session")
  let session = Session ref
  flip unGhc session $ do
    dflags <- liftIO $ initDynFlags fakeDynFlags
    liftIO $ setUnsafeGlobalDynFlags dflags
    env <- liftIO $ newHscEnv dflags
    setSession env
    GHC.withCleanupSession act

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
  , MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, GeneralizedNewtypeDeriving, TypeSynonymInstances
  , DefaultSignatures, StandaloneDeriving, FunctionalDependencies, DeriveFunctor
    -- let generalization
  , MonoLocalBinds
    -- replacing primitives
  , RebindableSyntax, OverloadedStrings
    -- strictness
  , Strict, StrictData
    -- avoiding letrec in list comp (see DEL-3841)
  , MonadComprehensions
    -- package imports
  , PackageImports
    -- our changes
  , DamlSyntax
  ]


-- | Language settings _disabled_ ($-XNo...$) in the DAML-1.2 compilation
xExtensionsUnset :: [Extension]
xExtensionsUnset = [ ]

-- | Flags set for DAML-1.2 compilation
xFlagsSet :: Options -> [GeneralFlag]
xFlagsSet options =
 [Opt_Ticky
 ] ++
 [ Opt_DoCoreLinting | optCoreLinting options ]

-- | Warning options set for DAML compilation. Note that these can be modified
--   (per file) by the user via file headers '{-# OPTIONS -fwarn-... #-} and
--   '{-# OPTIONS -no-warn-... #-}'.
wOptsSet :: [ WarningFlag ]
wOptsSet =
  [ Opt_WarnUnusedImports
--  , Opt_WarnPrepositiveQualifiedModule
  , Opt_WarnOverlappingPatterns
  , Opt_WarnIncompletePatterns
  ]

-- | Warning options set for DAML compilation, which become errors.
wOptsSetFatal :: [ WarningFlag ]
wOptsSetFatal =
  [ Opt_WarnMissingFields
  , Opt_WarnMissingMethods
  ]

-- | Warning options unset for DAML compilation. Note that these can be modified
--   (per file) by the user via file headers '{-# OPTIONS -fwarn-... #-} and
--   '{-# OPTIONS -no-warn-... #-}'.
wOptsUnset :: [ WarningFlag ]
wOptsUnset =
  [ Opt_WarnMissingMonadFailInstances -- failable pattern plus RebindableSyntax raises this error
  , Opt_WarnOverflowedLiterals -- this does not play well with -ticky and the error message is misleading
  ]


adjustDynFlags :: Options -> DynFlags -> DynFlags
adjustDynFlags options@Options{..} dflags
  =
  -- Generally, the lexer's "haddock mode" is disabled (`Haddock
  -- False` is the default option. In this case, we run the lexer in
  -- "keep raw token stream mode" (meaning basically, harvest all
  -- comments encountered during parsing). The exception is when
  -- parsing for daml-doc (c.f. `DA.Cli.Damlc.Command.Damldoc`).
  (case optHaddock of
      Haddock True -> flip gopt_set Opt_Haddock
      Haddock False -> flip gopt_set Opt_KeepRawTokenStream
  )
 $ setImports optImportPath
 $ setThisInstalledUnitId (maybe mainUnitId stringToUnitId optMbPackageName)
  -- once we have package imports working, we want to import the base package and set this to
  -- the default instead of always compiling in the context of ghc-prim.
  $ apply wopt_set wOptsSet
  $ apply wopt_unset wOptsUnset
  $ apply wopt_set_fatal wOptsSetFatal
  $ apply xopt_set xExtensionsSet
  $ apply xopt_unset xExtensionsUnset
  $ apply gopt_set (xFlagsSet options)
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
setupDamlGHC :: GhcMonad m => Options -> m ()
setupDamlGHC options@Options{..} = do
  modifyDynFlags $ adjustDynFlags options

  unless (null optGhcCustomOpts) $ do
    damlDFlags <- getSessionDynFlags
    (dflags', leftover, warns) <- parseDynamicFilePragma damlDFlags $ map noLoc optGhcCustomOpts

    let leftoverError = CmdLineError $
          (unlines . ("Unable to parse custom flags:":) . map unLoc) leftover
    unless (null leftover) $ liftIO $ throwGhcExceptionIO leftoverError

    unless (null warns) $
      liftIO $ putStrLn $ unlines $ "Warnings:" : map (unLoc . Cmd.warnMsg) warns

    modifySession $ \h ->
      h { hsc_dflags = dflags', hsc_IC = (hsc_IC h) {ic_dflags = dflags' } }

-- | Check for bad @DynFlags@.
-- Checks:
--    * thisInstalledUnitId not contained in loaded packages.
checkDFlags :: DynFlags -> IO DynFlags
checkDFlags dflags@DynFlags {..} = do
    case lookupPackage dflags $ DefiniteUnitId $ DefUnitId thisInstalledUnitId of
        Nothing -> pure dflags
        Just _conf ->
            fail $
            "Package " <> installedUnitIdString thisInstalledUnitId <>
            " imports a package with the same name. \
            \ Please check your dependencies and rename the package you are compiling \
            \ or the dependency."
