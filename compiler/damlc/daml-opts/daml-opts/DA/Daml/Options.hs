-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Set up the GHC monad in a way that works for us
module DA.Daml.Options
    ( checkDFlags
    , expandSdkPackages
    , fakeDynFlags
    , findProjectRoot
    , generatePackageState
    , memoIO
    , mkPackageFlag
    , mkBaseUnits
    , runGhcFast
    , setPackageDynFlags
    , setupDamlGHC
    , toCompileOpts
    , PackageDynFlags(..)
    , dataDependableExtensions
    ) where

import Control.Exception
import Control.Exception.Safe (handleIO)
import Control.Concurrent.Extra
import Control.Monad.Extra
import qualified CmdLineParser as Cmd (warnMsg)
import Data.IORef
import Data.List.Extra
import Data.Maybe (fromMaybe)
import DynFlags (parseDynamicFilePragma)
import qualified EnumSet as ES
import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import Config (cProjectVersion)
import Development.Shake (Action)
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Location
import qualified Platform as P
import qualified EnumSet
import GHC                         hiding (convertLit)
import GHC.Fingerprint (fingerprint0)
import GHC.LanguageExtensions.Type
import GhcMonad
import GhcPlugins as GHC hiding (fst3, (<>), parseUnitId)
import HscMain
import Panic (throwGhcExceptionIO)
import System.Directory
import System.FilePath
import qualified DA.Daml.LF.Ast.Version as LF

import DA.Bazel.Runfiles
import DA.Daml.Project.Consts
import DA.Daml.Project.Util
import DA.Daml.Options.Types
import DA.Daml.Preprocessor
import Development.IDE.GHC.Util
import qualified Development.IDE.Types.Options as Ghcide
import SdkVersion (damlStdlib)

-- | Convert to ghcide’s IdeOptions type.
toCompileOpts :: Options -> Ghcide.IdeReportProgress -> Ghcide.IdeOptions
toCompileOpts options@Options{..} reportProgress =
    Ghcide.IdeOptions
      { optPreprocessor = if optIsGenerated then generatedPreprocessor else damlPreprocessor dataDependableExtensions (optUnitId options)
      , optGhcSession = getDamlGhcSession options
      , optPkgLocationOpts = Ghcide.IdePkgLocationOptions
          { optLocateHieFile = locateInPkgDb "hie"
          , optLocateSrcFile = locateInPkgDb "daml"
          }
      , optExtensions = ["daml"]
      , optThreads = optThreads
      , optShakeFiles = if getIncrementalBuild optIncrementalBuild then Just ".daml/build/shake" else Nothing
      , optShakeProfiling = optShakeProfiling
      , optReportProgress = reportProgress
      , optLanguageSyntax = "daml"
      , optNewColonConvention = True
      , optKeywords = damlKeywords
      , optDefer = Ghcide.IdeDefer False
      }
  where
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

damlKeywords :: [T.Text]
damlKeywords =
  [ "as"
  , "case", "of"
  , "class", "instance", "type"
  , "data", "family", "newtype"
  , "default"
  , "deriving"
  , "do"
  , "forall"
  , "hiding"
  , "if", "then", "else"
  , "import", "qualified", "hiding"
  , "infix", "infixl", "infixr"
  , "let", "in", "where"
  , "module"

  -- DAML-specific keywords, sync with daml12.tmLanguage.xml when new
  -- keywords are added.
  , "agreement", "controller", "can", "ensure", "signatory", "nonconsuming", "observer"
  , "preconsuming", "postconsuming", "with", "choice", "template", "key", "maintainer"
  ]

getDamlGhcSession :: Options -> Action (FilePath -> Action HscEnvEq)
getDamlGhcSession _options@Options{..} = do
    findProjectRoot <- liftIO $ memoIO findProjectRoot
    pure $ \file -> do
        mbRoot <- liftIO (findProjectRoot file)
        useNoFile_ (DamlGhcSession $ toNormalizedFilePath' <$> mbRoot)

-- | Find the daml.yaml given a starting file or directory.
findProjectRoot :: FilePath -> IO (Maybe FilePath)
findProjectRoot file = do
    isFile <- doesFileExist (takeDirectory file)
    let dir = if isFile then takeDirectory file else file
    findM hasProjectConfig (ascendants dir)
  where
    hasProjectConfig :: FilePath -> IO Bool
    hasProjectConfig p = doesFileExist (p </> projectConfigName)


-- | Memoize an IO function, with the characteristics:
--
--   * If multiple people ask for a result simultaneously, make sure you only compute it once.
--
--   * If there are exceptions, repeatedly reraise them.
--
--   * If the caller is aborted (async exception) finish computing it anyway.
--
-- This matches the memoIO function in ghcide.
memoIO :: Ord a => (a -> IO b) -> IO (a -> IO b)
memoIO op = do
    ref <- newVar Map.empty
    return $ \k -> join $ mask_ $ modifyVar ref $ \mp ->
        case Map.lookup k mp of
            Nothing -> do
                res <- onceFork $ op k
                return (Map.insert k res mp, res)
            Just res -> return (mp, res)

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


generatePackageState :: LF.Version -> Maybe NormalizedFilePath -> [FilePath] -> [PackageFlag] -> IO PackageDynFlags
generatePackageState lfVersion mbProjRoot paths pkgImports = do
  versionedPaths <- getPackageDbs lfVersion mbProjRoot paths
  let dflags = setPackageImports pkgImports $ setPackageDbs versionedPaths fakeDynFlags
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

setPackageImports :: [PackageFlag] -> DynFlags -> DynFlags
setPackageImports pkgImports dflags = dflags {
    packageFlags = packageFlags dflags ++ pkgImports
    , generalFlags = Opt_HideAllPackages `EnumSet.insert` generalFlags dflags
    }

-- | fakeDynFlags that we can use as input for `initDynFlags`.
fakeDynFlags :: DynFlags
fakeDynFlags = defaultDynFlags
                  settings
                  mempty
    where
        settings = Settings
                   { sTargetPlatform = platform
                   , sPlatformConstants = platformConstants
                   , sProgramName = "ghc"
                   , sProjectVersion = cProjectVersion
                   , sOpt_P_fingerprint = fingerprint0
                   }
        platform = P.Platform
          { platformWordSize=8
          , platformOS=P.OSUnknown
          , platformUnregisterised=True
          }
        platformConstants = PlatformConstants
          { pc_DYNAMIC_BY_DEFAULT=False
          , pc_WORD_SIZE=8
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
  [ -- Haskell 2010 extensions which are enabled by default (we would need to
    -- list them for `dataDependableExtensions` below anyway, so let's make
    -- them explicit here)
    ImplicitPrelude, StarIsType, MonomorphismRestriction, TraditionalRecordSyntax
  , EmptyDataDecls, PatternGuards, DoAndIfThenElse, RelaxedPolyRec, NondecreasingIndentation
  , -- syntactic convenience
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

-- | Extensions which we support with data-dependencies.
dataDependableExtensions :: ES.EnumSet Extension
dataDependableExtensions = ES.fromList $ xExtensionsSet ++
  [ -- useful for beginners to learn about type inference
    PartialTypeSignatures
    -- needed for script and triggers
  , ApplicativeDo
    -- used in daml-stdlib and triggers and a very reasonable
    -- extension in general in the presence of TypeApplications
  , AllowAmbiguousTypes
    -- helpful for documentation purposes
  , InstanceSigs
    -- convenient syntactic sugar that does not impact the type level at all
  , MultiWayIf
    -- there's no way for our users to actually use this and listing it here
    -- removes a lot of warning from out stdlib, script and trigger builds
    -- NOTE: This should not appear on any list of extensions that are
    -- compatible with data-dependencies since this would spur wrong hopes.
  , Cpp
  ]

-- | Language settings _disabled_ ($-XNo...$) in the DAML-1.2 compilation
xExtensionsUnset :: [Extension]
xExtensionsUnset =
  [ -- This is part of Haskell 2010 and would hence be enabled by default,
    -- which makes zero sense for DAML.
    ForeignFunctionInterface
  ]

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
-- Can enable when we are on GHC >= 8.10 (we should, after all we
-- upstreamed it :) ).
--  , Opt_WarnPrepositiveQualifiedModule
  , Opt_WarnOverlappingPatterns
  , Opt_WarnIncompletePatterns
-- Confirmed that nothing in template desugaring prevents us from
-- enabling these.
  -- , Opt_WarnUnusedMatches
  -- , Opt_WarnUnusedForalls
  -- , Opt_WarnUnusedPatternBinds
  -- , Opt_WarnUnusedTopBinds
  -- , Opt_WarnUnusedTypePatterns
-- Template desugaring in the presence of local binds will currently
-- trigger this.
  -- , Opt_WarnUnusedLocalBinds
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

newtype GhcVersionHeader = GhcVersionHeader FilePath

adjustDynFlags :: Options -> GhcVersionHeader -> FilePath -> DynFlags -> DynFlags
adjustDynFlags options@Options{..} (GhcVersionHeader versionHeader) tmpDir dflags
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
 $ setThisInstalledUnitId (fromMaybe mainUnitId $ optUnitId options)
  -- once we have package imports working, we want to import the base package and set this to
  -- the default instead of always compiling in the context of ghc-prim.
  $ apply wopt_set wOptsSet
  $ apply wopt_unset wOptsUnset
  $ apply wopt_set_fatal wOptsSetFatal
  $ apply xopt_set xExtensionsSet
  $ apply xopt_unset xExtensionsUnset
  $ apply gopt_set (xFlagsSet options)
  $ addPlatformFlags
  $ addCppFlags
  dflags{
    mainModIs = mkModule primUnitId (mkModuleName "NotAnExistingName"), -- avoid DEL-6770
    debugLevel = 1,
    ghcLink = NoLink, hscTarget = HscNothing, -- avoid generating .o or .hi files
    {-, dumpFlags = Opt_D_ppr_debug `EnumSet.insert` dumpFlags dflags -- turn on debug output from GHC-}
    ghcVersionFile = Just versionHeader
  }
  where
    apply f xs d = foldl' f d xs
    alterSettings f d = d { settings = f (settings d) }
    addCppFlags = case optCppPath of
        Nothing -> id
        Just cppPath -> alterSettings $ \s -> s
            { sPgm_P = (cppPath, [])
            , sOpt_P = "-P" : ["-D" <> T.unpack flag | flag <- cppFlags]
                -- We add "-P" here to suppress #line pragmas from the
                -- preprocessor (hpp, specifically) because the daml
                -- parser can't handle them. This is a non-issue right now
                -- because ghcversion.h is empty, but if it weren't empty
                -- it would result in #line pragmas. By suppressing these
                -- pragmas, line numbers may be wrong up when using CPP.
                -- Ideally we fix the issue with the daml parser and
                -- then remove this flag.
            , sTmpDir = tmpDir
                -- sometimes this is required by CPP?
            }

    cppFlags = map LF.featureCppFlag (LF.allFeaturesForVersion optDamlLfVersion)

    -- We need to add platform info in order to run CPP. To prevent
    -- .hi file incompatibilities, we set the platform the same way
    -- for everyone even if they don't use CPP.
    addPlatformFlags = alterSettings $ \s -> s
        { sTargetPlatform = P.Platform
            { platformArch = P.ArchUnknown
            , platformOS = P.OSUnknown
            , platformWordSize = 8
            , platformUnregisterised = True
            , platformHasGnuNonexecStack = False
            , platformHasIdentDirective = False
            , platformHasSubsectionsViaSymbols = False
            , platformIsCrossCompiling = False
            }
        }

setThisInstalledUnitId :: UnitId -> DynFlags -> DynFlags
setThisInstalledUnitId unitId dflags =
  dflags {thisInstalledUnitId = toInstalledUnitId unitId}

setImports :: [FilePath] -> DynFlags -> DynFlags
setImports paths dflags = dflags { importPaths = paths }

locateGhcVersionHeader :: IO GhcVersionHeader
locateGhcVersionHeader = GhcVersionHeader <$> locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "ghcversion.h")

-- | Configures the @DynFlags@ for this session to DAML-1.2
--  compilation:
--     * Installs a custom log action;
--     * Sets up the package databases;
--     * Sets the import paths to the given list of 'FilePath'.
--     * if present, parses and applies custom options for GHC
--       (may fail if the custom options are inconsistent with std DAML ones)
setupDamlGHC :: GhcMonad m => Maybe NormalizedFilePath -> Options -> m ()
setupDamlGHC mbProjectRoot options@Options{..} = do
  tmpDir <- liftIO getTemporaryDirectory
  versionHeader <- liftIO locateGhcVersionHeader
  modifyDynFlags $ adjustDynFlags options versionHeader tmpDir

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
  whenJust mbProjectRoot $ \(fromNormalizedFilePath -> projRoot) ->
    -- Make import paths relative to project root. Otherwise, we
    -- can end up with the same file being represented multiple times
    -- with different prefixes or not found at all if our CWD is not the
    -- project root.
    -- Note that in the IDE project root is absolute whereas it is
    -- relative in `daml build`.
    modifyDynFlags $ \dflags ->
      dflags { importPaths = map (\p -> normalise (projRoot </> p)) (importPaths dflags) }


-- | Check for bad @DynFlags@.
-- Checks:
--    * thisInstalledUnitId not contained in loaded packages.
checkDFlags :: Options -> DynFlags -> IO DynFlags
checkDFlags Options {..} dflags@DynFlags {..}
    | not optDflagCheck || thisInstalledUnitId == toInstalledUnitId primUnitId =
        pure dflags
    | otherwise = do
        case lookupPackage dflags $
             DefiniteUnitId $ DefUnitId thisInstalledUnitId of
            Nothing -> pure dflags
            Just _conf ->
                fail $
                "Package " <> installedUnitIdString thisInstalledUnitId <>
                " imports a package with the same name. \
            \ Please check your dependencies and rename the package you are compiling \
            \ or the dependency."

-- Expand SDK package dependencies using the SDK root path.
-- E.g. `daml-trigger` --> `$DAML_SDK/daml-libs/daml-trigger.dar`
-- When invoked outside of the SDK, we will only error out
-- if there is actually an SDK package so that
-- When there is no SDK
expandSdkPackages :: LF.Version -> [FilePath] -> IO [FilePath]
expandSdkPackages lfVersion dars = do
    mbSdkPath <- handleIO (\_ -> pure Nothing) $ Just <$> getSdkPath
    mapM (expand mbSdkPath) (nubOrd $ concatMap addDep dars)
  where
    isSdkPackage fp = takeExtension fp `notElem` [".dar", ".dalf"]
    sdkSuffix = "-" <> LF.renderVersion lfVersion
    expand mbSdkPath fp
      | fp `elem` basePackages = pure fp
      | isSdkPackage fp = case mbSdkPath of
            Just sdkPath -> pure $ sdkPath </> "daml-libs" </> fp <> sdkSuffix <.> "dar"
            Nothing -> fail $ "Cannot resolve SDK dependency '" ++ fp ++ "'. Use daml assistant."
      | otherwise = pure fp
    -- For `dependencies` you need to specify all transitive dependencies.
    -- However, for the packages in the SDK that is an implementation detail
    -- so we automagically insert `daml-script` if you’ve specified `daml-trigger`.
    addDep fp
      | isSdkPackage fp = fp : Map.findWithDefault [] fp sdkDependencies
      | otherwise = [fp]
    sdkDependencies = Map.fromList
      [ ("daml-trigger", ["daml-script"]) ]


mkPackageFlag :: UnitId -> PackageFlag
mkPackageFlag unitId = ExposePackage ("--package " <> unitIdString unitId) (UnitIdArg unitId) (ModRenaming True [])

mkBaseUnits :: Maybe UnitId -> [UnitId]
mkBaseUnits optMbPackageName
  | optMbPackageName == Just (stringToUnitId "daml-prim") =
      []
  | optMbPackageName == Just damlStdlib =
      [ stringToUnitId "daml-prim" ]
  | otherwise =
      [ stringToUnitId "daml-prim"
      , damlStdlib ]
