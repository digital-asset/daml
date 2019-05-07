-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint

-- | GHC utility functions. Importantly, code using our GHC should never:
--
-- * Call runGhc, use runGhcFast instead. It's faster and doesn't require config we don't have.
--
-- * Call setSessionDynFlags, use modifyDynFlags instead. It's faster and avoids loading packages.
module Development.IDE.UtilGHC(
    PackageDynFlags(..), setPackageDynFlags, getPackageDynFlags,
    modifyDynFlags,
    setPackageImports,
    setPackageDbs,
    fakeSettings,
    fakeLlvmConfig,
    prettyPrint,
    runGhcFast
    ) where

import           Config
import           Fingerprint
import           GHC                         hiding (convertLit)
import           GhcMonad
import           GhcPlugins                  as GHC hiding (fst3, (<>))
import           HscMain
import qualified Packages
import           Platform
import qualified EnumSet
import           Data.IORef
import GHC.Generics (Generic)

----------------------------------------------------------------------
-- GHC setup

setPackageDbs :: [FilePath] -> DynFlags -> DynFlags
setPackageDbs paths dflags =
  dflags
    { packageDBFlags =
        [PackageDB $ PkgConfFile path | path <- paths] ++ [NoGlobalPackageDB, ClearPackageDBs]
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

modifyDynFlags :: GhcMonad m => (DynFlags -> DynFlags) -> m ()
modifyDynFlags f = do
  newFlags <- f <$> getSessionDynFlags
  -- We do not use setSessionDynFlags here since we handle package
  -- initialization separately.
  modifySession $ \h ->
    h { hsc_dflags = newFlags, hsc_IC = (hsc_IC h) {ic_dflags = newFlags} }

-- | The subset of @DynFlags@ computed by package initialization.
data PackageDynFlags = PackageDynFlags
    { pdfPkgDatabase :: !(Maybe [(FilePath, [Packages.PackageConfig])])
    , pdfPkgState :: !Packages.PackageState
    , pdfThisUnitIdInsts :: !(Maybe [(ModuleName, Module)])
    } deriving (Generic)

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


-- | A version of `showSDoc` that uses default flags (to avoid uses of
-- `showSDocUnsafe`).
showSDocDefault :: SDoc -> String
showSDocDefault = showSDoc dynFlags
  where dynFlags = defaultDynFlags fakeSettings fakeLlvmConfig

prettyPrint :: Outputable a => a -> String
prettyPrint = showSDocDefault . ppr

-- | Like 'runGhc' but much faster (400x), with less IO and no file dependency
runGhcFast :: Ghc a -> IO a
-- copied from GHC with the nasty bits dropped
runGhcFast act = do
  ref <- newIORef (error "empty session")
  let session = Session ref
  flip unGhc session $ do
    dflags <- liftIO $ initDynFlags $ defaultDynFlags fakeSettings fakeLlvmConfig
    liftIO $ setUnsafeGlobalDynFlags dflags
    env <- liftIO $ newHscEnv dflags
    setSession env
    withCleanupSession act

-- These settings are mostly undefined, but define just enough for what we want to do (which isn't code gen)
fakeSettings :: Settings
fakeSettings = Settings
  {sTargetPlatform=platform
  ,sPlatformConstants=platformConstants
  ,sProjectVersion=cProjectVersion
  ,sProgramName="ghc"
  ,sOpt_P_fingerprint=fingerprint0
  }
  where
    platform = Platform{platformWordSize=8, platformOS=OSUnknown, platformUnregisterised=True}
    platformConstants = PlatformConstants{pc_DYNAMIC_BY_DEFAULT=False,pc_WORD_SIZE=8}

fakeLlvmConfig :: (LlvmTargets, LlvmPasses)
fakeLlvmConfig = ([], [])
