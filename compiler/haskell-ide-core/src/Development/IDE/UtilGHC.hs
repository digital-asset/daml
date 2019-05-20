-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-missing-fields #-} -- to enable prettyPrint
{-# LANGUAGE CPP #-}

-- | GHC utility functions. Importantly, code using our GHC should never:
--
-- * Call runGhc, use runGhcFast instead. It's faster and doesn't require config we don't have.
--
-- * Call setSessionDynFlags, use modifyDynFlags instead. It's faster and avoids loading packages.
module Development.IDE.UtilGHC(
    lookupPackageConfig,
    modifyDynFlags,
    fakeDynFlags,
    prettyPrint,
    runGhcFast,
    runGhcEnv
    ) where

import           Config
import           Fingerprint
import           GHC                         hiding (convertLit)
import           GhcMonad
import           GhcPlugins                  as GHC hiding (fst3, (<>))
import           HscMain
import           Platform
import           Data.IORef
import Control.Exception
import FileCleanup

----------------------------------------------------------------------
-- GHC setup

modifyDynFlags :: GhcMonad m => (DynFlags -> DynFlags) -> m ()
modifyDynFlags f = do
  newFlags <- f <$> getSessionDynFlags
  -- We do not use setSessionDynFlags here since we handle package
  -- initialization separately.
  modifySession $ \h ->
    h { hsc_dflags = newFlags, hsc_IC = (hsc_IC h) {ic_dflags = newFlags} }

lookupPackageConfig :: UnitId -> HscEnv -> Maybe PackageConfig
lookupPackageConfig unitId env =
    lookupPackage' False pkgConfigMap unitId
    where
        pkgConfigMap =
            -- For some weird reason, the GHC API does not provide a way to get the PackageConfigMap
            -- from PackageState so we have to wrap it in DynFlags first.
            getPackageConfigMap $ hsc_dflags env



prettyPrint :: Outputable a => a -> String
prettyPrint = showSDoc fakeDynFlags . ppr

runGhcEnv :: HscEnv -> Ghc a -> IO a
runGhcEnv env act = do
    filesToClean <- newIORef emptyFilesToClean
    dirsToClean <- newIORef mempty
    let dflags = (hsc_dflags env){filesToClean=filesToClean, dirsToClean=dirsToClean}
    ref <- newIORef env{hsc_dflags=dflags}
    unGhc act (Session ref) `finally` do
        cleanTempFiles dflags
        cleanTempDirs dflags


-- | Like 'runGhc' but much faster (400x), with less IO and no file dependency
runGhcFast :: Ghc a -> IO a
-- copied from GHC with the nasty bits dropped
runGhcFast act = do
  ref <- newIORef (error "empty session")
  let session = Session ref
  flip unGhc session $ do
    dflags <- liftIO $ initDynFlags fakeDynFlags
    liftIO $ setUnsafeGlobalDynFlags dflags
    env <- liftIO $ newHscEnv dflags
    setSession env
    withCleanupSession act

-- Fake DynFlags which are mostly undefined, but define enough to do a little bit
fakeDynFlags :: DynFlags
fakeDynFlags = defaultDynFlags settings ([], [])
    where
        settings = Settings
            {sTargetPlatform = Platform
                {platformWordSize = 8
                ,platformOS = OSUnknown
                ,platformUnregisterised = True
                }
            ,sPlatformConstants = PlatformConstants
                {pc_DYNAMIC_BY_DEFAULT = False
                ,pc_WORD_SIZE = 8
                }
#ifndef GHC_STABLE
            ,sIntegerLibraryType = IntegerSimple
#endif
            ,sProjectVersion = cProjectVersion
            ,sProgramName = "ghc"
            ,sOpt_P_fingerprint = fingerprint0
            }
