-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-----------------------------------------------------------------------------
--
-- (c) The University of Glasgow 2004-2009.
--
-- Package management tool
--
-----------------------------------------------------------------------------

module DA.Cli.Damlc.GhcPkg (recache, Flag(..), Verbosity(..)) where

import qualified GHC.PackageDb as GhcPkg
import GHC.PackageDb (BinaryStringRep(..))
import qualified Data.ByteString.UTF8 as UTF8
import qualified Distribution.ModuleName as ModuleName
import Distribution.ModuleName (ModuleName)
import Distribution.Types.InstalledPackageInfo (InstalledPackageInfo(..), AbiDependency(..), ExposedModule(..))
import Distribution.InstalledPackageInfo (parseInstalledPackageInfo, showInstalledPackageInfo, installedComponentId)
import Distribution.Package hiding (installedUnitId)
import Distribution.Text (display, simpleParse)
import Distribution.Version (versionNumbers)
import Distribution.Backpack (OpenUnitId(..), OpenModule(..))
import Distribution.Types.UnqualComponentName (unUnqualComponentName)
import Distribution.Types.LibraryName (libraryNameString)
import Distribution.Simple.Utils (fromUTF8BS, toUTF8BS, writeUTF8File, readUTF8File)
import qualified Data.Version as Version
import System.FilePath as FilePath
import System.Directory ( createDirectoryIfMissing )

import Prelude

import qualified Control.Exception as Exception
import Data.Maybe

import Control.Monad
import System.Directory ( getDirectoryContents,
                          removeFile,
                          getCurrentDirectory )
import System.Exit ( exitWith, ExitCode(..) )
import System.Environment ( getProgName )
import System.IO
import System.IO.Error
import Data.List
import qualified Data.Foldable as F
import qualified Data.Traversable as F
import qualified Data.Map as Map

#if defined(mingw32_HOST_OS)
import GHC.ConsoleHandler
#endif

#if defined(GLOB)
import qualified System.Info(os)
#endif

-- -----------------------------------------------------------------------------
-- Command-line syntax

data Flag
  = FlagGlobalConfig FilePath
  deriving Eq

data Verbosity = Silent | Normal | Verbose
    deriving (Show, Eq, Ord)

-- -----------------------------------------------------------------------------
-- Do the business

data Force = NoForce | ForceFiles | ForceAll | CannotForce
  deriving (Eq,Ord)

-- -----------------------------------------------------------------------------
-- Package databases

-- Some commands operate on a single database:
--      register, unregister, expose, hide, trust, distrust
-- however these commands also check the union of the available databases
-- in order to check consistency.  For example, register will check that
-- dependencies exist before registering a package.
--
-- Some commands operate  on multiple databases, with overlapping semantics:
--      list, describe, field

data PackageDB (mode :: GhcPkg.DbMode)
  = PackageDB {
      location, locationAbsolute :: !FilePath,
      -- We need both possibly-relative and definitely-absolute package
      -- db locations. This is because the relative location is used as
      -- an identifier for the db, so it is important we do not modify it.
      -- On the other hand we need the absolute path in a few places
      -- particularly in relation to the ${pkgroot} stuff.

      packageDbLock :: !(GhcPkg.DbOpenMode mode GhcPkg.PackageDbLock),
      -- If package db is open in read write mode, we keep its lock around for
      -- transactional updates.

      packages :: [InstalledPackageInfo]
    }

type PackageDBStack = [PackageDB 'GhcPkg.DbReadOnly]
        -- A stack of package databases.  Convention: head is the topmost
        -- in the stack.

-- | Selector for picking the right package DB to modify as 'register' and
-- 'recache' operate on the database on top of the stack, whereas 'modify'
-- changes the first database that contains a specific package.
data DbModifySelector = TopOne

allPackagesInStack :: PackageDBStack -> [InstalledPackageInfo]
allPackagesInStack = concatMap packages

-- | Retain only the part of the stack up to and including the given package
-- DB (where the global package DB is the bottom of the stack). The resulting
-- package DB stack contains exactly the packages that packages from the
-- specified package DB can depend on, since dependencies can only extend
-- down the stack, not up (e.g. global packages cannot depend on user
-- packages).
stackUpTo :: FilePath -> PackageDBStack -> PackageDBStack
stackUpTo to_modify = dropWhile ((/= to_modify) . location)

getPkgDatabases :: Verbosity
                -> FilePath
                -> IO (PackageDB 'GhcPkg.DbReadOnly, PackageDB 'GhcPkg.DbReadWrite)
getPkgDatabases verbosity global_conf = do
  let top_db = global_conf

  (db_stack, db_to_operate_on) <- getDatabases top_db

  when (verbosity > Normal) $ do
    infoLn ("db stack: " ++ show (location db_stack))
    infoLn ("modifying: " ++ (location db_to_operate_on))

  return (db_stack, db_to_operate_on)
  where
    getDatabases top_db = do
        (db_stack, mto_modify) <- do
                  db <- readParseDatabase verbosity (GhcPkg.DbOpenReadWrite TopOne) top_db
                    `Exception.catch` couldntOpenDbForModification top_db
                  let ro_db = db { packageDbLock = GhcPkg.DbOpenReadOnly }
                  return (ro_db, Just db)

        to_modify <- case mto_modify of
          Just db -> return db
          Nothing -> die "no database selected for modification"

        return (db_stack, to_modify)
      where
        couldntOpenDbForModification :: FilePath -> IOError -> IO a
        couldntOpenDbForModification db_path e = die $ "Couldn't open database "
          ++ db_path ++ " for modification: " ++ show e

readParseDatabase :: forall mode t. Verbosity
                  -> GhcPkg.DbOpenMode mode t
                  -> FilePath
                  -> IO (PackageDB mode)
readParseDatabase verbosity mode path
  | otherwise
  = do e <- tryIO $ getDirectoryContents path
       case e of
         Left err -> ioError err
         Right fs -> ignore_cache (const $ return ())
            where
                 confs = map (path </>) $ filter (".conf" `isSuffixOf`) fs

                 ignore_cache :: (FilePath -> IO ()) -> IO (PackageDB mode)
                 ignore_cache checkTime = do
                     -- If we're opening for modification, we need to acquire a
                     -- lock even if we don't open the cache now, because we are
                     -- going to modify it later.
                     lock <- F.mapM (const $ GhcPkg.lockPackageDb cache) mode
                     let doFile f = do checkTime f
                                       parseSingletonPackageConf verbosity f
                     pkgs <- mapM doFile confs
                     mkPackageDB pkgs lock
  where
    cache = path </> cachefilename

    mkPackageDB :: [InstalledPackageInfo]
                -> GhcPkg.DbOpenMode mode GhcPkg.PackageDbLock
                -> IO (PackageDB mode)
    mkPackageDB pkgs lock = do
      path_abs <- absolutePath path
      return $ PackageDB {
          location = path,
          locationAbsolute = path_abs,
          packageDbLock = lock,
          packages = pkgs
        }

parseSingletonPackageConf :: Verbosity -> FilePath -> IO InstalledPackageInfo
parseSingletonPackageConf verbosity file = do
  when (verbosity > Normal) $ infoLn ("reading package config: " ++ file)
  readUTF8File file >>= fmap fst . parsePackageInfo

cachefilename :: FilePath
cachefilename = "package.cache"

-- -----------------------------------------------------------------------------
-- Creating a new package DB


parsePackageInfo
        :: String
        -> IO (InstalledPackageInfo, [ValidateWarning])
parsePackageInfo str =
  case parseInstalledPackageInfo (UTF8.fromString str) of
    Right (warnings, ok) -> pure (mungePackageInfo ok, ws)
      where
        ws = [ msg | msg <- warnings
                   , not ("Unrecognized field pkgroot" `isPrefixOf` msg) ]
    Left err -> die (unlines $ F.toList err)

mungePackageInfo :: InstalledPackageInfo -> InstalledPackageInfo
mungePackageInfo ipi = ipi

-- -----------------------------------------------------------------------------
-- Making changes to a package database

data DBOp = RemovePackage InstalledPackageInfo
          | AddPackage    InstalledPackageInfo
          | ModifyPackage InstalledPackageInfo

changeDB :: Verbosity
         -> [DBOp]
         -> PackageDB 'GhcPkg.DbReadWrite
         -> PackageDBStack
         -> IO ()
changeDB verbosity cmds db db_stack = do
  let db' = updateInternalDB db cmds
  createDirectoryIfMissing True (location db')
  changeDBDir verbosity cmds db' db_stack

updateInternalDB :: PackageDB 'GhcPkg.DbReadWrite
                 -> [DBOp] -> PackageDB 'GhcPkg.DbReadWrite
updateInternalDB db cmds = db{ packages = foldl do_cmd (packages db) cmds }
 where
  do_cmd pkgs (RemovePackage p) =
    filter ((/= installedUnitId p) . installedUnitId) pkgs
  do_cmd pkgs (AddPackage p) = p : pkgs
  do_cmd pkgs (ModifyPackage p) =
    do_cmd (do_cmd pkgs (RemovePackage p)) (AddPackage p)


changeDBDir :: Verbosity
            -> [DBOp]
            -> PackageDB 'GhcPkg.DbReadWrite
            -> PackageDBStack
            -> IO ()
changeDBDir verbosity cmds db db_stack = do
  mapM_ do_cmd cmds
  updateDBCache verbosity db db_stack
 where
  do_cmd (RemovePackage p) = do
    let file = location db </> display (installedUnitId p) <.> "conf"
    when (verbosity > Normal) $ infoLn ("removing " ++ file)
    removeFileSafe file
  do_cmd (AddPackage p) = do
    let file = location db </> display (installedUnitId p) <.> "conf"
    when (verbosity > Normal) $ infoLn ("writing " ++ file)
    writeUTF8File file (showInstalledPackageInfo p)
  do_cmd (ModifyPackage p) =
    do_cmd (AddPackage p)

updateDBCache :: Verbosity
              -> PackageDB 'GhcPkg.DbReadWrite
              -> PackageDBStack
              -> IO ()
updateDBCache verbosity db db_stack = do
  let filename = location db </> cachefilename
      db_stack_below = stackUpTo (location db) db_stack

      pkgsCabalFormat :: [InstalledPackageInfo]
      pkgsCabalFormat = packages db

      -- | All the packages we can legally depend on in this step.
      dependablePkgsCabalFormat :: [InstalledPackageInfo]
      dependablePkgsCabalFormat = allPackagesInStack db_stack_below

      pkgsGhcCacheFormat :: [(PackageCacheFormat, Bool)]
      pkgsGhcCacheFormat
        -- See Note [Recompute abi-depends]
        = map (recomputeValidAbiDeps dependablePkgsCabalFormat)
        $ map convertPackageInfoToCacheFormat
          pkgsCabalFormat

      hasAnyAbiDepends :: InstalledPackageInfo -> Bool
      hasAnyAbiDepends x = length (abiDepends x) > 0

  -- warn when we find any (possibly-)bogus abi-depends fields;
  -- Note [Recompute abi-depends]
  when (verbosity >= Normal) $ do
    let definitelyBrokenPackages =
          nub
            . sort
            . map (unPackageName . GhcPkg.packageName . fst)
            . filter snd
            $ pkgsGhcCacheFormat
    when (definitelyBrokenPackages /= []) $ do
      warn "the following packages have broken abi-depends fields:"
      forM_ definitelyBrokenPackages $ \pkg ->
        warn $ "    " ++ pkg
    when (verbosity > Normal) $ do
      let possiblyBrokenPackages =
            nub
              . sort
              . filter (not . (`elem` definitelyBrokenPackages))
              . map (unPackageName . pkgName . packageId)
              . filter hasAnyAbiDepends
              $ pkgsCabalFormat
      when (possiblyBrokenPackages /= []) $ do
          warn $
            "the following packages have correct abi-depends, " ++
            "but may break in the future:"
          forM_ possiblyBrokenPackages $ \pkg ->
            warn $ "    " ++ pkg

  when (verbosity > Normal) $
      infoLn ("writing cache " ++ filename)

  GhcPkg.writePackageDb filename (map fst pkgsGhcCacheFormat) pkgsCabalFormat
    `catchIO` \e ->
      if isPermissionError e
      then die $ filename ++ ": you don't have permission to modify this file"
      else ioError e

  case packageDbLock db of
    GhcPkg.DbOpenReadWrite lock -> GhcPkg.unlockPackageDb lock

type PackageCacheFormat = GhcPkg.InstalledPackageInfo
                            ComponentId
                            PackageIdentifier
                            PackageName
                            UnitId
                            OpenUnitId
                            ModuleName
                            OpenModule

{- Note [Recompute abi-depends]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Like most fields, `ghc-pkg` relies on who-ever is performing package
registration to fill in fields; this includes the `abi-depends` field present
for the package.

However, this was likely a mistake, and is not very robust; in certain cases,
versions of Cabal may use bogus abi-depends fields for a package when doing
builds. Why? Because package database information is aggressively cached; it is
possible to work Cabal into a situation where it uses a cached version of
`abi-depends`, rather than the one in the actual database after it has been
recomputed.

However, there is an easy fix: ghc-pkg /already/ knows the `abi-depends` of a
package, because they are the ABIs of the packages pointed at by the `depends`
field. So it can simply look up the abi from the dependencies in the original
database, and ignore whatever the system registering gave it.

So, instead, we do two things here:

  - We throw away the information for a registered package's `abi-depends` field.

  - We recompute it: we simply look up the unit ID of the package in the original
    database, and use *its* abi-depends.

See Trac #14381, and Cabal issue #4728.

Additionally, because we are throwing away the original (declared) ABI deps, we
return a boolean that indicates whether any abi-depends were actually
overridden.

-}

recomputeValidAbiDeps :: [InstalledPackageInfo]
                      -> PackageCacheFormat
                      -> (PackageCacheFormat, Bool)
recomputeValidAbiDeps db pkg =
  (pkg { GhcPkg.abiDepends = newAbiDeps }, abiDepsUpdated)
  where
    newAbiDeps =
      catMaybes . flip map (GhcPkg.abiDepends pkg) $ \(k, _) ->
        case filter (\d -> installedUnitId d == k) db of
          [x] -> Just (k, unAbiHash (abiHash x))
          _   -> Nothing
    abiDepsUpdated =
      GhcPkg.abiDepends pkg /= newAbiDeps

convertPackageInfoToCacheFormat :: InstalledPackageInfo -> PackageCacheFormat
convertPackageInfoToCacheFormat pkg =
    GhcPkg.InstalledPackageInfo {
       GhcPkg.unitId             = installedUnitId pkg,
       GhcPkg.componentId        = installedComponentId pkg,
       GhcPkg.instantiatedWith   = instantiatedWith pkg,
       GhcPkg.sourcePackageId    = sourcePackageId pkg,
       GhcPkg.packageName        = packageName pkg,
       GhcPkg.packageVersion     = Version.Version (versionNumbers (packageVersion pkg)) [],
       GhcPkg.sourceLibName      =
         fmap (mkPackageName . unUnqualComponentName) (libraryNameString $ sourceLibName pkg),
       GhcPkg.depends            = depends pkg,
       GhcPkg.abiDepends         = map (\(AbiDependency k v) -> (k,unAbiHash v)) (abiDepends pkg),
       GhcPkg.abiHash            = unAbiHash (abiHash pkg),
       GhcPkg.importDirs         = importDirs pkg,
       GhcPkg.hsLibraries        = hsLibraries pkg,
       GhcPkg.extraLibraries     = extraLibraries pkg,
       GhcPkg.extraGHCiLibraries = extraGHCiLibraries pkg,
       GhcPkg.libraryDirs        = libraryDirs pkg,
       GhcPkg.libraryDynDirs     = libraryDynDirs pkg,
       GhcPkg.frameworks         = frameworks pkg,
       GhcPkg.frameworkDirs      = frameworkDirs pkg,
       GhcPkg.ldOptions          = ldOptions pkg,
       GhcPkg.ccOptions          = ccOptions pkg,
       GhcPkg.includes           = includes pkg,
       GhcPkg.includeDirs        = includeDirs pkg,
       GhcPkg.haddockInterfaces  = haddockInterfaces pkg,
       GhcPkg.haddockHTMLs       = haddockHTMLs pkg,
       GhcPkg.exposedModules     = map convertExposed (exposedModules pkg),
       GhcPkg.hiddenModules      = hiddenModules pkg,
       GhcPkg.indefinite         = indefinite pkg,
       GhcPkg.exposed            = exposed pkg,
       GhcPkg.trusted            = trusted pkg
    }
  where
    convertExposed (ExposedModule n reexport) = (n, reexport)

instance GhcPkg.BinaryStringRep ComponentId where
  fromStringRep = mkComponentId . fromStringRep
  toStringRep   = toStringRep . display

instance GhcPkg.BinaryStringRep PackageName where
  fromStringRep = mkPackageName . fromStringRep
  toStringRep   = toStringRep . display

instance GhcPkg.BinaryStringRep PackageIdentifier where
  fromStringRep = fromMaybe (error "BinaryStringRep PackageIdentifier")
                . simpleParse . fromStringRep
  toStringRep = toStringRep . display

instance GhcPkg.BinaryStringRep ModuleName where
  fromStringRep = ModuleName.fromString . fromStringRep
  toStringRep   = toStringRep . display

instance GhcPkg.BinaryStringRep String where
  fromStringRep = fromUTF8BS
  toStringRep   = toUTF8BS

instance GhcPkg.BinaryStringRep UnitId where
  fromStringRep = mkUnitId . fromStringRep
  toStringRep   = toStringRep . display

instance GhcPkg.DbUnitIdModuleRep UnitId ComponentId OpenUnitId ModuleName OpenModule where
  fromDbModule (GhcPkg.DbModule uid mod_name) = OpenModule uid mod_name
  fromDbModule (GhcPkg.DbModuleVar mod_name) = OpenModuleVar mod_name
  toDbModule (OpenModule uid mod_name) = GhcPkg.DbModule uid mod_name
  toDbModule (OpenModuleVar mod_name) = GhcPkg.DbModuleVar mod_name
  fromDbUnitId (GhcPkg.DbUnitId cid insts) = IndefFullUnitId cid (Map.fromList insts)
  fromDbUnitId (GhcPkg.DbInstalledUnitId uid)
    = DefiniteUnitId (unsafeMkDefUnitId uid)
  toDbUnitId (IndefFullUnitId cid insts) = GhcPkg.DbUnitId cid (Map.toList insts)
  toDbUnitId (DefiniteUnitId def_uid)
    = GhcPkg.DbInstalledUnitId (unDefUnitId def_uid)


recache :: Verbosity -> FilePath -> IO ()
recache _verbosity globalPkgDb = do
  let verbosity = Verbose
  (_db_stack, db_to_operate_on) <- getPkgDatabases verbosity globalPkgDb
  changeDB verbosity [] db_to_operate_on [_db_stack]

-----------------------------------------------------------------------------
-- Sanity-check a new package config, and automatically build GHCi libs
-- if requested.

type ValidateError   = (Force,String)
type ValidateWarning = String

newtype Validate a = V { runValidate :: IO (a, [ValidateError],[ValidateWarning]) }

instance Functor Validate where
    fmap = liftM

instance Applicative Validate where
    pure a = V $ pure (a, [], [])
    (<*>) = ap

instance Monad Validate where
   m >>= k = V $ do
      (a, es, ws) <- runValidate m
      (b, es', ws') <- runValidate (k a)
      return (b,es++es',ws++ws')

getProgramName :: IO String
getProgramName = liftM (`withoutSuffix` ".bin") getProgName
   where str `withoutSuffix` suff
            | suff `isSuffixOf` str = take (length str - length suff) str
            | otherwise             = str

die :: String -> IO a
die = dieWith 1

dieWith :: Int -> String -> IO a
dieWith ec s = do
  prog <- getProgramName
  reportError (prog ++ ": " ++ s)
  exitWith (ExitFailure ec)

warn :: String -> IO ()
warn = reportError

-- send info messages to stdout
infoLn :: String -> IO ()
infoLn = putStrLn

reportError :: String -> IO ()
reportError s = do hFlush stdout; hPutStrLn stderr s

catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch

tryIO :: IO a -> IO (Either Exception.IOException a)
tryIO = Exception.try

-- removeFileSave doesn't throw an exceptions, if the file is already deleted
removeFileSafe :: FilePath -> IO ()
removeFileSafe fn =
  removeFile fn `catchIO` \ e ->
    when (not $ isDoesNotExistError e) $ ioError e

-- | Turn a path relative to the current directory into a (normalised)
-- absolute path.
absolutePath :: FilePath -> IO FilePath
absolutePath path = return . normalise . (</> path) =<< getCurrentDirectory

