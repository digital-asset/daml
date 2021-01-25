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

-- We never want to link against terminfo while bootstrapping.
#if defined(BOOTSTRAPPING)
#if defined(WITH_TERMINFO)
#undef WITH_TERMINFO
#endif
#endif

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
import Distribution.InstalledPackageInfo as Cabal
import Distribution.Package hiding (installedUnitId)
import Distribution.Text
import Distribution.Version
import Distribution.Backpack
import Distribution.Types.UnqualComponentName
import Distribution.Types.LibraryName
import Distribution.Types.MungedPackageName
import Distribution.Types.MungedPackageId
import Distribution.Simple.Utils (fromUTF8BS, toUTF8BS, writeUTF8File, readUTF8File)
import qualified Data.Version as Version
import System.FilePath as FilePath
import qualified System.FilePath.Posix as FilePath.Posix
import System.Directory ( createDirectoryIfMissing,
                          getModificationTime )

import Prelude

import qualified Control.Exception as Exception
import Data.Maybe

import Control.Monad
import System.Directory ( doesDirectoryExist, getDirectoryContents,
                          doesFileExist, removeFile,
                          getCurrentDirectory )
import System.Exit ( exitWith, ExitCode(..) )
import System.Environment ( getProgName )
#if defined(darwin_HOST_OS) || defined(linux_HOST_OS) || defined(mingw32_HOST_OS)
import System.Environment ( getExecutablePath )
#endif
import System.IO
import System.IO.Error
import GHC.IO.Exception (IOErrorType(InappropriateType))
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

#if defined(WITH_TERMINFO)
import System.Console.Terminfo as Terminfo
#endif

#if defined(mingw32_HOST_OS)
# if defined(i386_HOST_ARCH)
#  define WINDOWS_CCONV stdcall
# elif defined(x86_64_HOST_ARCH)
#  define WINDOWS_CCONV ccall
# else
#  error Unknown mingw32 arch
# endif
#endif

-- | Short-circuit 'any' with a \"monadic predicate\".
anyM :: (Monad m) => (a -> m Bool) -> [a] -> m Bool
anyM _ [] = return False
anyM p (x:xs) = do
  b <- p x
  if b
    then return True
    else anyM p xs

-- -----------------------------------------------------------------------------
-- Command-line syntax

data Flag
  = FlagUser
  | FlagGlobal
  | FlagHelp
  | FlagVersion
  | FlagConfig FilePath
  | FlagGlobalConfig FilePath
  | FlagUserConfig FilePath
  | FlagForce
  | FlagForceFiles
  | FlagMultiInstance
  | FlagExpandEnvVars
  | FlagExpandPkgroot
  | FlagNoExpandPkgroot
  | FlagSimpleOutput
  | FlagNamesOnly
  | FlagIgnoreCase
  | FlagNoUserDb
  | FlagVerbosity (Maybe String)
  | FlagUnitId
  | FlagShowUnitIds
  deriving Eq

data Verbosity = Silent | Normal | Verbose
    deriving (Show, Eq, Ord)

-- -----------------------------------------------------------------------------
-- Do the business

data Force = NoForce | ForceFiles | ForceAll | CannotForce
  deriving (Eq,Ord)

-- | Represents how a package may be specified by a user on the command line.
data PackageArg
    -- | A package identifier foo-0.1, or a glob foo-*
    = Id GlobPackageIdentifier
    -- | An installed package ID foo-0.1-HASH.  This is guaranteed to uniquely
    -- match a single entry in the package database.
    | IUId UnitId
    -- | A glob against the package name.  The first string is the literal
    -- glob, the second is a function which returns @True@ if the argument
    -- matches.
    | Substring String (String->Bool)


-- | Either an exact 'PackageIdentifier', or a glob for all packages
-- matching 'PackageName'.
data GlobPackageIdentifier
    = ExactPackageIdentifier MungedPackageId
    | GlobPackageIdentifier  MungedPackageName

displayGlobPkgId :: GlobPackageIdentifier -> String
displayGlobPkgId (ExactPackageIdentifier pid) = display pid
displayGlobPkgId (GlobPackageIdentifier pn) = display pn ++ "-*"

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
data DbModifySelector = TopOne | ContainsPkg PackageArg

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
                -> GhcPkg.DbOpenMode mode DbModifySelector
                -> Bool    -- read caches, if available
                -> Bool    -- expand vars, like ${pkgroot} and $topdir
                -> [Flag]
                -> IO (PackageDBStack,
                          -- the real package DB stack: [global,user] ++
                          -- DBs specified on the command line with -f.
                       GhcPkg.DbOpenMode mode (PackageDB mode),
                          -- which one to modify, if any
                       PackageDBStack)
                          -- the package DBs specified on the command
                          -- line, or [global,user] otherwise.  This
                          -- is used as the list of package DBs for
                          -- commands that just read the DB, such as 'list'.

getPkgDatabases verbosity mode use_cache expand_vars my_flags = do
  -- first we determine the location of the global package config.  On Windows,
  -- this is found relative to the ghc-pkg.exe binary, whereas on Unix the
  -- location is passed to the binary using the --global-package-db flag by the
  -- wrapper script.
  let err_msg = "missing --global-package-db option, location of global package database unknown\n"
  global_conf <-
     case [ f | FlagGlobalConfig f <- my_flags ] of
        [] -> do mb_dir <- getLibDir
                 case mb_dir of
                   Nothing  -> die err_msg
                   Just dir -> do
                     r <- lookForPackageDBIn dir
                     case r of
                       Nothing -> die ("Can't find package database in " ++ dir)
                       Just path -> return path
        fs -> return (last fs)

  -- The value of the $topdir variable used in some package descriptions
  -- Note that the way we calculate this is slightly different to how it
  -- is done in ghc itself. We rely on the convention that the global
  -- package db lives in ghc's libdir.
  top_dir <- absolutePath (takeDirectory global_conf)

  -- If the user database exists, and for "use_user" commands (which includes
  -- "ghc-pkg check" and all commands that modify the db) we will attempt to
  -- use the user db.
  let sys_databases = [global_conf]

  let env_stack = sys_databases

  let flag_db_names = env_stack

  -- For a "modify" command, treat all the databases as
  -- a stack, where we are modifying the top one, but it
  -- can refer to packages in databases further down the
  -- stack.

  -- -f flags on the command line add to the database
  -- stack, unless any of them are present in the stack
  -- already.
  let final_stack = filter (`notElem` env_stack)
                     [ f | FlagConfig f <- reverse my_flags ]
                     ++ env_stack

      top_db = global_conf

  (db_stack, db_to_operate_on) <- getDatabases top_dir
                                               flag_db_names final_stack top_db

  let flag_db_stack = [ db | db_name <- flag_db_names,
                        db <- db_stack, location db == db_name ]

  when (verbosity > Normal) $ do
    infoLn ("db stack: " ++ show (map location db_stack))
    F.forM_ db_to_operate_on $ \db ->
      infoLn ("modifying: " ++ (location db))
    infoLn ("flag db stack: " ++ show (map location flag_db_stack))

  return (db_stack, db_to_operate_on, flag_db_stack)
  where
    getDatabases top_dir flag_db_names
                 final_stack top_db = case mode of
      -- When we open in read only mode, we simply read all of the databases/
      GhcPkg.DbOpenReadOnly -> do
        db_stack <- mapM readDatabase final_stack
        return (db_stack, GhcPkg.DbOpenReadOnly)

      -- The only package db we open in read write mode is the one on the top of
      -- the stack.
      GhcPkg.DbOpenReadWrite TopOne -> do
        (db_stack, mto_modify) <- stateSequence Nothing
          [ \case
              to_modify@(Just _) -> (, to_modify) <$> readDatabase db_path
              Nothing -> if db_path /= top_db
                then (, Nothing) <$> readDatabase db_path
                else do
                  db <- readParseDatabase verbosity
                                          mode use_cache db_path
                    `Exception.catch` couldntOpenDbForModification db_path
                  let ro_db = db { packageDbLock = GhcPkg.DbOpenReadOnly }
                  return (ro_db, Just db)
          | db_path <- final_stack ]

        to_modify <- case mto_modify of
          Just db -> return db
          Nothing -> die "no database selected for modification"

        return (db_stack, GhcPkg.DbOpenReadWrite to_modify)

      -- The package db we open in read write mode is the first one included in
      -- flag_db_names that contains specified package. Therefore we need to
      -- open each one in read/write mode first and decide whether it's for
      -- modification based on its contents.
      GhcPkg.DbOpenReadWrite (ContainsPkg pkgarg) -> do
        (db_stack, mto_modify) <- stateSequence Nothing
          [ \case
              to_modify@(Just _) -> (, to_modify) <$> readDatabase db_path
              Nothing -> if db_path `notElem` flag_db_names
                then (, Nothing) <$> readDatabase db_path
                else do
                  let hasPkg :: PackageDB mode -> Bool
                      hasPkg = not . null . findPackage pkgarg . packages

                      openRo (e::IOError) = do
                        db <- readDatabase db_path
                        if hasPkg db
                          then couldntOpenDbForModification db_path e
                          else return (db, Nothing)

                  -- If we fail to open the database in read/write mode, we need
                  -- to check if it's for modification first before throwing an
                  -- error, so we attempt to open it in read only mode.
                  Exception.handle openRo $ do
                    db <- readParseDatabase verbosity
                                            mode use_cache db_path
                    let ro_db = db { packageDbLock = GhcPkg.DbOpenReadOnly }
                    if hasPkg db
                      then return (ro_db, Just db)
                      else do
                        -- If the database is not for modification after all,
                        -- drop the write lock as we are already finished with
                        -- the database.
                        case packageDbLock db of
                          GhcPkg.DbOpenReadWrite lock ->
                            GhcPkg.unlockPackageDb lock
                        return (ro_db, Nothing)
          | db_path <- final_stack ]

        to_modify <- case mto_modify of
          Just db -> return db
          Nothing -> cannotFindPackage pkgarg Nothing

        return (db_stack, GhcPkg.DbOpenReadWrite to_modify)
      where
        couldntOpenDbForModification :: FilePath -> IOError -> IO a
        couldntOpenDbForModification db_path e = die $ "Couldn't open database "
          ++ db_path ++ " for modification: " ++ show e

        -- Parse package db in read-only mode.
        readDatabase :: FilePath -> IO (PackageDB 'GhcPkg.DbReadOnly)
        readDatabase db_path = do
          db <- readParseDatabase verbosity
                                  GhcPkg.DbOpenReadOnly use_cache db_path
          if expand_vars
            then return $ mungePackageDBPaths top_dir db
            else return db

    stateSequence :: Monad m => s -> [s -> m (a, s)] -> m ([a], s)
    stateSequence s []     = return ([], s)
    stateSequence s (m:ms) = do
      (a, s')   <- m s
      (as, s'') <- stateSequence s' ms
      return (a : as, s'')

lookForPackageDBIn :: FilePath -> IO (Maybe FilePath)
lookForPackageDBIn dir = do
  let path_dir = dir </> "package.conf.d"
  exists_dir <- doesDirectoryExist path_dir
  if exists_dir then return (Just path_dir) else do
    let path_file = dir </> "package.conf"
    exists_file <- doesFileExist path_file
    if exists_file then return (Just path_file) else return Nothing

readParseDatabase :: forall mode t. Verbosity
                  -> GhcPkg.DbOpenMode mode t
                  -> Bool -- use cache
                  -> FilePath
                  -> IO (PackageDB mode)
readParseDatabase verbosity mode use_cache path
  | otherwise
  = do e <- tryIO $ getDirectoryContents path
       case e of
         Left err
           | ioeGetErrorType err == InappropriateType -> do
              -- We provide a limited degree of backwards compatibility for
              -- old single-file style db:
              mdb <- tryReadParseOldFileStyleDatabase verbosity
                       mode use_cache path
              case mdb of
                Just db -> return db
                Nothing ->
                  die $ "ghc no longer supports single-file style package "
                     ++ "databases (" ++ path ++ ") use 'ghc-pkg init'"
                     ++ "to create the database with the correct format."

           | otherwise -> ioError err
         Right fs
           | not use_cache -> ignore_cache (const $ return ())
           | otherwise -> do
              e_tcache <- tryIO $ getModificationTime cache
              case e_tcache of
                Left ex -> do
                  whenReportCacheErrors $
                    if isDoesNotExistError ex
                      then
                        -- It's fine if the cache is not there as long as the
                        -- database is empty.
                        when (not $ null confs) $ do
                            warn ("WARNING: cache does not exist: " ++ cache)
                            warn ("ghc will fail to read this package db. " ++
                                  recacheAdvice)
                      else do
                        warn ("WARNING: cache cannot be read: " ++ show ex)
                        warn "ghc will fail to read this package db."
                  ignore_cache (const $ return ())
                Right tcache -> do
                  when (verbosity >= Verbose) $ do
                      warn ("Timestamp " ++ show tcache ++ " for " ++ cache)
                  -- If any of the .conf files is newer than package.cache, we
                  -- assume that cache is out of date.
                  cache_outdated <- (`anyM` confs) $ \conf ->
                    (tcache <) <$> getModificationTime conf
                  if not cache_outdated
                      then do
                          when (verbosity > Normal) $
                             infoLn ("using cache: " ++ cache)
                          GhcPkg.readPackageDbForGhcPkg cache mode
                            >>= uncurry mkPackageDB
                      else do
                          whenReportCacheErrors $ do
                              warn ("WARNING: cache is out of date: " ++ cache)
                              warn ("ghc will see an old view of this " ++
                                    "package db. " ++ recacheAdvice)
                          ignore_cache $ \file -> do
                            when (verbosity >= Verbose) $ do
                              tFile <- getModificationTime file
                              let rel = case tcache `compare` tFile of
                                    LT -> " (NEWER than cache)"
                                    GT -> " (older than cache)"
                                    EQ -> " (same as cache)"
                              warn ("Timestamp " ++ show tFile
                                ++ " for " ++ file ++ rel)
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

                 -- We normally report cache errors for read-only commands,
                 -- since modify commands will usually fix the cache.
                 whenReportCacheErrors = when $ verbosity > Normal
                   || verbosity >= Normal && GhcPkg.isDbOpenReadMode mode
  where
    cache = path </> cachefilename

    recacheAdvice
      = "Use 'ghc-pkg recache' to fix."

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

mungePackageDBPaths :: FilePath -> PackageDB mode -> PackageDB mode
mungePackageDBPaths top_dir db@PackageDB { packages = pkgs } =
    db { packages = map (mungePackagePaths top_dir pkgroot) pkgs }
  where
    pkgroot = takeDirectory $ dropTrailingPathSeparator (locationAbsolute db)
    -- It so happens that for both styles of package db ("package.conf"
    -- files and "package.conf.d" dirs) the pkgroot is the parent directory
    -- ${pkgroot}/package.conf  or  ${pkgroot}/package.conf.d/

-- TODO: This code is duplicated in compiler/main/Packages.hs
mungePackagePaths :: FilePath -> FilePath
                  -> InstalledPackageInfo -> InstalledPackageInfo
-- Perform path/URL variable substitution as per the Cabal ${pkgroot} spec
-- (http://www.haskell.org/pipermail/libraries/2009-May/011772.html)
-- Paths/URLs can be relative to ${pkgroot} or ${pkgrooturl}.
-- The "pkgroot" is the directory containing the package database.
--
-- Also perform a similar substitution for the older GHC-specific
-- "$topdir" variable. The "topdir" is the location of the ghc
-- installation (obtained from the -B option).
mungePackagePaths top_dir pkgroot pkg =
    pkg {
      importDirs  = munge_paths (importDirs pkg),
      includeDirs = munge_paths (includeDirs pkg),
      libraryDirs = munge_paths (libraryDirs pkg),
      libraryDynDirs = munge_paths (libraryDynDirs pkg),
      frameworkDirs = munge_paths (frameworkDirs pkg),
      haddockInterfaces = munge_paths (haddockInterfaces pkg),
                     -- haddock-html is allowed to be either a URL or a file
      haddockHTMLs = munge_paths (munge_urls (haddockHTMLs pkg))
    }
  where
    munge_paths = map munge_path
    munge_urls  = map munge_url

    munge_path p
      | Just p' <- stripVarPrefix "${pkgroot}" p = pkgroot ++ p'
      | Just p' <- stripVarPrefix "$topdir"    p = top_dir ++ p'
      | otherwise                                = p

    munge_url p
      | Just p' <- stripVarPrefix "${pkgrooturl}" p = toUrlPath pkgroot p'
      | Just p' <- stripVarPrefix "$httptopdir"   p = toUrlPath top_dir p'
      | otherwise                                   = p

    toUrlPath r p = "file:///"
                 -- URLs always use posix style '/' separators:
                 ++ FilePath.Posix.joinPath
                        (r : -- We need to drop a leading "/" or "\\"
                             -- if there is one:
                             dropWhile (all isPathSeparator)
                                       (FilePath.splitDirectories p))

    -- We could drop the separator here, and then use </> above. However,
    -- by leaving it in and using ++ we keep the same path separator
    -- rather than letting FilePath change it to use \ as the separator
    stripVarPrefix var path = case stripPrefix var path of
                              Just [] -> Just []
                              Just cs@(c : _) | isPathSeparator c -> Just cs
                              _ -> Nothing


-- -----------------------------------------------------------------------------
-- Workaround for old single-file style package dbs

-- Single-file style package dbs have been deprecated for some time, but
-- it turns out that Cabal was using them in one place. So this code is for a
-- workaround to allow older Cabal versions to use this newer ghc.

-- We check if the file db contains just "[]" and if so, we look for a new
-- dir-style db in path.d/, ie in a dir next to the given file.
-- We cannot just replace the file with a new dir style since Cabal still
-- assumes it's a file and tries to overwrite with 'writeFile'.

-- ghc itself also cooperates in this workaround

tryReadParseOldFileStyleDatabase :: Verbosity
                                 -> GhcPkg.DbOpenMode mode t -> Bool -> FilePath
                                 -> IO (Maybe (PackageDB mode))
tryReadParseOldFileStyleDatabase verbosity
                                 mode use_cache path = do
  -- assumes we've already established that path exists and is not a dir
  content <- readFile path `catchIO` \_ -> return ""
  if take 2 content == "[]"
    then do
      path_abs <- absolutePath path
      let path_dir = adjustOldDatabasePath path
      warn $ "Warning: ignoring old file-style db and trying " ++ path_dir
      direxists <- doesDirectoryExist path_dir
      if direxists
        then do
          db <- readParseDatabase verbosity mode use_cache path_dir
          -- but pretend it was at the original location
          return $ Just db {
              location         = path,
              locationAbsolute = path_abs
            }
         else do
           lock <- F.forM mode $ \_ -> do
             createDirectoryIfMissing True path_dir
             GhcPkg.lockPackageDb $ path_dir </> cachefilename
           return $ Just PackageDB {
               location         = path,
               locationAbsolute = path_abs,
               packageDbLock    = lock,
               packages         = []
             }

    -- if the path is not a file, or is not an empty db then we fail
    else return Nothing

adjustOldFileStylePackageDB :: PackageDB mode -> IO (PackageDB mode)
adjustOldFileStylePackageDB db = do
  -- assumes we have not yet established if it's an old style or not
  mcontent <- liftM Just (readFile (location db)) `catchIO` \_ -> return Nothing
  case fmap (take 2) mcontent of
    -- it is an old style and empty db, so look for a dir kind in location.d/
    Just "[]" -> return db {
        location         = adjustOldDatabasePath $ location db,
        locationAbsolute = adjustOldDatabasePath $ locationAbsolute db
      }
    -- it is old style but not empty, we have to bail
    Just  _   -> die $ "ghc no longer supports single-file style package "
                    ++ "databases (" ++ location db ++ ") use 'ghc-pkg init'"
                    ++ "to create the database with the correct format."
    -- probably not old style, carry on as normal
    Nothing   -> return db

adjustOldDatabasePath :: FilePath -> FilePath
adjustOldDatabasePath = (<.> "d")

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
  db'' <- adjustOldFileStylePackageDB db'
  createDirectoryIfMissing True (location db'')
  changeDBDir verbosity cmds db'' db_stack

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


recache :: Verbosity -> [Flag] -> IO ()
recache verbosity my_flags = do
  (_db_stack, GhcPkg.DbOpenReadWrite db_to_operate_on, _flag_dbs) <-
    getPkgDatabases verbosity (GhcPkg.DbOpenReadWrite TopOne)
      False{-no cache-} False{-expand vars-} my_flags
  changeDB verbosity [] db_to_operate_on _db_stack

findPackage :: PackageArg -> [InstalledPackageInfo] -> [InstalledPackageInfo]
findPackage pkgarg pkgs = filter (pkgarg `matchesPkg`) pkgs

cannotFindPackage :: PackageArg -> Maybe (PackageDB mode) -> IO a
cannotFindPackage pkgarg mdb = die $ "cannot find package " ++ pkg_msg pkgarg
  ++ maybe "" (\db -> " in " ++ location db) mdb
  where
    pkg_msg (Id pkgid)           = displayGlobPkgId pkgid
    pkg_msg (IUId ipid)          = display ipid
    pkg_msg (Substring pkgpat _) = "matching " ++ pkgpat

matches :: GlobPackageIdentifier -> MungedPackageId -> Bool
GlobPackageIdentifier pn `matches` pid'
  = (pn == mungedName pid')
ExactPackageIdentifier pid `matches` pid'
  = mungedName pid == mungedName pid' &&
    (mungedVersion pid == mungedVersion pid' || mungedVersion pid == nullVersion)

matchesPkg :: PackageArg -> InstalledPackageInfo -> Bool
(Id pid)        `matchesPkg` pkg = pid `matches` mungedId pkg
(IUId ipid)     `matchesPkg` pkg = ipid == installedUnitId pkg
(Substring _ m) `matchesPkg` pkg = m (display (mungedId pkg))

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

-----------------------------------------
-- Cut and pasted from ghc/compiler/main/SysTools

getLibDir :: IO (Maybe String)

#if defined(mingw32_HOST_OS) || defined(darwin_HOST_OS) || defined(linux_HOST_OS)
getLibDir = Just . (\p -> p </> "lib") . takeDirectory . takeDirectory <$> getExecutablePath
#else
getLibDir = return Nothing
#endif

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

