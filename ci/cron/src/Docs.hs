-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | The docs cronjob runs through the following steps
--
-- 1. Download the list of available releases from GH releases.
-- 2. Download the list of available releases from S3 based on the contents of
--    `/versions.json` and `/snapshots.json`.
-- 3. For each version that is in GH but not in S3 run through those steps:
--    3.1. Check if the release has already been uploaded under `/$version/`. If so do nothing.
--         We hit this for the split release process for 2.0 and newer.
--    3.2. If not, build it in the Daml repository and upload.
--         We hit this for the non-split release process.
-- 4. At this point, all releases have been uploaded. What remains to be done is
--    updating the top-level release to the latest stable release that has
--    been marked stable on GH.
-- 5. Finally we update the versions.json and snapshots.json on S3 to match GH.
--
-- This assumes that:
--
-- 1. The assembly repo uploads a release to `/version/` but leaves
--    moving it to the top-level to this cron job.
-- 2. The assembly repo uploads to S3 before creating a GH release.
module Docs (docs, sdkDocOpts, damlOnSqlDocOpts) where

import Control.Exception.Safe
import qualified Control.Monad as Control
import qualified Control.Monad.Extra
import qualified Data.Aeson as JSON
import qualified Data.Aeson.Key as JSON
import qualified Data.Aeson.KeyMap as JSON
import qualified Data.ByteString.Lazy.UTF8 as LBS
import Data.Either (isRight)
import qualified Data.Foldable
import Data.Function ((&))
import qualified Data.List
import Data.Maybe (fromMaybe)
import qualified Data.Ord
import qualified Data.Set as Set
import qualified System.Directory.Extra as Directory
import qualified System.FilePath as FilePath
import qualified System.Environment
import qualified System.Exit as Exit
import System.FilePath.Posix ((</>))
import qualified System.IO.Extra as IO
import qualified System.Process as System

import Github

die :: String -> Int -> String -> String -> IO a
die cmd exit out err =
    fail $ unlines ["Subprocess:",
                    cmd,
                    "failed with exit code " <> show exit <> "; output:",
                    "---",
                    out,
                    "---",
                    "err:",
                    "---",
                    err,
                    "---"]

shell :: String -> IO String
shell cmd = System.readCreateProcess (System.shell cmd) ""

proc :: [String] -> IO String
proc args = System.readCreateProcess (System.proc (head args) (tail args)) ""

shell_ :: String -> IO ()
shell_ cmd = do
    Control.void $ shell cmd

proc_ :: [String] -> IO ()
proc_ args = Control.void $ do
    print args
    proc args

shell_env_ :: [(String, String)] -> String -> IO ()
shell_env_ env cmd = do
    parent_env <- System.Environment.getEnvironment
    Control.void $ System.readCreateProcess ((System.shell cmd) {System.env = Just (parent_env ++ env)}) ""

robustly_download_nix_packages :: IO ()
robustly_download_nix_packages = do
    h (10 :: Integer)
    where
        cmd = "nix-build nix -A tools -A ci-cached --no-out-link"
        h n = do
            (exit, out, err) <- System.readCreateProcessWithExitCode (System.shell cmd) ""
            case (exit, n) of
              (Exit.ExitSuccess, _) -> return ()
              (Exit.ExitFailure exit, 0) -> die cmd exit out err
              _ | "unexpected end-of-file" `Data.List.isInfixOf` err -> h (n - 1)
              (Exit.ExitFailure exit, _) -> die cmd exit out err

s3Path :: DocOptions -> FilePath -> String
s3Path DocOptions{s3Subdir} file =
    "s3://docs-daml-com" </> fromMaybe "" s3Subdir </> file

-- We use a check for a file to check if the docs for a given version exist or not.
-- This is technically slightly racy if the upload happens concurrently and we end up with a partial upload
-- but the window seems small enough to make this acceptable.
doesVersionExist :: DocOptions -> Version -> IO Bool
doesVersionExist opts version = do
    r <- tryIO $ IO.withTempFile $ \file -> proc_ ["aws", "s3", "cp", s3Path opts (show version </> fileToCheck opts), file]
    pure (isRight r)

build_and_push :: DocOptions -> FilePath -> [Version] -> IO ()
build_and_push opts@DocOptions{build} temp versions = do
    restore_sha $ do
        Data.Foldable.for_ versions (\version -> do
            putStrLn $ "Check if version  " <> show version <> " exists ..."
            exists <- doesVersionExist opts version
            if exists
                then putStrLn $ "Version " <> show version <> " already exists (split-release)"
                else do
                    putStrLn $ "Building " <> show version <> "..."
                    build temp version
                    putStrLn $ "Pushing " <> show version <> " to S3 (as subfolder)..."
                    push version
            putStrLn "Done.")
    where
        restore_sha io =
            bracket (init <$> shell "git symbolic-ref --short HEAD 2>/dev/null || git rev-parse HEAD")
                    (\cur_sha -> proc_ ["git", "checkout", cur_sha])
                    (const io)
        push version =
            proc_ ["aws", "s3", "cp",
                   temp </> show version,
                   s3Path opts (show version),
                   "--recursive", "--acl", "public-read"]

fetch_if_missing :: DocOptions -> FilePath -> Version -> IO ()
fetch_if_missing opts temp v = do
    missing <- not <$> Directory.doesDirectoryExist (temp </> show v)
    if missing then do
        -- We hit this for all split releases as well as non-split releases that
        -- have been built before being marked stable.
        putStrLn $ "Downloading " <> show v <> "..."
        proc_ ["aws", "s3", "cp", s3Path opts (show v), temp </> show v, "--recursive"]
        putStrLn "Done."
    else do
        putStrLn $ show v <> " already present."

update_s3 :: DocOptions -> FilePath -> Versions -> IO ()
update_s3 opts temp vs = do
    let displayed = dropdown vs
    let hidden = Data.List.sortOn Data.Ord.Down $ Set.toList $ all_versions vs `Set.difference` Set.fromList displayed
    -- The assistant depends on these three files, they are not just internal
    -- to the docs process.
    push (versions_json displayed) "versions.json"
    push (versions_json hidden) "snapshots.json"
    Control.Monad.Extra.whenJust (top vs) $ \latest -> push (show latest) "latest"
    putStrLn "Done."
    where
        -- Not going through Aeson because it represents JSON objects as
        -- unordered maps, and here order matters.
        versions_json vs = vs
                           & map ((\s -> "\"" <> s <> "\": \"" <> s <> "\"") . show)
                           & Data.List.intercalate ", "
                           & \s -> "{" <> s <> "}"
        push text name = do
            writeFile (temp </> name) text
            proc_ ["aws", "s3", "cp", temp </> name, s3Path opts name, "--acl", "public-read"]

update_top_level :: DocOptions -> FilePath -> Version -> Maybe Version -> IO ()
update_top_level opts temp new mayOld = do
    new_files <- Set.fromList <$> files_under new
    old_files <- case mayOld of
        Nothing -> pure Set.empty
        Just old -> Set.fromList <$> files_under old
    let to_delete = Set.toList $ old_files `Set.difference` new_files
    Control.when (not $ null to_delete) $ do
        putStrLn $ "Deleting top-level files: " <> show to_delete
        Data.Foldable.for_ to_delete (\f -> do
            proc_ ["aws", "s3", "rm", s3Path opts f])
        putStrLn "Done."
    putStrLn $ "Pushing " <> show new <> " to top-level..."
    let root = s3Path opts "" <> "/"
    proc_ ["aws", "s3", "cp", temp </> show new, root, "--recursive", "--acl", "public-read"]
    putStrLn "Done."
  where files_under folder = do
          let root = temp </> show folder
          full_paths <- Directory.listFilesRecursive root
          return $ map (FilePath.makeRelative root) full_paths

reset_cloudfront :: IO ()
reset_cloudfront = do
    putStrLn "Refreshing CloudFront cache..."
    shell_ "aws cloudfront create-invalidation --distribution-id E1U753I56ERH55 --paths '/*'"

fetch_s3_versions :: DocOptions -> IO Versions
fetch_s3_versions opts = do
    -- On the first run, this will fail so treat it like an empty file.
    dropdown <- fetch "versions.json" False `catchIO` (\_ -> pure [])
    hidden <- fetch "snapshots.json" True `catchIO` (\_ -> pure [])
    return $ versions $ dropdown <> hidden
    where fetch file prerelease = do
              temp <- shell "mktemp"
              proc_ ["aws", "s3", "cp", s3Path opts file, temp]
              s3_raw <- proc ["cat", temp]
              let type_annotated_value :: Maybe JSON.Object
                  type_annotated_value = JSON.decode $ LBS.fromString s3_raw
              case type_annotated_value of
                  Just s3_json -> return $ map (\s -> GitHubRelease prerelease (version $ JSON.toText s) []) $ JSON.keys s3_json
                  Nothing -> Exit.die "Failed to get versions from s3"

data DocOptions = DocOptions
  { s3Subdir :: Maybe FilePath
  , includedVersion :: Version -> Bool
    -- Exclusive minimum version bound for which we build docs
  , build :: FilePath -> Version -> IO ()
  , fileToCheck :: FilePath
  }

sdkDocOpts :: DocOptions
sdkDocOpts = DocOptions
  { s3Subdir = Nothing
    -- versions prior to 0.13.10 cannot be built anymore and are not present in
    -- the repo.
  , includedVersion = \v -> v >= version "0.13.10"
  , build = \temp version -> do
        proc_ ["git", "checkout", "v" <> show version]
        robustly_download_nix_packages
        shell_env_ [("DAML_SDK_RELEASE_VERSION", show version)] "bazel build //docs:docs"
        proc_ ["mkdir", "-p", temp </> show version]
        proc_ ["tar", "xzf", "bazel-bin/docs/html.tar.gz", "--strip-components=1", "-C", temp </> show version]
  , fileToCheck = "versions.json"
  }

damlOnSqlDocOpts :: DocOptions
damlOnSqlDocOpts = DocOptions
  { s3Subdir = Just "daml-driver-for-postgresql"
  , includedVersion = \v -> v > version "1.8.0-snapshot.20201201.5776.0.4b91f2a6"
        && v <= version "2.0.0-snapshot.20220209.9212.0.b7fc9f57"
  , build = \temp version -> do
        proc_ ["git", "checkout", "v" <> show version]
        robustly_download_nix_packages
        shell_env_ [("DAML_SDK_RELEASE_VERSION", show version)] "bazel build //ledger/daml-on-sql:docs"
        proc_ ["mkdir", "-p", temp </> show version]
        proc_ ["tar", "xzf", "bazel-bin/ledger/daml-on-sql/html.tar.gz", "--strip-components=1", "-C", temp </> show version]
  , fileToCheck = "index.html"
  }

docs :: DocOptions -> IO ()
docs opts@DocOptions{includedVersion} = do
    putStrLn "Checking for new version..."
    gh_versions <- fetch_gh_versions includedVersion
    s3_versions <- fetch_s3_versions opts
    if s3_versions == gh_versions
    then do
        putStrLn "Versions match, nothing to do."
    else do
        -- We may have added versions. We need to build and push them.
        let added = Set.toList $ all_versions gh_versions `Set.difference` all_versions s3_versions
        -- post-2.0 versions are built and pushed by the docs.daml.com repo
        let to_build = filter (version "2.0.0" >) added
        IO.withTempDir $ \temp_dir -> do
            putStrLn $ "Versions to build: " <> show added
            build_and_push opts temp_dir to_build
            -- If there is no version on GH, we don’t have to do anything.
            Control.Monad.Extra.whenJust (top gh_versions) $ \gh_top ->
              Control.when (Just gh_top /= top s3_versions) $ do
                putStrLn $ "Updating top-level version from " <> (show $ top s3_versions) <> " to " <> (show $ top gh_versions)
                fetch_if_missing opts temp_dir gh_top
                Control.Monad.Extra.whenJust (top s3_versions) (fetch_if_missing opts temp_dir)
                update_top_level opts temp_dir gh_top (top s3_versions)
            putStrLn "Updating versions.json, snapshots.json, latest..."
            update_s3 opts temp_dir gh_versions
        reset_cloudfront
