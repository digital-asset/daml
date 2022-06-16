-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module CheckReleases (check_releases) where

import Github

import qualified Control.Concurrent.Async
import qualified Control.Concurrent.QSem
import Control.Exception.Safe
import qualified Control.Monad as Control
import Control.Retry
import Data.Conduit (runConduit, (.|))
import Data.Conduit.Combinators (sinkHandle)
import qualified Data.Foldable
import Data.Maybe (isJust)
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Client.Conduit (bodyReaderSource)
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.URI
import qualified System.Directory as Directory
import qualified System.Exit as Exit
import System.FilePath.Posix ((</>))
import qualified System.IO.Extra as IO
import qualified System.Process as System

shell :: String -> IO String
shell cmd = System.readCreateProcess (System.shell cmd) ""

shell_ :: String -> IO ()
shell_ cmd = Control.void $ shell cmd

download_assets :: FilePath -> GitHubRelease -> IO ()
download_assets tmp release = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    tokens <- Control.Concurrent.QSem.newQSem 20
    Control.Concurrent.Async.forConcurrently_ (map uri $ assets release) $ \url ->
        bracket_
          (Control.Concurrent.QSem.waitQSem tokens)
          (Control.Concurrent.QSem.signalQSem tokens)
          (do
              req <- add_github_contact_header <$> HTTP.parseRequest (show url)
              recovering
                retryPolicy
                [retryHandler]
                (\_ -> downloadFile req manager url)
          )
  where -- Retry for 5 minutes total, doubling delay starting with 20ms
        retryPolicy = limitRetriesByCumulativeDelay (5 * 60 * 1000 * 1000) (exponentialBackoff (20 * 1000))
        retryHandler status =
          logRetries
            (\e -> pure $ isJust (fromException @IOException e) || isJust (fromException @HTTP.HttpException e)) -- Don’t try to be clever, just retry
            (\shouldRetry err status -> IO.hPutStrLn IO.stderr $ defaultLogMsg shouldRetry err status)
            status
        downloadFile req manager url = HTTP.withResponse req manager $ \resp -> do
            IO.withBinaryFile (tmp </> (last $ Network.URI.pathSegments url)) IO.WriteMode $ \handle ->
                runConduit $ bodyReaderSource (HTTP.responseBody resp) .| sinkHandle handle

verify_signatures :: FilePath -> FilePath -> String -> IO ()
verify_signatures bash_lib tmp version_tag = do
    System.callCommand $ unlines ["bash -c '",
        "set -euo pipefail",
        "source \"" <> bash_lib <> "\"",
        "shopt -s extglob", -- enable !() pattern: things that _don't_ match
        "cd \"" <> tmp <> "\"",
        "for f in !(*.asc); do",
            "p=" <> version_tag <> "/github/$f",
            "if ! test -f $f.asc; then",
                "echo $p: no signature file",
            "else",
                "LOG=$(mktemp)",
                "if gpg_verify $f.asc >$LOG 2>&1; then",
                    "echo $p: signature matches",
                "else",
                    "echo $p: signature does not match",
                    "echo Full gpg output:",
                    "cat $LOG",
                    "exit 2",
                "fi",
            "fi",
       "done",
       "'"]

does_backup_exist :: FilePath -> IO Bool
does_backup_exist path = do
    out <- shell $ unlines ["bash -c '",
        "set -euo pipefail",
        "if gsutil ls \"" <> path <> "\" >/dev/null; then",
            "echo True",
        "else",
            "echo False",
        "fi",
        "'"]
    return $ read out

gcs_cp :: FilePath -> FilePath -> IO ()
gcs_cp from to = do
    shell_ $ unlines ["bash -c '",
        "set -euo pipefail",
        "gsutil cp \"" <> from <> "\" \"" <> to <> "\" &>/dev/null",
        "'"]

check_files_match :: String -> String -> IO Bool
check_files_match f1 f2 = do
    (exitCode, stdout, stderr) <- System.readProcessWithExitCode "diff" [f1, f2] ""
    case exitCode of
      Exit.ExitSuccess -> return True
      Exit.ExitFailure 1 -> return False
      Exit.ExitFailure _ -> fail $ "Diff failed.\n" ++ "STDOUT:\n" ++ stdout ++ "\nSTDERR:\n" ++ stderr

check_releases :: String -> Maybe Int -> IO ()
check_releases bash_lib max_releases = do
    releases' <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    let releases = case max_releases of
                     Nothing -> releases'
                     Just n -> take n releases'
    Data.Foldable.for_ releases (\release -> recoverAll retryPolicy $ \_ -> do
        let v = show $ tag release
        putStrLn $ "Checking release " <> v <> " ..."
        IO.withTempDir $ \temp_dir -> do
            download_assets temp_dir release
            verify_signatures bash_lib temp_dir v
            files <- Directory.listDirectory temp_dir
            Control.Concurrent.Async.forConcurrently_ files $ \f -> do
                let local_github = temp_dir </> f
                let local_gcp = temp_dir </> f <> ".gcp"
                let remote_gcp = "gs://daml-data/releases/" <> v <> "/github/" <> f
                exists <- does_backup_exist remote_gcp
                if exists then do
                    gcs_cp remote_gcp local_gcp
                    check_files_match local_github local_gcp >>= \case
                        True -> putStrLn $ f <> " matches GCS backup."
                        False -> fail $ f <> " does not match GCS backup."
                else do
                    fail $ remote_gcp <> " does not exist. Aborting.")
  where
     -- Retry for 10 minutes total, delay of 1s
     retryPolicy = limitRetriesByCumulativeDelay (10 * 60 * 1000 * 1000) (constantDelay 1000_000)
