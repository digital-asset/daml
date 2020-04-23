-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import qualified Bazel.Runfiles as Runfiles
import Control.Exception
import Data.Text (Text)
import qualified Data.Text as T
import System.Directory.Extra
import System.Environment
import System.FilePath
import System.IO.Extra
import System.Process

-- This is modelled after com.daml.testing.postgresql.PostgresAround.
-- We make this a separate executable since it should only
-- depend on released artifacts so we cannot easily use sandbox as a library.

postgresConfig :: Int -> Text
postgresConfig port = T.unlines
    [ "unix_socket_directories = '/tmp'"
    , "shared_buffers = 12MB"
    , "fsync = off"
    , "synchronous_commit = off"
    , "full_page_writes = off"
    , "log_min_duration_statement = 0"
    , "log_connections = on"
    , "listen_addresses = 'localhost'"
    , "port = " <> T.pack (show port)
    ]

dbUser :: Text
dbUser = "test"

dbName :: Text
dbName = "test"

-- | Hardcoded for now since those tests are exclusive anyway.
dbPort :: Int
dbPort = 4321

jdbcUrl :: Text
jdbcUrl = "jdbc:postgresql://localhost:" <> T.pack (show dbPort) <> "/" <> dbName <> "?user=" <> dbName

-- Launch a temporary postgres instance and provide a jdbc url to access that database.
withPostgres :: (Text -> IO a) -> IO a
withPostgres f =
    withTempDir $ \tmpDir -> do
    let dataDir = tmpDir </> "data"
    let logFile = tmpDir </> "postgresql.log"
    createDirectory dataDir
    runfiles <- Runfiles.create
    -- For reasons I don’t entirely understand, `locateRunfiles` does not
    -- work here. Hardcoding the paths to external/... matches what we do in
    -- com.daml.testing.postgresql.Tool.
    callProcess
        "external/postgresql_nix/bin/initdb"
        ["--username=" <> T.unpack dbUser, dataDir]
    writeFileUTF8 (dataDir </> "postgresql.conf") (T.unpack $ postgresConfig dbPort)
    bracket_ (startPostgres dataDir logFile) (stopPostgres dataDir) $ do
      createDatabase
      f jdbcUrl
  where startPostgres dataDir logFile =
            callProcess
                "external/postgresql_nix/bin/pg_ctl"
                ["-o", "-F -p " <> show dbPort, "-w", "-D", dataDir, "-l", logFile, "start"]
        stopPostgres dataDir =
            callProcess
                "external/postgresql_nix/bin/pg_ctl"
                ["-w", "-D", dataDir, "-m", "immediate", "stop"]
        createDatabase =
            callProcess
                "external/postgresql_nix/bin/createdb"
                ["-h", "localhost", "-U", T.unpack dbUser, "-p", show dbPort, T.unpack dbName]
main :: IO ()
main = do
    (version : args) <- getArgs
    withPostgres $ \jdbcUrl ->
        callProcess ("external/daml-sdk-" <> version </> "daml") (args <> ["--jdbcurl=" <> T.unpack jdbcUrl])

