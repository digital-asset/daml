-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module WithOracle (withOracle) where

import Control.Exception.Safe
import qualified Data.UUID as UUID
import Data.UUID.V4
import Data.Text (Text)
import Data.Maybe
import qualified Data.Text as T
import Network.Socket
import System.Environment.Blank
import System.IO.Extra
import System.Process

-- This is loosely modelled after com.daml.testing.oracle.OracleAround.
-- We make this a separate executable since it should only
-- depend on released artifacts so we cannot easily use sandbox as a library.

data OracleConnection = OracleConnection
  { port :: PortNumber
  , adminUsername :: Text
  , adminPassword :: Text
  , dockerPath :: Text
  }

withOracle :: (Text -> IO a) -> IO a
withOracle f = do
    connection <- getOracleConnection
    -- Note: we do not check whether the generated user name already exists
    username <- randomUserName
    bracket_ (createUser connection username) (dropUser connection username) $ f (jdbcUrl connection username)

-- Takes a UUID and modifies it so that it conforms to Oracle constraints for usernames and passwords
randomUserName :: IO Text
randomUserName = do fmap (T.take 30 . T.cons 'u' . T.replace "-" "" . T.pack . UUID.toString) nextRandom

-- Note: currently we only start Oracle databases through docker.
-- It's easier to shell out to sqlplus than dealing with Oracle-compatible SQL libraries.
executeStatement :: OracleConnection -> Text -> IO ()
executeStatement OracleConnection { .. } str = do
    -- Note: need to login as 'sys', not as the user provided through the ORACLE_USERNAME environment variable
    let cmd = "echo '" <> str <> "' | " <> "sqlplus sys/" <> adminPassword <> "@ORCLPDB1 as sysdba"
    -- hPutStrLn stderr $ T.unpack $ "Executing command " <> cmd <> " through " <> dockerPath
    callProcess (T.unpack dockerPath)
        [ "exec"
        , "oracle"
        , "bash"
        , "-c"
        , T.unpack cmd
        ]

jdbcUrl :: OracleConnection -> Text -> Text
jdbcUrl OracleConnection { .. } username =
    "jdbc:oracle:thin:" <>
    username <>
    "/" <>
    username <>
    "@localhost:" <>
    T.pack (show port) <>
    "/ORCLPDB1"

getOracleConnection :: IO OracleConnection
getOracleConnection = do
    mbPort <- getEnv "ORACLE_PORT"
    mbUser <- getEnv "ORACLE_USERNAME"
    mbPwd <- getEnv "ORACLE_PWD"
    mbDocker <- getEnv "ORACLE_DOCKER_PATH"
    let port = read $ fromMaybe (error "ORACLE_PORT environment variable not set") mbPort
    let username = T.pack $ fromMaybe (error "ORACLE_USERNAME environment variable not set") mbUser
    let password = T.pack $ fromMaybe (error "ORACLE_PWD environment variable not set") mbPwd
    let docker = T.pack $ fromMaybe (error "ORACLE_DOCKER_PATH environment variable not set") mbDocker
    pure $ OracleConnection port username password docker

createUser :: OracleConnection -> Text -> IO ()
createUser connection name = do
    hPutStrLn stderr $ T.unpack $ "Creating Oracle user " <> name
    executeStatement connection $ "create user " <> name <> " identified by " <> name <> ";"
    executeStatement connection $ "grant connect, resource to " <> name <> ";"
    executeStatement connection $ "grant create table, create materialized view, create view, create procedure, create sequence, create type to " <> name <> ";"
    executeStatement connection $ "alter user " <> name <> " quota unlimited on users;"
    executeStatement connection $ "GRANT EXECUTE ON SYS.DBMS_LOCK TO " <> name <> ";"
    executeStatement connection $ "GRANT SELECT ON V_$MYSTAT TO " <> name <> ";"
    executeStatement connection $ "GRANT SELECT ON V_$LOCK TO " <> name <> ";"

dropUser :: OracleConnection -> Text -> IO ()
dropUser connection name = do
    hPutStrLn stderr $ T.unpack $ "Dropping Oracle user " <> name
    executeStatement connection ("drop user " <> name <> " cascade;")
