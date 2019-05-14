-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Cache
    ( cacheLatestSdkVersion
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Util
import DAML.Project.Config
import Control.Exception.Safe
import Control.Monad.Extra
import Data.Either.Extra
import Data.Maybe
import Data.String
import Data.Time.Clock
import qualified Data.Yaml as Y
import System.Directory
import System.FilePath
import System.IO.Extra

newtype CacheKey = CacheKey String
    deriving (Show, Eq, Ord, IsString)

newtype CacheTimeout = CacheTimeout NominalDiffTime
    deriving (Show, Eq, Ord, Y.FromJSON)

data UpdateCheck
    = UpdateCheckNever
    | UpdateCheckEvery CacheTimeout
    deriving (Show, Eq, Ord)

instance Y.FromJSON UpdateCheck where
    parseJSON (Y.String "never") = pure UpdateCheckNever
    parseJSON y = UpdateCheckEvery <$> Y.parseJSON y

cacheLatestSdkVersion
    :: DamlPath
    -> IO (Maybe SdkVersion)
    -> IO (Maybe SdkVersion)
cacheLatestSdkVersion damlPath getVersion = do
    damlConfigE <- tryConfig $ readDamlConfig damlPath
    let updateCheckM = join $ eitherToMaybe (queryDamlConfig ["update-check"] =<< damlConfigE)
        defaultUpdateCheck = UpdateCheckEvery (CacheTimeout 86400)
    case fromMaybe defaultUpdateCheck updateCheckM of
        UpdateCheckNever -> pure Nothing
        UpdateCheckEvery timeout ->
            cacheWith "latest-sdk-version" timeout
                serializeMaybeSdkVersion deserializeMaybeSdkVersion
                damlPath getVersion

serializeMaybeSdkVersion :: Maybe SdkVersion -> String
serializeMaybeSdkVersion = \case
    Nothing -> ""
    Just v -> versionToString v

deserializeMaybeSdkVersion :: String -> Maybe (Maybe SdkVersion)
deserializeMaybeSdkVersion = \case
    "" -> Nothing
    v  -> fmap Just . eitherToMaybe $ parseVersion (pack v)

cacheDirPath :: DamlPath -> FilePath
cacheDirPath (DamlPath damlPath) = damlPath </> "cache"

cacheFilePath :: DamlPath -> CacheKey -> FilePath
cacheFilePath damlPath (CacheKey key) = cacheDirPath damlPath </> key

cacheWith
    :: CacheKey
    -> CacheTimeout
    -> (t -> String)
    -> (String -> Maybe t)
    -> DamlPath
    -> IO t
    -> IO t
cacheWith key (CacheTimeout timeout) serialize deserialize damlPath getValue = do
    let path = cacheFilePath damlPath key

    modTimeE <- tryIO (getModificationTime path)
    curTimeE <- tryIO getCurrentTime
    let useCachedE = liftM2 (\mt ct -> diffUTCTime ct mt < timeout) modTimeE curTimeE
        useCached = fromRight False useCachedE

    valueMEM <- whenMaybe useCached $ tryIO $ do
        valueStr <- readFileUTF8 path
        pure (deserialize valueStr)

    case valueMEM of
        Just (Right (Just value)) -> pure value
        _ -> do
            value <- getValue
            void . tryIO $ do
                createDirectoryIfMissing True (cacheDirPath damlPath)
                writeFileUTF8 path (serialize value)
            pure value

