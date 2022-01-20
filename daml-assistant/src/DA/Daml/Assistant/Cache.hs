-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Cache
    ( cacheAvailableSdkVersions
    , saveAvailableSdkVersions
    , CacheAge (..)
    ) where

import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Project.Config
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

type Serialize t = t -> String
type Deserialize t = String -> Maybe t

versionsKey :: CacheKey
versionsKey = "versions.txt"

saveAvailableSdkVersions
    :: CachePath
    -> [SdkVersion]
    -> IO ()
saveAvailableSdkVersions cachePath =
    saveToCacheWith cachePath versionsKey serializeVersions

cacheAvailableSdkVersions
    :: DamlPath
    -> CachePath
    -> IO [SdkVersion]
    -> IO ([SdkVersion], CacheAge)
cacheAvailableSdkVersions damlPath cachePath getVersions = do
    damlConfigE <- tryConfig $ readDamlConfig damlPath
    let updateCheckM = join $ eitherToMaybe (queryDamlConfig ["update-check"] =<< damlConfigE)
        defaultUpdateCheck = UpdateCheckEvery (CacheTimeout 86400)
    case fromMaybe defaultUpdateCheck updateCheckM of
        UpdateCheckNever -> do
            valueAgeM <- loadFromCacheWith cachePath versionsKey (CacheTimeout 0) deserializeVersions
            pure $ fromMaybe ([], Stale) valueAgeM

        UpdateCheckEvery timeout ->
            cacheWith cachePath versionsKey timeout
                serializeVersions deserializeVersions
                getVersions

serializeVersions :: Serialize [SdkVersion]
serializeVersions =
    unlines . map versionToString

deserializeVersions :: Deserialize [SdkVersion]
deserializeVersions =
    Just . mapMaybe (eitherToMaybe . parseVersion . pack) . lines

cacheFilePath :: CachePath -> CacheKey -> FilePath
cacheFilePath cachePath (CacheKey key) = unwrapCachePath cachePath </> key

cacheWith
    :: CachePath
    -> CacheKey
    -> CacheTimeout
    -> Serialize t
    -> Deserialize t
    -> IO t
    -> IO (t, CacheAge)
cacheWith cachePath key timeout serialize deserialize getFresh = do
    valueAgeM <- loadFromCacheWith cachePath key timeout deserialize
    case valueAgeM of
        Just (value, Fresh) -> pure (value, Fresh)
        Just (value, Stale) -> do
            valueE <- tryAny getFresh
            case valueE of
                Left _ -> pure (value, Stale)
                Right value' -> do
                    saveToCacheWith cachePath key serialize value'
                    pure (value', Fresh)
        Nothing -> do
            value <- getFresh
            saveToCacheWith cachePath key serialize value
            pure (value, Fresh)

-- | A representation of the age of a cache value. We only care if the value is stale or fresh.
data CacheAge
    = Stale
    | Fresh

-- | Save value to cache. Never raises an exception.
saveToCache :: CachePath -> CacheKey -> String -> IO ()
saveToCache cachePath key value =
    void . tryIO $ do
        let dirPath = unwrapCachePath cachePath
            filePath = cacheFilePath cachePath key
        createDirectoryIfMissing True dirPath
        writeFileUTF8 filePath value

-- | Save value to cache, with serialization function.
saveToCacheWith :: CachePath -> CacheKey -> Serialize t -> t -> IO ()
saveToCacheWith cachePath key serialize value = saveToCache cachePath key (serialize value)

-- | Read value from cache, including its age. Never raises an exception.
loadFromCache :: CachePath -> CacheKey -> CacheTimeout -> IO (Maybe (String, CacheAge))
loadFromCache cachePath key (CacheTimeout timeout) = do
    let path = cacheFilePath cachePath key
    modTimeE <- tryIO (getModificationTime path)
    curTimeE <- tryIO getCurrentTime
    let isStaleE = liftM2 (\mt ct -> diffUTCTime ct mt >= timeout) modTimeE curTimeE
        isStale  = fromRight True isStaleE
        age  = if isStale then Stale else Fresh
    valueM <- eitherToMaybe <$> tryIO (readFileUTF8 path)
    pure $ fmap (, age) valueM

-- | Read value from cache, including its age, with deserialization function.
loadFromCacheWith :: CachePath -> CacheKey -> CacheTimeout -> Deserialize t -> IO (Maybe (t, CacheAge))
loadFromCacheWith cachePath key timeout deserialize = do
    valueAgeM <- loadFromCache cachePath key timeout
    pure $ do
        (valueStr, age) <- valueAgeM
        value <- deserialize valueStr
        Just (value, age)
