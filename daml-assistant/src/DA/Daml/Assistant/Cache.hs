-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Cache
    ( cacheAvailableSdkVersions
    , CacheAge (..)
    , UseCache (..)
    , cacheWith
    , loadFromCacheWith
    , saveToCacheWith
    , CacheTimeout (..)
    , serializeVersions
    , deserializeVersions
    ) where

import Control.Exception.Safe
import Control.Monad.Extra
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Project.Config
import Data.Either.Extra
import Data.Maybe
import Data.String
import Data.Time.Clock
import Data.Yaml qualified as Y
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

data UseCache
  = UseCache
      { overrideTimeout :: Maybe CacheTimeout
      , cachePath :: CachePath
      , damlPath :: DamlPath
      }
  | DontUseCache
  deriving (Show, Eq)

cacheAvailableSdkVersions
    :: UseCache
    -> IO [SdkVersion]
    -> IO ([SdkVersion], CacheAge)
cacheAvailableSdkVersions DontUseCache getVersions = (, Fresh) <$> getVersions
cacheAvailableSdkVersions UseCache { overrideTimeout, cachePath, damlPath } getVersions = do
    damlConfigE <- tryConfig $ readDamlConfig damlPath
    let configUpdateCheckM = join $ eitherToMaybe (queryDamlConfig ["update-check"] =<< damlConfigE)
        (neverRefresh, timeout)
            -- A few different things can override the timeout behaviour in
            -- daml-config.yaml, chiefly `daml install latest` and `daml version --force-reload yes`
          | Just timeout <- overrideTimeout = (False, timeout)
          | Just updateCheck <- configUpdateCheckM =
              case updateCheck of
                -- When UpdateCheckNever, still refresh the cache if it doesn't exist
                UpdateCheckNever -> (True, CacheTimeout (1000 * 365 * 86400))
                UpdateCheckEvery timeout -> (False, timeout)
          | otherwise = (False, CacheTimeout 86400)
    mVal <- cacheWith cachePath versionsKey timeout
        serializeVersions deserializeVersions
        getVersions
        neverRefresh
    pure (fromMaybe ([], Stale) mVal)

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
    -> Bool
    -> IO (Maybe (t, CacheAge))
cacheWith cachePath key timeout serialize deserialize getFresh neverRefresh = do
    valueAgeM <- loadFromCacheWith cachePath key timeout deserialize
    if neverRefresh
       then pure valueAgeM
       else Just <$> case valueAgeM of
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
    deriving (Show, Eq, Ord)

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
    valueM <- eitherToMaybe <$> tryIO (readFileUTF8' path)
    pure $ fmap (, age) valueM

-- | Read value from cache, including its age, with deserialization function.
loadFromCacheWith :: CachePath -> CacheKey -> CacheTimeout -> Deserialize t -> IO (Maybe (t, CacheAge))
loadFromCacheWith cachePath key timeout deserialize = do
    valueAgeM <- loadFromCache cachePath key timeout
    pure $ do
        (valueStr, age) <- valueAgeM
        value <- deserialize valueStr
        Just (value, age)
