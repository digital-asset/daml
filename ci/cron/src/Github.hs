-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Github
  ( Asset(..)
  , GitHubRelease(..)
  , Version(..)
  , Versions(..)
  , add_github_contact_header
  , version
  , versions
  , fetch_gh_paginated
  , fetch_gh_versions
  ) where

import Data.Aeson
import qualified Data.ByteString.UTF8 as BS
import qualified Data.CaseInsensitive as CI
import Data.Function ((&))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HMS
import Data.List (sortOn)
import qualified Data.List.Split as Split
import Data.Ord (Down(..))
import qualified Data.SemVer as SemVer
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import Network.HTTP.Types.Status (statusCode)
import Network.URI
import Safe (headMay)
import qualified System.Exit as Exit
import qualified Text.Regex.TDFA as Regex

data Asset = Asset { uri :: Network.URI.URI }
instance FromJSON Asset where
    parseJSON = withObject "Asset" $ \v -> Asset
        <$> (do
            Just url <- Network.URI.parseURI <$> v .: "browser_download_url"
            return url)

data GitHubRelease = GitHubRelease { prerelease :: Bool, tag :: Version, assets :: [Asset] }
instance FromJSON GitHubRelease where
    parseJSON = withObject "GitHubRelease" $ \v -> GitHubRelease
        <$> (v .: "prerelease")
        <*> (version . Text.tail <$> v .: "tag_name")
        <*> (v .: "assets")

data Version = Version SemVer.Version
    deriving (Ord, Eq)
instance Show Version where
    show (Version v) = SemVer.toString v

version :: Text.Text -> Version
version t = Version $ (\case Left s -> (error s); Right v -> v) $ SemVer.fromText t

data Versions = Versions { top :: Maybe Version, all_versions :: Set Version, dropdown :: [Version] }
    deriving Eq

versions :: [GitHubRelease] -> Versions
versions vs =
    let all_versions = Set.fromList $ map tag vs
        dropdown = vs
                   & filter (not . prerelease)
                   & map tag
                   & filter (>= version "1.0.0")
                   & sortOn Down
        top = headMay dropdown
    in Versions {..}

fetch_gh_versions :: (Version -> Bool) -> IO Versions
fetch_gh_versions pred = do
    response <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    return $ versions $ filter (\v -> pred (tag v)) response

fetch_gh_paginated :: String -> IO [GitHubRelease]
fetch_gh_paginated url = do
    (resp_0, headers) <- http_get url
    case parse_next =<< HMS.lookup "link" headers of
      Nothing -> return resp_0
      Just next -> do
          rest <- fetch_gh_paginated next
          return $ resp_0 ++ rest
    where parse_next header = lookup "next" $ map parse_link $ split header
          split h = Split.splitOn ", " h
          link_regex = "<(.*)>; rel=\"(.*)\"" :: String
          parse_link l =
              let typed_regex :: (String, String, String, [String])
                  typed_regex = l Regex.=~ link_regex
              in
              case typed_regex of
                (_, _, _, [url, rel]) -> (rel, url)
                _ -> error $ "Assumption violated: link header entry did not match regex.\nEntry: " <> l

http_get :: FromJSON a => String -> IO (a, HashMap String String)
http_get url = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request <- add_github_contact_header <$> HTTP.parseRequest url
    response <- HTTP.httpLbs request manager
    let body = decode $ HTTP.responseBody response
    let status = statusCode $ HTTP.responseStatus response
    case (status, body) of
      (200, Just body) -> return (body, response & HTTP.responseHeaders & map (\(n, v) -> (n & CI.foldedCase & BS.toString, BS.toString v)) & HMS.fromList)
      _ -> Exit.die $ unlines ["GET \"" <> url <> "\" returned status code " <> show status <> ".",
                               show $ HTTP.responseBody response]

add_github_contact_header :: HTTP.Request -> HTTP.Request
add_github_contact_header req =
    req { HTTP.requestHeaders = ("User-Agent", "Daml cron (team-daml-app-runtime@digitalasset.com)") : HTTP.requestHeaders req }

