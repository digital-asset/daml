-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.Function ((&))

import qualified Control.Monad as Control
import qualified Data.Aeson as JSON
import qualified Data.ByteString.UTF8 as BS
import qualified Data.ByteString.Lazy.UTF8 as LBS
import qualified Data.Foldable as Foldable
import qualified Data.HashMap.Strict as H
import qualified Data.List as List
import qualified Data.List.Extra as List
import qualified Data.List.Utils as List
import qualified Data.List.Split as Split
import qualified Data.Ord as Ord
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.HTTP.Types.Header as Header
import qualified Network.HTTP.Types.Status as Status
import qualified System.Environment as Env
import qualified System.Exit as Exit
import qualified System.IO.Extra as Temp
import qualified System.Process as System

shell_exit_code :: String -> IO (Exit.ExitCode, String, String)
shell_exit_code cmd = do
    System.readCreateProcessWithExitCode (System.shell cmd) ""

die :: String -> Exit.ExitCode -> String -> String -> IO a
die cmd (Exit.ExitFailure exit) out err =
    Exit.die $ unlines ["Subprocess:",
                         cmd,
                        "failed with exit code " <> show exit <> "; output:",
                        "---",
                        out,
                        "---",
                        "err:",
                        "---",
                        err,
                        "---"]
die _ _ _ _ = Exit.die "Type system too weak."


shell :: String -> IO String
shell cmd = do
    (exit, out, err) <- shell_exit_code cmd
    if exit == Exit.ExitSuccess
    then return out
    else die cmd exit out err

shell_ :: String -> IO ()
shell_ cmd = do
    Control.void $ shell cmd

robustly_download_nix_packages :: IO ()
robustly_download_nix_packages = do
    h (10 :: Integer)
    where
        cmd = "nix-build nix -A tools -A cached"
        h n = do
            (exit, out, err) <- shell_exit_code cmd
            case (exit, n) of
              (Exit.ExitSuccess, _) -> return ()
              (_, 0) -> die cmd exit out err
              _ | "unexpected end-of-file" `List.isInfixOf` err -> h (n - 1)
              _ -> die cmd exit out err

http_get :: JSON.FromJSON a => String -> IO a
http_get url = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request' <- HTTP.parseRequest url
    -- Be polite
    let request = request' { HTTP.requestHeaders = [("User-Agent", "DAML cron (team-daml-language@digitalasset.com)")] }
    response <- HTTP.httpLbs request manager
    let body = JSON.decode $ HTTP.responseBody response
    let status = Status.statusCode $ HTTP.responseStatus response
    case (status, body) of
      (200, Just body) -> return body
      _ -> Exit.die $ unlines ["GET \"" <> url <> "\" returned status code " <> show status <> ".",
                               show $ HTTP.responseBody response]

http_post :: String -> Header.RequestHeaders -> LBS.ByteString -> IO LBS.ByteString
http_post url headers body = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request' <- HTTP.parseRequest url
    -- Be polite
    let request = request' { HTTP.requestHeaders = ("User-Agent", "DAML cron (team-daml-language@digitalasset.com)") : headers,
                             HTTP.method = "POST",
                             HTTP.requestBody = HTTP.RequestBodyBS $ BS.fromString $ LBS.toString body }
    response <- HTTP.httpLbs request manager
    let status = Status.statusCode $ HTTP.responseStatus response
    case status `quot` 100 of
      2 -> return $ HTTP.responseBody response
      _ -> Exit.die $ "POST " <> url <> " failed with " <> show status <> "."

github_versions :: [GitHubVersion] -> Set.Set String
github_versions vs = Set.fromList $ map name vs

remove_prereleases :: [GitHubVersion] -> [GitHubVersion]
remove_prereleases = filter (\v -> not (prerelease v))

docs_versions :: H.HashMap String String -> Set.Set String
docs_versions json =
    Set.fromList $ H.keys json

newtype Version = Version (Int, Int, Int)
  deriving (Eq, Ord)

to_v :: String -> Version
to_v s = case map read $ Split.splitOn "." s of
    [major, minor, patch] -> Version (major, minor, patch)
    _ -> error $ "Invalid data, needs manual repair. Got this for a version string: " <> s

build_docs_folder :: String -> [String] -> IO ()
build_docs_folder path versions = do
    out <- shell "git rev-parse HEAD"
    let cur_sha = init out -- remove ending newline
    shell_ $ "mkdir -p " <> path
    let latest = head versions
    putStrLn $ "Building latest docs: " <> latest
    shell_ $ "git checkout v" <> latest
    robustly_download_nix_packages
    shell_ "bazel build //docs:docs"
    shell_ $ "tar xzf bazel-genfiles/docs/html.tar.gz --strip-components=1 -C " <> path
    -- Not going through Aeson because it represents JSON objects as unordered
    -- maps, and here order matters.
    let versions_json = versions
                        & map (\s -> "\"" <> s <> "\": \"" <> s <> "\"")
                        & List.join ", "
                        & \s -> "{" <> s <> "}"
    writeFile (path <> "/versions.json") versions_json
    shell_ $ "mkdir -p  " <> path <> "/" <> latest
    shell_ $ "tar xzf bazel-genfiles/docs/html.tar.gz --strip-components=1 -C " <> path <> "/" <> latest
    Foldable.for_ (tail versions) $ \version -> do
        putStrLn $ "Building older docs: " <> version
        shell_ $ "git checkout v" <> version
        robustly_download_nix_packages
        shell_ "bazel build //docs:docs"
        shell_ $ "mkdir -p  " <> path <> "/" <> version
        shell_ $ "tar xzf bazel-genfiles/docs/html.tar.gz --strip-components=1 -C" <> path <> "/" <> version
    shell_ $ "git checkout " <> cur_sha

check_s3_versions :: Set.Set String -> IO Bool
check_s3_versions gh_versions = do
    temp <- shell "mktemp"
    shell_ $ "aws s3 cp s3://docs-daml-com/versions.json " <> temp
    s3_raw <- shell $ "cat " <> temp
    case JSON.decode $ LBS.fromString s3_raw of
      Just s3_json -> return $ docs_versions s3_json == gh_versions
      Nothing -> Exit.die "Failed to get versions from s3"

push_to_s3 :: String -> IO ()
push_to_s3 doc_folder = do
    putStrLn "Pushing new versions file first..."
    shell_ $ "aws s3 cp " <> doc_folder <> "/versions.json s3://docs-daml-com/versions.json --acl public-read"
    putStrLn "Pushing to S3 bucket..."
    shell_ $ "aws s3 sync " <> doc_folder
             <> " s3://docs-daml-com/"
             <> " --delete"
             <> " --acl public-read"
             <> " --exclude '*.doctrees/*'"
             <> " --exclude '*.buildinfo'"
    putStrLn "Refreshing CloudFront cache..."
    shell_ $ "aws cloudfront create-invalidation"
             <> " --distribution-id E1U753I56ERH55"
             <> " --paths '/*'"

data BlogId = BlogId { blog_id :: Integer } deriving Show
instance JSON.FromJSON BlogId where
    parseJSON = JSON.withObject "BlogId" $ \v -> BlogId
        <$> v JSON..: Text.pack "id"

data SubmitBlog = SubmitBlog {
                      body :: String,
                      date :: Integer,
                      summary :: String,
                      version :: String
                  } deriving Show
instance JSON.ToJSON SubmitBlog where
    toJSON SubmitBlog{body, date, summary, version} =
    -- content_group_id and blog_author_id reference existing items in HubSpot
        JSON.object ["name" JSON..= ("Release of DAML SDK " <> version),
                     "post_body" JSON..= body,
                     "content_group_id" JSON..= (11411412838 :: Integer),
                     "publish_date" JSON..= date,
                     "post_summary" JSON..= summary,
                     "slug" JSON..= version,
                     "blog_author_id" JSON..= (11513309969 :: Integer),
                     "meta_description" JSON..= summary]

tell_hubspot :: GitHubVersion -> IO ()
tell_hubspot latest = do
    putStrLn $ "Publishing "<> name latest <> " to Hubspot..."
    desc <- http_post "https://api.github.com/markdown/raw" [("Content-Type", "text/plain")] (LBS.fromString $ notes latest)
    date <- (read <$> (<> "000")) . init <$> (shell $ "date -d " <> published_at latest <> " +%s")
    let summary = "Release notes for version " <> name latest <> "."
    token <- Env.getEnv "HUBSPOT_TOKEN"
    submit_blog <- http_post ("https://api.hubapi.com/content/api/v2/blog-posts?hapikey=" <> token)
                             [("Content-Type", "application/json")]
                             $ JSON.encode $ SubmitBlog { body = LBS.toString desc,
                                                          date,
                                                          summary,
                                                          version = name latest }
    case JSON.decode submit_blog of
      Nothing -> Exit.die $ "No blog id from HubSpot: \n" <> LBS.toString submit_blog
      Just BlogId { blog_id } -> do
          _ <- http_post ("https://api.hubapi.com/content/api/v2/blog-posts/" <> show blog_id <> "/publish-action?hapikey=" <> token)
                         [("Content-Type", "application/json")]
                         (JSON.encode $ JSON.object [("action", "schedule-publish")])
          return ()

data GitHubVersion = GitHubVersion { prerelease :: Bool, tag_name :: String, notes :: String, published_at :: String } deriving Show
instance JSON.FromJSON GitHubVersion where
    parseJSON = JSON.withObject "GitHubVersion" $ \v -> GitHubVersion
        <$> v JSON..: Text.pack "prerelease"
        <*> v JSON..: Text.pack "tag_name"
        <*> v JSON..:? Text.pack "body" JSON..!= ""
        <*> v JSON..: Text.pack "published_at"

name :: GitHubVersion -> String
name gh = tail $ tag_name gh

main :: IO ()
main = do
    robustly_download_nix_packages
    putStrLn "Checking for new version..."
    gh_resp <- remove_prereleases <$> http_get "https://api.github.com/repos/digital-asset/daml/releases"
    docs_resp <- docs_versions <$> http_get "https://docs.daml.com/versions.json"
    if github_versions gh_resp == docs_resp
    then do
        putStrLn "No new version found, skipping."
        Exit.exitSuccess
    else do
        Temp.withTempDir $ \docs_folder -> do
            putStrLn "Building docs listing"
            build_docs_folder docs_folder $ List.sortOn (Ord.Down . to_v) $ map name gh_resp
            putStrLn "Done building docs bundle. Checking versions again to avoid race condition..."
            s3_matches <- check_s3_versions (github_versions gh_resp)
            if s3_matches
            then do
                putStrLn "No more new version, another process must have pushed already."
                Exit.exitSuccess
            else do
                push_to_s3 docs_folder
                let gh_latest = List.maximumOn (to_v . name) gh_resp
                let docs_latest = List.maximumOn to_v $ Set.toList docs_resp
                if to_v (name gh_latest) > to_v docs_latest
                then do
                    putStrLn "New version detected, telling HubSpot"
                    tell_hubspot gh_latest
                else
                    putStrLn "Not a new release, not telling HubSpot."
