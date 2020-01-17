-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.Function ((&))
import System.FilePath.Posix ((</>))

import qualified Control.Exception
import qualified Control.Monad as Control
import qualified Data.Aeson as JSON
import qualified Data.ByteString.UTF8 as BS
import qualified Data.ByteString.Lazy.UTF8 as LBS
import qualified Data.Foldable as Foldable
import qualified Data.HashMap.Strict as H
import qualified Data.List as List
import qualified Data.List.Extra as List
import qualified Data.List.Split as Split
import qualified Data.Ord
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.HTTP.Types.Header as Header
import qualified Network.HTTP.Types.Status as Status
import qualified System.Directory as Directory
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
    -- DEBUG
    putStrLn $ "About to POST to " <> url <> "\n" <> show body
    response <- HTTP.httpLbs request manager
    let status = Status.statusCode $ HTTP.responseStatus response
    -- DEBUG
    putStrLn $ "Received " <> show status <> " with:\n" <> show (HTTP.responseBody response)
    case status `quot` 100 of
      2 -> return $ HTTP.responseBody response
      _ -> Exit.die $ "POST " <> url <> " failed with " <> show status <> "."

newtype Version = Version (Int, Int, Int)
  deriving (Eq, Ord)

instance Show Version where
    show (Version (a, b, c)) = show a <> "." <> show b <> "." <> show c

to_v :: String -> Version
to_v s = case map read $ Split.splitOn "." s of
    [major, minor, patch] -> Version (major, minor, patch)
    _ -> error $ "Invalid data, needs manual repair. Got this for a version string: " <> s

build_docs_folder :: String -> [String] -> String -> IO String
build_docs_folder path versions latest = do
    restore_sha $ do
        let old = path </> "old"
        let new = path </> "new"
        shell_ $ "mkdir -p " <> new
        shell_ $ "mkdir -p " <> old
        download_existing_site_from_s3 old
        Foldable.for_ versions $ \version -> do
            putStrLn $ "Building " <> version <> "..."
            putStrLn "  Checking for existing folder..."
            old_version_exists <- exists $ old </> version
            if to_v version < to_v "0.13.36"
            then do
                -- Maven has stopped accepting http requests and now requires
                -- https. We have a patch for 0.13.36 and above, which has been
                -- merged between 0.13.43 and 0.13.44.
                if old_version_exists
                then do
                    putStrLn "  Found. Too old to rebuild, copying over..."
                    copy (old </> version) $ new </> version
                else
                    putStrLn "  Too old to rebuild and no existing version. Skipping."
            else if old_version_exists
            then do
                -- Note: this checks for upload errors; this is NOT in any way
                -- a protection against tampering at the s3 level as we get the
                -- checksums from the s3 bucket.
                putStrLn "  Found. Checking integrity..."
                checksums_match <- checksums $ old </> version
                if checksums_match
                then do
                    putStrLn "  Checks, reusing existing."
                    copy (old </> version) $ new </> version
                else do
                    putStrLn "  Check failed. Rebuilding..."
                    build version new
            else do
                putStrLn "  Not found. Building..."
                build version new
            putStrLn $ "Done " <> version <> "."
        putStrLn $ "Copying latest (" <> latest <> ") to top-level..."
        copy (new </> latest </> "*") (new <> "/")
        putStrLn "Creating versions.json..."
        create_versions_json versions new
        return new
    where
        restore_sha io =
            Control.Exception.bracket (init <$> shell "git rev-parse HEAD")
                                      (\cur_sha -> shell_ $ "git checkout " <> cur_sha)
                                      (const io)
        download_existing_site_from_s3 path = do
            shell_ $ "mkdir -p " <> path
            shell_ $ "aws s3 sync s3://docs-daml-com/ " <> path
        exists dir = Directory.doesDirectoryExist dir
        checksums path = do
            let cmd = "cd " <> path <> "; sha256sum -c checksum"
            (code, _, _) <- shell_exit_code cmd
            case code of
                Exit.ExitSuccess -> return True
                _ -> return False
        copy from to = do
            shell_ $ "cp -r " <> from <> " " <> to
        build version path = do
            shell_ $ "git checkout v" <> version
            -- Maven does not accept http connections anymore; this patches the
            -- scala rules for Bazel to use https instead. This is not needed
            -- after 0.13.43.
            if to_v version < to_v "0.13.44"
            then do
                shell_ "git -c user.name=CI -c user.email=CI@example.com cherry-pick 0c4f9d7f92c4f2f7e2a75a0d85db02e20cbb497b"
            else pure ()
            robustly_download_nix_packages
            shell_ "bazel build //docs:docs"
            shell_ $ "mkdir -p  " <> path </> version
            shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> path </> version
            -- when syncing with s3 later on we exclude .doctrees and
            -- .buildinfo, so we need to do the same here
            checksums <- shell $ "cd " <> path </> version <> "; find . -type f | grep -v '\\.doctrees' | grep -v '\\.buildinfo' | xargs sha256sum"
            writeFile (path </> version </> "checksum") checksums
        create_versions_json versions path = do
            -- Not going through Aeson because it represents JSON objects as
            -- unordered maps, and here order matters.
            let versions_json = versions
                                & List.sortOn (Data.Ord.Down . to_v)
                                & map (\s -> "\"" <> s <> "\": \"" <> s <> "\"")
                                & List.intercalate ", "
                                & \s -> "{" <> s <> "}"
            writeFile (path </> "versions.json") versions_json

fetch_s3_versions :: IO (Set.Set Version)
fetch_s3_versions = do
    temp <- shell "mktemp"
    shell_ $ "aws s3 cp s3://docs-daml-com/versions.json " <> temp
    s3_raw <- shell $ "cat " <> temp
    let type_annotated_value :: Maybe JSON.Object
        type_annotated_value = JSON.decode $ LBS.fromString s3_raw
    case type_annotated_value of
      Just s3_json -> return $ Set.fromList $ map (to_v . Text.unpack) $ H.keys s3_json
      Nothing -> Exit.die "Failed to get versions from s3"

push_to_s3 :: String -> IO ()
push_to_s3 doc_folder = do
    putStrLn "Pushing new versions file first..."
    shell_ $ "aws s3 cp " <> doc_folder </> "versions.json s3://docs-daml-com/versions.json --acl public-read"
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
    -- DEBUG
    putStrLn $ "About to read date " <> (show $ published_at latest) <> " to epoch ms"
    date <- (read <$> (<> "000")) . init <$> (shell $ "date -d " <> published_at latest <> " +%s")
    -- DEBUG
    putStrLn $ "date read as " <> show date
    let summary = "Release notes for version " <> name latest <> "."
    -- DEBUG
    putStrLn "Fetching hs token from env"
    token <- Env.getEnv "HUBSPOT_TOKEN"
    submit_blog <- http_post ("https://api.hubapi.com/content/api/v2/blog-posts?hapikey=" <> token)
                             [("Content-Type", "application/json")]
                             $ JSON.encode $ SubmitBlog { body = LBS.toString desc,
                                                          date,
                                                          summary,
                                                          version = name latest }
    case JSON.decode submit_blog of
      Nothing -> do
          -- DEBUG
          putStrLn "About to die because blog id could not be parsed"
          Exit.die $ "No blog id from HubSpot: \n" <> LBS.toString submit_blog
      Just BlogId { blog_id } -> do
          -- DEBUG
          putStrLn $ "Parsed blog ID as " <> show blog_id
          _ <- http_post ("https://api.hubapi.com/content/api/v2/blog-posts/" <> show blog_id </> "publish-action?hapikey=" <> token)
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

fetch_gh_versions :: IO (Set.Set Version, GitHubVersion)
fetch_gh_versions = do
    resp <- http_get "https://api.github.com/repos/digital-asset/daml/releases"
    let releases = filter (not . prerelease) resp
    let versions = Set.fromList $ map (to_v . name) releases
    let latest = List.maximumOn (to_v . name) releases
    return (versions, latest)

main :: IO ()
main = do
    robustly_download_nix_packages
    putStrLn "Checking for new version..."
    (gh_versions, gh_latest) <- fetch_gh_versions
    s3_versions_before <- fetch_s3_versions
    if s3_versions_before == gh_versions
    then do
        putStrLn "No new version found, skipping."
        Exit.exitSuccess
    else do
        Temp.withTempDir $ \temp_dir -> do
            putStrLn "Building docs listing"
            docs_folder <- build_docs_folder temp_dir (map show $ Set.toList gh_versions) $ name gh_latest
            putStrLn "Done building docs bundle. Checking versions again to avoid race condition..."
            s3_versions_after <- fetch_s3_versions
            if s3_versions_after == gh_versions
            then do
                putStrLn "No more new version, another process must have pushed already."
                Exit.exitSuccess
            else do
                push_to_s3 docs_folder
                let prev_latest = List.maximum $ Set.toList s3_versions_before
                if to_v (name gh_latest) > prev_latest
                then do
                    putStrLn "New version detected, telling HubSpot"
                    tell_hubspot gh_latest
                else
                    putStrLn "Not a new release, not telling HubSpot."
