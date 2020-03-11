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
import qualified Data.CaseInsensitive as CI
import qualified Data.HashMap.Strict as H
import qualified Data.List as List
import qualified Data.List.Extra as List
import qualified Data.List.Split as Split
import qualified Data.Maybe as Maybe
import qualified Data.Ord
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Data.Traversable as Traversable
import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as TLS
import qualified Network.HTTP.Types.Status as Status
import qualified System.Directory as Directory
import qualified System.Exit as Exit
import qualified System.IO.Extra as Temp
import qualified System.Process as System
import qualified Text.Regex.TDFA as Regex

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
        cmd = "nix-build nix -A tools -A ci-cached"
        h n = do
            (exit, out, err) <- shell_exit_code cmd
            case (exit, n) of
              (Exit.ExitSuccess, _) -> return ()
              (_, 0) -> die cmd exit out err
              _ | "unexpected end-of-file" `List.isInfixOf` err -> h (n - 1)
              _ -> die cmd exit out err

http_get :: JSON.FromJSON a => String -> IO (a, H.HashMap String String)
http_get url = do
    manager <- HTTP.newManager TLS.tlsManagerSettings
    request' <- HTTP.parseRequest url
    -- Be polite
    let request = request' { HTTP.requestHeaders = [("User-Agent", "DAML cron (team-daml-language@digitalasset.com)")] }
    response <- HTTP.httpLbs request manager
    let body = JSON.decode $ HTTP.responseBody response
    let status = Status.statusCode $ HTTP.responseStatus response
    case (status, body) of
      (200, Just body) -> return (body, response & HTTP.responseHeaders & map (\(n, v) -> (n & CI.foldedCase & BS.toString, BS.toString v)) & H.fromList)
      _ -> Exit.die $ unlines ["GET \"" <> url <> "\" returned status code " <> show status <> ".",
                               show $ HTTP.responseBody response]

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
        documented_versions <- Maybe.catMaybes <$> Traversable.for versions (\version -> do
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
                    return $ Just version
                else do
                    putStrLn "  Too old to rebuild and no existing version. Skipping."
                    return Nothing
            else if to_v version < to_v "0.13.45"
            then do
                -- Versions prior to 0.13.45 do have a checksum file, and
                -- should be buildable with the Maven cherry-pick (see `build`
                -- function below), but their build is not reproducible
                -- (includes date of build) and therefore the checksum file is
                -- useless
                if old_version_exists
                then do
                    putStrLn "  Found. No reliable checksum; copying over and hoping for the best..."
                    copy (old </> version) $ new </> version
                    return $ Just version
                else do
                    putStrLn "  Not found. Building..."
                    build version new
                    return $ Just version
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
                    return $ Just version
                else do
                    putStrLn "  Check failed. Rebuilding..."
                    build version new
                    return $ Just version
            else do
                putStrLn "  Not found. Building..."
                build version new
                return $ Just version)
        putStrLn $ "Copying latest (" <> latest <> ") to top-level..."
        copy (new </> latest </> "*") (new <> "/")
        putStrLn "Creating versions.json..."
        create_versions_json documented_versions new
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
                build_helper version path
            else if to_v version < to_v "0.13.55"
            then do
                build_helper version path
            -- Starting after 0.13.54, we have changed the way in which we
            -- trigger releases. Rather than releasing the current commit by
            -- changing the VERSION file, we now mark an existing commit as the
            -- source code for a release by changing the LATEST file. However,
            -- release notes still need to be taken from the release commit
            -- (i.e. the one that changes the LATEST file, not the one being
            -- pointed to).
            else do
                -- The release-triggering commit does not have a tag, so we
                -- need to find it by walking through the git history of the
                -- LATEST file.
                sha <- find_commit_for_version version
                Control.Exception.bracket
                    (shell_ $ "git checkout " <> sha <> " -- docs/source/support/release-notes.rst")
                    (\_ -> shell_ "git reset --hard")
                    (\_ -> build_helper version path)
        build_helper version path = do
            robustly_download_nix_packages
            shell_ "bazel build //docs:docs"
            shell_ $ "mkdir -p  " <> path </> version
            shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> path </> version
            checksums <- shell $ "cd " <> path </> version <> "; find . -type f -exec sha256sum {} \\;"
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

find_commit_for_version :: String -> IO String
find_commit_for_version version = do
    release_commits <- lines <$> shell "git log --format=%H origin/master -- LATEST"
    ver_sha <- init <$> (shell $ "git rev-parse v" <> version)
    let expected = ver_sha <> " " <> version
    matching <- Maybe.catMaybes <$> Traversable.for release_commits (\sha -> do
        latest <- init <$> (shell $ "git show " <> sha <> ":LATEST")
        if latest == expected
        then return $ Just sha
        else return Nothing)
    case matching of
      [sha] -> return sha
      _ -> error $ "Expected single commit to match release " <> version <> ", but instead found: " <> show matching

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

data GitHubVersion = GitHubVersion { prerelease :: Bool, tag_name :: String, notes :: String, published_at :: String } deriving Show
instance JSON.FromJSON GitHubVersion where
    parseJSON = JSON.withObject "GitHubVersion" $ \v -> GitHubVersion
        <$> v JSON..: Text.pack "prerelease"
        <*> v JSON..: Text.pack "tag_name"
        <*> v JSON..:? Text.pack "body" JSON..!= ""
        <*> v JSON..: Text.pack "published_at"

name :: GitHubVersion -> String
name gh = tail $ tag_name gh

fetch_gh_paginated :: JSON.FromJSON a => String -> IO [a]
fetch_gh_paginated url = do
    (resp_0, headers) <- http_get url
    case parse_next =<< H.lookup "link" headers of
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

fetch_gh_versions :: IO (Set.Set Version, GitHubVersion)
fetch_gh_versions = do
    resp <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
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
    let prev_latest = List.maximum $ Set.toList s3_versions_before
    if prev_latest == to_v (name gh_latest)
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
