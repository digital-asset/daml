-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import qualified Data.Foldable
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
import qualified System.IO.Extra as IO
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

robustly_download_nix_packages :: String -> IO ()
robustly_download_nix_packages v = do
    h (10 :: Integer)
    where
        cmd = if to_v v < to_v "0.13.55"
              then "nix-build nix -A tools -A cached"
              else "nix-build nix -A tools -A ci-cached"
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

newtype Version = Version (Int, Int, Int, Maybe String)
  deriving (Eq, Ord)

instance Show Version where
    show (Version (a, b, c, q)) = show a <> "." <> show b <> "." <> show c <> Maybe.maybe "" (\qual -> "-" <> qual) q

to_v :: String -> Version
to_v s = case Split.splitOn "-" s of
    [prefix, qualifier] -> let (major, minor, patch) = parse_stable prefix
                           in Version (major, minor, patch, Just qualifier)
    [stable] -> let (major, minor, patch) = parse_stable stable
                in Version (major, minor, patch, Nothing)
    _ -> error $ "Invalid data, needs manual repair. Got this for a version string: " <> s
    where parse_stable s = case map read $ Split.splitOn "." s of
              [major, minor, patch] -> (major, minor, patch)
              _ -> error $ "Invalid data, needs manual repair. Got this for a version string: " <> s

build_docs_folder :: String -> [GitHubVersion] -> String -> IO String
build_docs_folder path versions current = do
    restore_sha $ do
        latest_release_notes_sha <- init <$> shell "git log -n1 --format=%H HEAD -- LATEST"
        let old = path </> "old"
        let new = path </> "new"
        shell_ $ "mkdir -p " <> new
        shell_ $ "mkdir -p " <> old
        download_existing_site_from_s3 old
        documented_versions <- Maybe.catMaybes <$> Traversable.for versions (\gh_version -> do
            let version = name gh_version
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
                    return $ Just (gh_version, False)
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
                    return $ Just (gh_version, False)
                else do
                    putStrLn "  Not found. Building..."
                    build version new latest_release_notes_sha
                    return $ Just (gh_version, True)
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
                    return $ Just (gh_version, False)
                else do
                    putStrLn "  Check failed. Rebuilding..."
                    build version new latest_release_notes_sha
                    return $ Just (gh_version, True)
            else do
                putStrLn "  Not found. Building..."
                build version new latest_release_notes_sha
                return $ Just (gh_version, True))
        putStrLn $ "Copying current (" <> current <> ") to top-level..."
        copy (new </> current </> "*") (new <> "/")
        putStrLn "Creating versions.json..."
        let (releases, snapshots) = List.partition (not . prerelease . fst) documented_versions
        create_versions_json (map fst releases) (new </> "versions.json")
        create_versions_json (map fst snapshots) (new </> "snapshots.json")
        case filter snd documented_versions of
          ((newly_built,_):_) -> do
              putStrLn $ "Copying release notes from " <> name newly_built <> " to all other versions..."
              let p v = new </> name v </> "support" </> "release-notes.html"
              let top_level_release_notes = new </> "support" </> "release-notes.html"
              shell_ $ "cp " <> p newly_built <> " " <> top_level_release_notes
              Data.Foldable.for_ documented_versions $ \(gh_version, _) -> do
                  shell_ $ "cp " <> top_level_release_notes <> " " <> p gh_version
          _ -> do
              putStrLn "No version built, so no release page copied."
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
            let cmd = "cd " <> path <> "; sed -i ':support/release-notes.html:d' checksum; sha256sum -c checksum"
            (code, _, _) <- shell_exit_code cmd
            case code of
                Exit.ExitSuccess -> return True
                _ -> return False
        copy from to = do
            shell_ $ "cp -r " <> from <> " " <> to
        build version path latest_sha = do
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
            -- source code for a release by changing the LATEST file. This
            -- raises the question of the release notes: as we tag a commit
            -- from the past, and keep the changelog outside of the worktree
            -- (in commit messages), that means that commit cannot contain its
            -- own release notes. We have decided to resolve that conundrum by
            -- always including the release notes from the most recent release
            -- in all releases.
            else do
                Control.Exception.bracket_
                    (shell_ $ "git checkout " <> latest_sha <> " -- docs/source/support/release-notes.rst")
                    (shell_ "git reset --hard")
                    (build_helper version path)
        build_helper version path = do
            robustly_download_nix_packages version
            shell_ $ "DAML_SDK_RELEASE_VERSION=" <> version <> " bazel build //docs:docs"
            shell_ $ "mkdir -p  " <> path </> version
            shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> path </> version
            checksums <- shell $ "cd " <> path </> version <> "; find . -type f -exec sha256sum {} \\; | grep -v 'support/release-notes.html'"
            writeFile (path </> version </> "checksum") checksums
        create_versions_json versions path = do
            -- Not going through Aeson because it represents JSON objects as
            -- unordered maps, and here order matters.
            let versions_json = versions
                                & map name
                                & List.sortOn (Data.Ord.Down . to_v)
                                & map (\s -> "\"" <> s <> "\": \"" <> s <> "\"")
                                & List.intercalate ", "
                                & \s -> "{" <> s <> "}"
            writeFile path versions_json

fetch_s3_versions :: IO (Set.Set Version, Set.Set Version)
fetch_s3_versions = do
    releases <- fetch "versions.json"
    snapshots <- fetch "snapshots.json"
    return (releases, snapshots)
    where fetch file = do
              temp <- shell "mktemp"
              shell_ $ "aws s3 cp s3://docs-daml-com/" <> file <> " " <> temp
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

data GitHubVersion = GitHubVersion { prerelease :: Bool, tag_name :: String, notes :: String } deriving Show
instance JSON.FromJSON GitHubVersion where
    parseJSON = JSON.withObject "GitHubVersion" $ \v -> GitHubVersion
        <$> v JSON..: Text.pack "prerelease"
        <*> v JSON..: Text.pack "tag_name"
        <*> v JSON..:? Text.pack "body" JSON..!= ""

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

fetch_gh_versions :: IO ([GitHubVersion], GitHubVersion)
fetch_gh_versions = do
    resp <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    let latest = List.maximumOn (to_v . name) $ filter (not . prerelease) resp
    return (resp, latest)

same_versions :: (Set.Set Version, Set.Set Version) -> [GitHubVersion] -> Bool
same_versions s3_versions gh_versions =
    -- Versions 0.13.5 and earlier can no longer be built by this script, and
    -- happen to not exist in the s3 repo. This means they are not generated
    -- and thus do not appear in the versions.json file on s3. This means that
    -- if we do not remove them from the listing here, the docs cron always
    -- believes there is a change to deploy.
    let gh_releases = Set.fromList $ map (to_v . name) $ filter (\v -> to_v (name v) > to_v "0.13.5") $ filter (not . prerelease) gh_versions
        gh_snapshots = Set.fromList $ map (to_v . name) $ filter prerelease gh_versions
    in s3_versions == (gh_releases, gh_snapshots)

main :: IO ()
main = do
    Control.forM_ [IO.stdout, IO.stderr] $
        \h -> IO.hSetBuffering h IO.LineBuffering
    putStrLn "Checking for new version..."
    (gh_versions, gh_latest) <- fetch_gh_versions
    s3_versions_before <- fetch_s3_versions
    if same_versions s3_versions_before gh_versions
    then do
        putStrLn "No new version found, skipping."
        Exit.exitSuccess
    else do
        IO.withTempDir $ \temp_dir -> do
            putStrLn "Building docs listing"
            docs_folder <- build_docs_folder temp_dir gh_versions $ name gh_latest
            putStrLn "Done building docs bundle. Checking versions again to avoid race condition..."
            s3_versions_after <- fetch_s3_versions
            if same_versions s3_versions_after gh_versions
            then do
                putStrLn "No more new version, another process must have pushed already."
                Exit.exitSuccess
            else do
                push_to_s3 docs_folder
