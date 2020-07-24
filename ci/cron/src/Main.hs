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
import qualified Data.List.Split as Split
import qualified Data.Maybe as Maybe
import qualified Data.Ord
import qualified Data.SemVer
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

build_and_push :: FilePath -> [Version] -> IO ()
build_and_push temp versions = do
    restore_sha $ do
        Data.Foldable.for_ versions (\version -> do
            putStrLn $ "Building " <> show version <> "..."
            build version
            putStrLn $ "Pushing " <> show version <> " to S3 (as subfolder)..."
            push temp $ show version
            putStrLn "Done.")
    where
        restore_sha io =
            Control.Exception.bracket (init <$> shell "git rev-parse HEAD")
                                      (\cur_sha -> shell_ $ "git checkout " <> cur_sha)
                                      (const io)
        build version = do
            shell_ $ "git checkout v" <> show version
            -- The release-triggering commit does not have a tag, so we need to
            -- find it by walking through the git history of the LATEST file.
            sha <- find_commit_for_version version
            Control.Exception.bracket_
                (shell_ $ "git checkout " <> sha <> " -- docs/source/support/release-notes.rst")
                (shell_ "git reset --hard")
                (build_helper version)
        build_helper version = do
            robustly_download_nix_packages
            shell_ $ "DAML_SDK_RELEASE_VERSION=" <> show version <> " bazel build //docs:docs"
            shell_ $ "mkdir -p  " <> temp </> show version
            shell_ $ "tar xzf bazel-bin/docs/html.tar.gz --strip-components=1 -C" <> temp </> show version

push :: FilePath -> FilePath -> IO ()
push local remote =
    shell_ $ "aws s3 cp " <> local <> " " <> "s3://docs-daml-com" </> remote <> " --recursive"

fetch_if_missing :: FilePath -> Version -> IO ()
fetch_if_missing temp v = do
    missing <- not <$> Directory.doesDirectoryExist (temp </> show v)
    if missing then do
        putStrLn $ "Downloading " <> show v <> "..."
        shell_ $ "aws s3 cp s3://docs-daml-com" </> show v <> " " <> temp </> show v <> " --recursive"
        putStrLn "Done."
    else do
        putStrLn $ show v <> " already present."

update_s3 :: FilePath -> Versions -> IO ()
update_s3 temp vs = do
    putStrLn "Updating versions.json & hidden.json..."
    create_versions_json (dropdown vs) (temp </> "versions.json")
    let hidden = List.sortOn Data.Ord.Down $ Set.toList $ all_versions vs `Set.difference` (Set.fromList $ dropdown vs)
    create_versions_json hidden (temp </> "hidden.json")
    shell_ $ "aws s3 cp " <> temp </> "versions.json s3://docs-daml-com/versions.json"
    shell_ $ "aws s3 cp " <> temp </> "hidden.json s3://docs-daml-com/hidden.json"
    -- FIXME: remove after running once (and updating the reading bit in this file)
    shell_ $ "aws s3 cp " <> temp </> "hidden.json s3://docs-daml-com/snapshots.json"
    putStrLn "Done."
    where
        create_versions_json versions file = do
            -- Not going through Aeson because it represents JSON objects as
            -- unordered maps, and here order matters.
            let versions_json = versions
                                & map show
                                & map (\s -> "\"" <> s <> "\": \"" <> s <> "\"")
                                & List.intercalate ", "
                                & \s -> "{" <> s <> "}"
            writeFile file versions_json

update_top_level :: FilePath -> Version -> Version -> IO ()
update_top_level temp new old = do
    new_files <- Set.fromList <$> Directory.listDirectory (temp </> show new)
    old_files <- Set.fromList <$> Directory.listDirectory (temp </> show old)
    let to_delete = Set.toList $ old_files `Set.difference` new_files
    Control.when (not $ null to_delete) $ do
        putStrLn $ "Deleting top-level files: " <> show to_delete
        Data.Foldable.for_ to_delete (\f -> do
            shell_ $ "aws s3 rm s3://docs-daml-com" </> f <> " --recursive")
        putStrLn "Done."
    putStrLn $ "Pushing " <> show new <> " to top-level..."
    shell_ $ "aws s3 cp " <> temp </> show new </> " s3://docs-daml-com/ --recursive"
    putStrLn "Done."

reset_cloudfront :: IO ()
reset_cloudfront = do
    putStrLn "Refreshing CloudFront cache..."
    shell_ $ "aws cloudfront create-invalidation"
             <> " --distribution-id E1U753I56ERH55"
             <> " --paths '/*'"

find_commit_for_version :: Version -> IO String
find_commit_for_version version = do
    ver_sha <- init <$> (shell $ "git rev-parse v" <> show version)
    let expected = ver_sha <> " " <> show version
    -- git log -G 'regex' returns all the commits for which 'regex' appears in
    -- the diff. To find out the commit that "released" the version. The commit
    -- we want is a commit that added a single line, which matches the version
    -- we are checking for.
    matching_commits <- lines <$> (shell $ "git log --format=%H --all -G '" <> ver_sha <> "' -- LATEST")
    matching <- Maybe.catMaybes <$> Traversable.for matching_commits (\sha -> do
        after <- Set.fromList . lines <$> (shell $ "git show " <> sha <> ":LATEST")
        before <- Set.fromList . lines <$> (shell $ "git show " <> sha <> "~:LATEST")
        return $ case Set.toList(after `Set.difference` before) of
                     [line] | line == expected -> Just sha
                     _ -> Nothing)
    case matching of
      [sha] -> return sha
      _ -> fail $ "Expected single commit to match release " <> show version <> ", but instead found: " <> show matching

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
                _ -> fail $ "Assumption violated: link header entry did not match regex.\nEntry: " <> l

data PreVersion = PreVersion { prerelease :: Bool, tag :: Version }
instance JSON.FromJSON PreVersion where
    parseJSON = JSON.withObject "PreVersion" $ \v -> PreVersion
        <$> v JSON..: "prerelease"
        <*> let json_text = v JSON..: "tag_name"
            in (version . Text.tail <$> json_text)

data Version = Version Data.SemVer.Version
    deriving (Ord, Eq)
instance Show Version where
    show (Version v) = Data.SemVer.toString v

version :: Text.Text -> Version
version t = Version $ (\case Left s -> (error s); Right v -> v) $ Data.SemVer.fromText t

data Versions = Versions { top :: Version, all_versions :: Set.Set Version, dropdown :: [Version] }
    deriving Eq

versions :: [PreVersion] -> Versions
versions vs =
    let all_versions = Set.fromList $ map tag vs
        dropdown = vs
                   & filter (not . prerelease)
                   & map tag
                   & filter (>= version "1.0.0")
                   & List.sortOn Data.Ord.Down
        top = head dropdown
    in Versions {..}

fetch_gh_versions :: IO Versions
fetch_gh_versions = do
    response <- fetch_gh_paginated "https://api.github.com/repos/digital-asset/daml/releases"
    -- versions prior to 0.13.10 cannot be built anymore and are not present in
    -- the repo.
    return $ versions $ filter (\v -> tag v >= version "0.13.10") response

fetch_s3_versions :: IO Versions
fetch_s3_versions = do
    dropdown <- fetch "versions.json" False
    -- TODO: read hidden.json after this has run once
    hidden <- fetch "snapshots.json" True
    return $ versions $ dropdown <> hidden
    where fetch file prerelease = do
              temp <- shell "mktemp"
              shell_ $ "aws s3 cp s3://docs-daml-com/" <> file <> " " <> temp
              s3_raw <- shell $ "cat " <> temp
              let type_annotated_value :: Maybe JSON.Object
                  type_annotated_value = JSON.decode $ LBS.fromString s3_raw
              case type_annotated_value of
                  Just s3_json -> return $ map (\s -> PreVersion prerelease (version s)) $ H.keys s3_json
                  Nothing -> Exit.die "Failed to get versions from s3"

main :: IO ()
main = do
    Control.forM_ [IO.stdout, IO.stderr] $
        \h -> IO.hSetBuffering h IO.LineBuffering
    putStrLn "Checking for new version..."
    gh_versions <- fetch_gh_versions
    s3_versions <- fetch_s3_versions
    if s3_versions == gh_versions
    then do
        putStrLn "Versions match, nothing to do."
        Exit.exitSuccess
    else do
        -- We may have added versions. We need to build and push them.
        let added = Set.toList $ all_versions gh_versions `Set.difference` all_versions s3_versions
        IO.withTempDir $ \temp_dir -> do
            putStrLn $ "Versions to build: " <> show added
            build_and_push temp_dir added
            Control.when (top gh_versions /= top s3_versions) $ do
                putStrLn $ "Updating top-level version from " <> (show $ top s3_versions) <> " to " <> (show $ top gh_versions)
                fetch_if_missing temp_dir (top gh_versions)
                fetch_if_missing temp_dir (top s3_versions)
                update_top_level temp_dir (top gh_versions) (top s3_versions)
            putStrLn "Updating versions.json & hidden.json"
            update_s3 temp_dir gh_versions
        reset_cloudfront
