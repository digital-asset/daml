-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Management
    ( sdkManagement
    ) where

import Control.Monad.Except
import Data.Foldable (traverse_)
import DA.Sdk.Cli.Command.Types
import DA.Sdk.Cli.Monad
import DA.Sdk.Cli.Conf.Types
import qualified DA.Sdk.Pretty as P
import DA.Sdk.Prelude
import qualified Data.Text.Extended as T
import qualified Data.Set as Set
import qualified Data.Map.Strict as MS
import qualified Data.Yaml as Yaml
import qualified DA.Sdk.Cli.Repository as Repo
import qualified DA.Sdk.Version as V
import qualified System.Exit
import           DA.Sdk.Cli.Monad.UserInteraction

data ReleaseTagError
    = RTEVersionOnlyOnServer V.SemVersion
    | RTEVersionOnlyInFile V.SemVersion
    deriving (Show)

data Error
    = ErrorBintrayCredentialsMissing
    | ErrorRequest Repo.Error
    | ErrorYaml Yaml.ParseException
    deriving (Show)

-- | Run commands for the sdk team to manage releases.
sdkManagement :: SdkAction -> CliM ()
sdkManagement action = do
    errOrSuccess <- runExceptT $ doSdkManagement action
    case errOrSuccess of
        Left err -> do
            display $ T.show err
            liftIO System.Exit.exitFailure
        Right () -> pure ()

doSdkManagement :: SdkAction -> ExceptT Error CliM ()
doSdkManagement action = do
    repositoryURLs <- asks (confRepositoryURLs . envConf)
    credentials <- asks (confCredentials . envConf)
    handle <- Repo.makeManagementHandle repositoryURLs credentials
    case action of
        SdkActionTags tagActions ->
            case tagActions of
                SdkTagsActionList Nothing -> do
                    releases <- requestToExceptT $ Repo._mhListReleases handle
                    displayDoc $ Repo.prettyReleasesWithTags releases
                SdkTagsActionList (Just sdkVersion) ->
                    requestAndSayTags $ Repo._mhGetReleaseTags handle $ Repo.semToGenericVersion sdkVersion
                SdkTagsActionAdd sdkVersion newTags ->
                    requestAndSayTags
                        $ Repo._mhAddReleaseTags handle (Repo.semToGenericVersion sdkVersion)
                        $ Set.fromList newTags
                SdkTagsActionRemove sdkVersion removeTags ->
                    requestAndSayTags
                        $ Repo._mhRemoveReleaseTags handle (Repo.semToGenericVersion sdkVersion)
                        $ Set.fromList removeTags
                SdkTagsActionSync filePath -> do
                    local <-
                        toExceptT ErrorYaml
                      $ liftIO
                      $ Yaml.decodeFileEither
                      $ pathToString filePath
                    remote <- requestToExceptT $ Repo._mhListReleases handle
                    let (errors, releases) = diffReleases remote local
                    traverse_ (displayDoc . warningReleaseTagError) errors
                    if anyChanges releases
                    then do
                        displayDoc $ prettyReleaseDiffs releases
                        isScript <- asks (confIsScript . envConf)
                        answer <-
                            if isScript
                            then
                                pure True
                            else do
                                display "Are you sure you want to synchronize these changes? (y/n)"
                                (== "y") <$> liftIO getLine
                        if answer
                        then
                            forM_ releases $ \releaseEdit ->
                                syncReleaseEdit handle releaseEdit
                        else
                            lift $ display "No changes where made."
                    else
                        lift $ display "No tags have been changed."
                  where
                    anyChanges :: [Repo.Release (PartitionedEdits Text)] -> Bool
                    anyChanges =
                        any (\(Repo.Release _ tags) -> hasChanges tags)
  where
    requestAndSayTags ::
           CliM (Either Repo.Error (Set.Set Text))
        -> ExceptT Error CliM ()
    requestAndSayTags request =
        requestToExceptT request >>= displayDoc . prettyTags


-- | Wrap an error with the 'Error' of this module.
toExceptT ::
        Functor m
    => (err -> Error)
    -> m (Either err a)
    -> ExceptT Error m a
toExceptT wrapper = withExceptT wrapper . ExceptT

requestToExceptT :: Functor m => m (Either Repo.Error a) -> ExceptT Error m a
requestToExceptT = toExceptT ErrorRequest

-- | Diff local and remote release tags.
-- Versions not defined on both ends are collected, since we want to raise a
-- warning, but not synchronize anything in that case.
diffReleases ::
       [Repo.Release (Set.Set Text)]
    -> [Repo.Release (Set.Set Text)]
    -> ([ReleaseTagError], [Repo.Release (PartitionedEdits Text)])
diffReleases remote local = (onlyOnRemote ++ onlyOnLocal, needToChange)
  where
    -- Convert a list of releases to a map keyed by the release version.
    toReleaseMap :: [Repo.Release a] -> MS.Map V.SemVersion (Repo.Release a)
    toReleaseMap =
        MS.fromList . map (\release@(Repo.Release version _) -> (version, release))

    remoteMap = toReleaseMap remote
    localMap = toReleaseMap local

    -- | Versions only on the remote.
    onlyOnRemote :: [ReleaseTagError]
    onlyOnRemote = RTEVersionOnlyOnServer <$> MS.keys (MS.difference remoteMap localMap)

    -- | Versions only defined locally.
    onlyOnLocal :: [ReleaseTagError]
    onlyOnLocal = RTEVersionOnlyInFile <$> MS.keys (MS.difference localMap remoteMap)

    -- | Versions found both on the remote and locally.
    inBoth ::
           MS.Map
            V.SemVersion
            (Repo.Release (Set.Set Text), Repo.Release (Set.Set Text))
    inBoth = MS.intersectionWith (,) remoteMap localMap
    needToChange :: [Repo.Release (PartitionedEdits Text)]
    needToChange = fmap diffRelease $ MS.toList inBoth

    diffRelease ::
           (V.SemVersion, (Repo.Release (Set.Set Text), Repo.Release (Set.Set Text)))
        -> Repo.Release (PartitionedEdits Text)
    diffRelease (version, (Repo.Release _ remoteTags, Repo.Release _ localTags)) =
        Repo.Release version
      $ PartitionedEdits
        { _peCopied = onBothTags
        , _peInserted = onlyOnLocalTags
        , _peDeleted = onlyOnRemoteTags
        }
      where
        onBothTags = Set.toList $ Set.intersection remoteTags localTags
        onlyOnLocalTags = Set.toList $ Set.difference localTags remoteTags
        onlyOnRemoteTags = Set.toList $ Set.difference remoteTags localTags

syncReleaseEdit :: Repo.ManagementHandle -> Repo.Release (PartitionedEdits Text) -> ExceptT Error CliM ()
syncReleaseEdit handle release@(Repo.Release sdkVersion edits) =
    if hasChanges edits
    then do
        displayDoc $ P.t "Synchronizing" P.<+> prettyReleaseDiff release
        void $ requestToExceptT $ Repo._mhSetReleaseTags handle (Repo.semToGenericVersion sdkVersion) tags
    else
        displayDoc $ P.t "No changes for:" P.<+> P.pretty sdkVersion
  where
    tags :: Set.Set Text
    tags = Set.fromList $ _peCopied edits ++ _peInserted edits

--------------------------------------------------------------------------------
-- Edits
--------------------------------------------------------------------------------

-- | The result of partitioning edits.
data PartitionedEdits a = PartitionedEdits
    { _peCopied :: [a]
    , _peInserted :: [a]
    , _peDeleted :: [a]
    }
    deriving (Show, Eq)

-- | Are there any changes in the passed 'PartitionedEdits'?
-- There are changes if either inserted or deleted is a non empty list.
hasChanges :: PartitionedEdits a -> Bool
hasChanges (PartitionedEdits _ inserted deleted) =
    not (null inserted && null deleted)

--------------------------------------------------------------------------------
-- Pretty printing
--------------------------------------------------------------------------------

prettyTags :: Set.Set Text -> P.Doc ann
prettyTags = P.hsep . fmap P.t . Set.toList

prettyPartitionedDiffs :: PartitionedEdits Text -> P.Doc ann
prettyPartitionedDiffs (PartitionedEdits _copied inserted deleted) =
    P.sep (fmap add inserted ++ fmap remove deleted)
  where
    prependSymbol :: P.Doc ann -> Text -> P.Doc ann
    prependSymbol symbol t = symbol <> P.t t

    add = prependSymbol "+"

    remove = prependSymbol "-"

prettyReleaseDiff :: Repo.Release (PartitionedEdits Text) -> P.Doc ann
prettyReleaseDiff (Repo.Release version edits) = P.nest 4 $ P.sep
    [ P.pretty version <> ":"
    , prettyPartitionedDiffs edits
    ]

prettyReleaseDiffs :: [Repo.Release (PartitionedEdits Text)] -> P.Doc ann
prettyReleaseDiffs = P.vsep . fmap prettyReleaseDiff


warningReleaseTagError :: ReleaseTagError -> P.Doc ann
warningReleaseTagError = \case
    RTEVersionOnlyOnServer version ->
        "Warning: Version" P.<+> P.pretty version P.<+> "only found on the remote."
    RTEVersionOnlyInFile version ->
        "Warning: Version" P.<+> P.pretty version P.<+> "only found in the local yaml file."
