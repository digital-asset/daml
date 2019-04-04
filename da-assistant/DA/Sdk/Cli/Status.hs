-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Status
    ( status
    ) where

import           DA.Sdk.Prelude

import           DA.Sdk.Cli.Conf        (Project (..))
import           DA.Sdk.Cli.Job         (getProcesses, isProjectProcess)
import           DA.Sdk.Cli.Job.Types   (Process (..), describeJob)
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Cli.SelfUpgrade (displayUpdateChannelNote)
import           DA.Sdk.Cli.Monad.UserInteraction

status :: CliM ()
status = do
    display "Welcome to the SDK Assistant."
    mbProj <- asks envProject
    case mbProj of
        Nothing -> do
            display "You are currently NOT in a DAML project."
            display "Status command reports the global state of processes in this case."
            displayNewLine
            display "To start a new project type: da new my_new_project"
        Just project -> do
            let name = projectProjectName project
                dir = pathToText $ projectPath project
            display $ "You are currently in the project '" <> name <> "' (" <> dir <> ")."
    let filterInterestingProcs = maybe id (\p -> filter (isProjectProcess p)) mbProj
    procs <- filterInterestingProcs <$> fmap (map snd) getProcesses
    if null procs
    then do
        display "No processes are running. To start them, type: da start"
    else do
        display "The following processes are running:"
        displayNewLine
        forM_ procs $ \p -> do
            displayDoc $ describeJob $ processJob p
            displayNewLine
    displayUpdateChannelNote
