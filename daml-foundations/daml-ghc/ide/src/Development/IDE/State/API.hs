-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.State.API
    ( IdeState
    , Action
    , initialise
    , getDalf
    , getAtPoint
    , getDefinition
    , shutdown
    , setFilesOfInterest
    , modifyFilesOfInterest
    , setOpenVirtualResources
    , modifyOpenVirtualResources
    , setBufferModified
    , runAction
    , runActions
    , runActionSync
    , runActionsSync
    , writeProfile
    , getDiagnostics
    , unsafeClearDiagnostics
    , generatePackageMap
    , getDependencies
    , ideLogger
    , VFSHandle
    , makeVFSHandle
    , makeLSPVFSHandle
    ) where

import           Development.IDE.Core.Service.Daml
import           Development.IDE.Core.Rules.Daml
import           Development.IDE.Core.FileStore
import           Development.Shake                             (Action)
