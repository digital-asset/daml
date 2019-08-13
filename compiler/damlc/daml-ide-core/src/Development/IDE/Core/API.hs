-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.Core.API
    ( IdeState
    , Action
    , initialise
    , mkDamlEnv
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
    , runActionSync
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
