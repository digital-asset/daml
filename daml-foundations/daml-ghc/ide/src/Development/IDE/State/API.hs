-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.State.API
    ( HoverText
    , getHoverTextContent
    , IdeState
    , Action
    , initialise
    , getDalf
    , getAtPoint
    , getDefinition
    , shutdown
    , setFilesOfInterest
    , setOpenVirtualResources
    , setBufferModified
    , runAction
    , runActions
    , writeProfile
    , getDiagnostics
    , unsafeClearDiagnostics
    , generatePackageMap
    , getDependencies
    , logDebug
    , logSeriousError
    , VFSHandle(..)
    , makeVFSHandle
    , makeLSPVFSHandle
    ) where

import           Development.IDE.Types.LSP
import           Development.IDE.State.Service.Daml
import           Development.IDE.State.Rules.Daml
import           Development.IDE.State.FileStore
import           Development.Shake                             (Action)
