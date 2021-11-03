-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Completion is an LF postprocessing step. It happens after LF conversion,
-- but before simplification and typechecking. Its purpose is to propagate
-- any type-level information that can only be obtained with access to the
-- LF world.
module DA.Daml.LF.Completer
    ( completeModule
    ) where

import qualified Data.NameMap as NM
import qualified Data.Set as S
import qualified Data.Text as T
import DA.Daml.LF.Ast as LF

completeModule :: LF.World -> LF.Version -> LF.Module -> LF.Module
completeModule world lfVersion mod@Module{..}
    | LF.supports lfVersion featureInterfaces
    = mod { moduleTemplates = NM.map (completeTemplate world') moduleTemplates }

    | otherwise
    = mod
  where
    world' = extendWorldSelf mod world

completeTemplate :: LF.World -> LF.Template -> LF.Template
completeTemplate world tpl@Template{..} =
    tpl { tplImplements = NM.map (completeTemplateImplements world) tplImplements }

completeTemplateImplements :: LF.World -> LF.TemplateImplements -> LF.TemplateImplements
completeTemplateImplements world tpi@TemplateImplements{..} =
    case lookupInterface tpiInterface world of
        Left _ -> error ("Could not find interface " <> T.unpack (T.intercalate "." (unTypeConName (qualObject tpiInterface))))
        Right DefInterface { intFixedChoices, intPrecondition } ->
            tpi { tpiInheritedChoiceNames = S.fromList (NM.names intFixedChoices)
                , tpiPrecond = intPrecondition}
