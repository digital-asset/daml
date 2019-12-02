-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.NameCollision
    ( checkModule
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import Data.Maybe
import Control.Monad.Extra
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Control.Monad.State.Strict as S

-- | The various names we wish to track within a package.
-- This type separates all the different kinds of names
-- out nicely, and preserves case sensitivity, so the
-- names are easy to display in error messages. To get
-- the corresponding case insensitive fully resolved name,
-- see 'FRName'.
data Name
    = NModule ModuleName
    | NRecordType ModuleName TypeConName
    | NVariantType ModuleName TypeConName
    | NEnumType ModuleName TypeConName
    | NVariantCon ModuleName TypeConName VariantConName
    | NEnumCon ModuleName TypeConName VariantConName
    | NField ModuleName TypeConName FieldName
    | NChoice ModuleName TypeConName ChoiceName

-- | Display a name in a super unambiguous way.
displayName :: Name -> T.Text
displayName = \case
    NModule (ModuleName m) ->
        T.concat ["module ", dot m]
    NRecordType (ModuleName m) (TypeConName t) ->
        T.concat ["record ", dot m, ":", dot t]
    NVariantType (ModuleName m) (TypeConName t) ->
        T.concat ["variant ", dot m, ":", dot t]
    NEnumType (ModuleName m) (TypeConName t) ->
        T.concat ["enum ", dot m, ":", dot t]
    NVariantCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        T.concat ["variant constructor ", dot m, ":", dot t, ".", v]
    NEnumCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        T.concat ["enum constructor ", dot m, ":", dot t, ".", v]
    NField (ModuleName m) (TypeConName t) (FieldName f) ->
        T.concat ["field ", dot m, ":", dot t, ".", f]
    NChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        T.concat ["choice ", dot m, ":", dot t, ".", c]
  where
    dot = T.intercalate "."

-- | Asks whether a name collision is permitted. According to the
-- LF Spec, a name collision is only permitted when it occurs
-- between a record type and a variant constructor defined in
-- the same module.
nameCollisionPermitted :: Name -> Name -> Bool
nameCollisionPermitted a b =
    case (a,b) of
        (NRecordType m1 _, NVariantCon m2 _ _) -> m1 == m2
        (NVariantCon m1 _ _, NRecordType m2 _) -> m1 == m2
        _ -> False

-- | Asks whether a name collision is forbidden.
nameCollisionForbidden :: Name -> Name -> Bool
nameCollisionForbidden a b = not (nameCollisionPermitted a b)

-- | Fully resolved name within a package. We don't use
-- Qualified from DA.Daml.LF.Ast because that hides collisions
-- between module names and type names. This should only be
-- constructed lower case in order to have case-insensitivity.
--
-- This corresponds to the following section of the LF spec:
-- https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst#fully-resolved-name
newtype FRName = FRName [T.Text]
    deriving (Eq, Ord)

-- | Turn a name into a fully resolved name.
fullyResolve :: Name -> FRName
fullyResolve = FRName . map T.toLower . \case
    NModule (ModuleName m) ->
        m
    NRecordType (ModuleName m) (TypeConName t) ->
        m ++ t
    NVariantType (ModuleName m) (TypeConName t) ->
        m ++ t
    NEnumType (ModuleName m) (TypeConName t) ->
        m ++ t
    NVariantCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        m ++ t ++ [v]
    NEnumCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        m ++ t ++ [v]
    NField (ModuleName m) (TypeConName t) (FieldName f) ->
        m ++ t ++ [f]
    NChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        m ++ t ++ [c]

-- | State of the name collision checker. This is a
-- map from fully resolved names within a package to their
-- original names. We update this map as we go along.
newtype NCState = NCState (M.Map FRName [Name])

-- | Initial name collision checker state.
initialState :: NCState
initialState = NCState M.empty

-- | Try to add a name to the NCState. Returns Error only
-- if the name results in a forbidden name collision.
addName :: Name -> NCState -> Either Error NCState
addName name (NCState nameMap) = do
    let frName = fullyResolve name
        oldNames = fromMaybe [] (M.lookup frName nameMap)
        badNames = filter (nameCollisionForbidden name) oldNames
    if null badNames then do
        Right . NCState $ M.insert frName (name : oldNames) nameMap
    else do
        Left $ EForbiddenNameCollision
            (displayName name)
            (map displayName badNames)

checkName :: MonadGamma m => Name -> S.StateT NCState m ()
checkName name = do
    oldState <- S.get
    case addName name oldState of
        Left err ->
            throwWithContext err
        Right !newState ->
            S.put newState

checkDataType :: MonadGamma m => ModuleName -> DefDataType -> S.StateT NCState m ()
checkDataType moduleName DefDataType{..} =
    case dataCons of
        DataRecord fields -> do
            checkName (NRecordType moduleName dataTypeCon)
            forM_ fields $ \(fieldName, _) -> do
                checkName (NField moduleName dataTypeCon fieldName)

        DataVariant constrs -> do
            checkName (NVariantType moduleName dataTypeCon)
            forM_ constrs $ \(vconName, _) -> do
                checkName (NVariantCon moduleName dataTypeCon vconName)

        DataEnum constrs -> do
            checkName (NEnumType moduleName dataTypeCon)
            forM_ constrs $ \vconName -> do
                checkName (NEnumCon moduleName dataTypeCon vconName)

        DataSynonym _ -> return ()

checkTemplate :: MonadGamma m => ModuleName -> Template -> S.StateT NCState m ()
checkTemplate moduleName Template{..} = do
    forM_ tplChoices $ \TemplateChoice{..} ->
        checkName (NChoice moduleName tplTypeCon chcName)

-- | Check whether a module satisfies the name collision condition.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 =
    void . flip S.runStateT initialState $ do
        checkName (NModule (moduleName mod0))
        forM_ (moduleDataTypes mod0) $ \dataType ->
            withContext (ContextDefDataType mod0 dataType) $
                checkDataType (moduleName mod0) dataType
        forM_ (moduleTemplates mod0) $ \tpl ->
            withContext (ContextTemplate mod0 tpl TPWhole) $
                checkTemplate (moduleName mod0) tpl
