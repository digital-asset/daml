-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.TypeChecker.NameCollision
    ( runCheckModuleDeps
    , runCheckPackage
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Error
import Data.List
import Data.Maybe
import Development.IDE.Types.Diagnostics
import Control.Monad.Extra
import qualified Data.NameMap as NM
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Control.Monad.Trans.RWS.CPS

-- | The various names we wish to track within a package.
-- This type separates all the different kinds of names
-- out nicely, and preserves case sensitivity, so the
-- names are easy to display in error messages. To get
-- the corresponding case insensitive fully resolved name,
-- see 'FRName'.
data Name
    = NModule ModuleName
    | NVirtualModule ModuleName ModuleName
    -- ^ For a module A.B.C we insert virtual module names A and A.B.
    -- This is used to check that you do not have a type B
    -- in a module A and a module A.B.C at the same time,
    -- even if you don't have a module A.B.
    -- Note that for now this is not enforced on the Scala side.
    | NRecordType ModuleName TypeConName
    | NVariantType ModuleName TypeConName
    | NEnumType ModuleName TypeConName
    | NTypeSynonym ModuleName TypeSynName
    | NVariantCon ModuleName TypeConName VariantConName
    | NEnumCon ModuleName TypeConName VariantConName
    | NField ModuleName TypeConName FieldName
    | NChoice ModuleName TypeConName ChoiceName
    | NChoiceViaInterface ModuleName TypeConName ChoiceName (Qualified TypeConName)
    | NInterface ModuleName TypeConName
    | NInterfaceChoice ModuleName TypeConName ChoiceName

-- | Display a name in a super unambiguous way.
displayName :: Name -> T.Text
displayName = \case
    NModule (ModuleName m) ->
        T.concat ["module ", dot m]
    NVirtualModule (ModuleName m) (ModuleName mOrigin) ->
        T.concat ["module prefix ", dot m, " (from ", dot mOrigin <> ")"]
    NRecordType (ModuleName m) (TypeConName t) ->
        T.concat ["record ", dot m, ":", dot t]
    NVariantType (ModuleName m) (TypeConName t) ->
        T.concat ["variant ", dot m, ":", dot t]
    NEnumType (ModuleName m) (TypeConName t) ->
        T.concat ["enum ", dot m, ":", dot t]
    NTypeSynonym (ModuleName m) (TypeSynName t) ->
        T.concat ["synonym ", dot m, ":", dot t]
    NVariantCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        T.concat ["variant constructor ", dot m, ":", dot t, ".", v]
    NEnumCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        T.concat ["enum constructor ", dot m, ":", dot t, ".", v]
    NField (ModuleName m) (TypeConName t) (FieldName f) ->
        T.concat ["field ", dot m, ":", dot t, ".", f]
    NChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        T.concat ["choice ", dot m, ":", dot t, ".", c]
    NChoiceViaInterface (ModuleName m) (TypeConName t) (ChoiceName c) (Qualified _ (ModuleName imod) (TypeConName ityp)) ->
        T.concat ["choice ", dot m, ":", dot t, ".", c, " (via interface ", dot imod, ":", dot ityp, ")"]
    NInterface (ModuleName m) (TypeConName t) ->
        T.concat ["interface ", dot m, ":", dot t]
    NInterfaceChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        T.concat ["interface choice ", dot m, ":", dot t, ".", c]
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
        (NVirtualModule {}, NVirtualModule {}) -> True
        (NModule {}, NVirtualModule {}) -> True
        (NVirtualModule {}, NModule {}) -> True
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
-- https://github.com/digital-asset/daml/blob/main/daml-lf/spec/daml-lf-1.rst#fully-resolved-name
newtype FRName = FRName [T.Text]
    deriving (Eq, Ord)

-- | Turn a name into a fully resolved name.
fullyResolve :: Name -> FRName
fullyResolve = FRName . map T.toLower . \case
    NModule (ModuleName m) ->
        m
    NVirtualModule (ModuleName m) _ ->
        m
    NRecordType (ModuleName m) (TypeConName t) ->
        m ++ t
    NVariantType (ModuleName m) (TypeConName t) ->
        m ++ t
    NEnumType (ModuleName m) (TypeConName t) ->
        m ++ t
    NTypeSynonym (ModuleName m) (TypeSynName t) ->
        m ++ t
    NVariantCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        m ++ t ++ [v]
    NEnumCon (ModuleName m) (TypeConName t) (VariantConName v) ->
        m ++ t ++ [v]
    NField (ModuleName m) (TypeConName t) (FieldName f) ->
        m ++ t ++ [f]
    NChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        m ++ t ++ [c]
    NChoiceViaInterface (ModuleName m) (TypeConName t) (ChoiceName c) _ ->
        m ++ t ++ [c]
    NInterface (ModuleName m) (TypeConName t) ->
        m ++ t
    NInterfaceChoice (ModuleName m) (TypeConName t) (ChoiceName c) ->
        m ++ t ++ [c]

-- | State of the name collision checker. This is a
-- map from fully resolved names within a package to their
-- original names. We update this map as we go along.
newtype NCState = NCState (M.Map FRName [Name])

-- | Initial name collision checker state.
initialState :: NCState
initialState = NCState M.empty

-- | Monad in which to run the name collision check.
type NCMonad t = RWS () [Diagnostic] NCState t

-- | Run the name collision with a blank initial state.
runNameCollision :: NCMonad t -> [Diagnostic]
runNameCollision m = snd (evalRWS m () initialState)

-- | Try to add a name to the NCState. Returns Error only
-- if the name results in a forbidden name collision.
addName :: Name -> NCState -> Either Diagnostic NCState
addName name (NCState nameMap)
    | null badNames =
        Right . NCState $ M.insert frName (name : oldNames) nameMap
    | otherwise =
        let err = EForbiddenNameCollision (displayName name) (map displayName badNames)
            diag = toDiagnostic err
        in Left diag
  where
    frName = fullyResolve name
    oldNames = fromMaybe [] (M.lookup frName nameMap)
    badNames = filter (nameCollisionForbidden name) oldNames

checkName :: Name -> NCMonad ()
checkName name = do
    oldState <- get
    case addName name oldState of
        Left err ->
            tell [err]
        Right !newState ->
            put newState

checkDataType :: ModuleName -> DefDataType -> NCMonad ()
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

        DataInterface -> checkName (NInterface moduleName dataTypeCon)

checkTemplate :: ModuleName -> Template -> NCMonad ()
checkTemplate moduleName Template{..} = do
    forM_ tplChoices $ \TemplateChoice{..} ->
        checkName (NChoice moduleName tplTypeCon chcName)

checkSynonym :: ModuleName -> DefTypeSyn -> NCMonad ()
checkSynonym moduleName DefTypeSyn{..} =
    checkName (NTypeSynonym moduleName synName)

checkInterface :: ModuleName -> DefInterface -> NCMonad ()
checkInterface moduleName DefInterface{..} = do
    forM_ intChoices $ \TemplateChoice{..} ->
        checkName (NInterfaceChoice moduleName intName chcName)

checkModuleName :: Module -> NCMonad ()
checkModuleName m =
    checkName (NModule (moduleName m))

checkVirtualModuleName :: (ModuleName, ModuleName) -> NCMonad ()
checkVirtualModuleName (m, mOrigin) =
    checkName (NVirtualModule m mOrigin)

checkModuleBody :: Module -> NCMonad ()
checkModuleBody m = do
    forM_ (moduleDataTypes m) $ \dataType ->
        checkDataType (moduleName m) dataType
    forM_ (moduleTemplates m) $ \tpl ->
        checkTemplate (moduleName m) tpl
    forM_ (moduleSynonyms m) $ \synonym ->
        checkSynonym (moduleName m) synonym
    forM_ (moduleInterfaces m) $ \iface ->
        checkInterface (moduleName m) iface

checkModule :: Module -> NCMonad ()
checkModule m = do
    checkModuleName m
    mapM_ checkVirtualModuleName (virtualModuleNames m)
    checkModuleBody m

-- | Is one module an ascendant of another? For instance
-- module "A" is an ascendant of module "A.B" and "A.B.C".
--
-- Normally we wouldn't care about this in Daml, because
-- the name of a module has no relation to its logical
-- dependency structure. But since we're compiling to LF,
-- module names (e.g. "A.B") may conflict with type names
-- ("A:B"), so we need to check modules in which this conflict
-- may arise.
--
-- The check here is case-insensitive because the name-collision
-- condition in Daml-LF is case-insensitiv (in order to make
-- codegen easier for languages that control case differently
-- from Daml).
isAscendant :: ModuleName -> ModuleName -> Bool
isAscendant (ModuleName xs) (ModuleName ys) =
    (length xs < length ys) && and (zipWith sameish xs ys)
    where sameish a b = T.toLower a == T.toLower b


-- | Check whether a module and its dependencies satisfy the
-- name collision condition.
checkModuleDeps :: World -> Module -> NCMonad ()
checkModuleDeps world mod0 = do
    let modules = NM.toList (getWorldSelfPkgModules world)
        name0 = moduleName mod0
        ascendants = filter (flip isAscendant name0 . moduleName) modules
        descendants = filter (isAscendant name0 . moduleName) modules
    mapM_ checkModuleBody ascendants -- only need type and synonym names
    mapM_ checkModuleName descendants -- only need module names
    mapM_ checkVirtualModuleName (concatMap virtualModuleNames descendants)
    checkModule mod0

-- | Check a whole package for name collisions. This is used
-- when building a DAR, which may include modules in conflict
-- that don't depend on each other.
checkPackage :: Package -> NCMonad ()
checkPackage = mapM_ checkModule . packageModules

runCheckModuleDeps :: World -> Module -> [Diagnostic]
runCheckModuleDeps w m = runNameCollision (checkModuleDeps w m)

runCheckPackage :: Package -> [Diagnostic]
runCheckPackage = runNameCollision . checkPackage

-- | Given module name A.B.C produce the virtual module names A and A.B.
virtualModuleNames :: Module -> [(ModuleName, ModuleName)]
virtualModuleNames Module{moduleName = origin@(ModuleName components)}
    | null components = error "Empty module names are invalid."
    | otherwise = map (\v -> (ModuleName v, origin))  ((tail . inits . init) components)
    -- init goes from A.B.C to A.B, inits gives us [[], [A], [A,B]], tail drops []
