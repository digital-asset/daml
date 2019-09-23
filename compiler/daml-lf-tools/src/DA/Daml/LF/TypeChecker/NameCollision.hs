module DA.Daml.LF.TypeChecker.NameCollision
    ( checkModule
    ) where

import DA.Daml.LF.Ast
import DA.Daml.LF.TypeChecker.Env
import DA.Daml.LF.TypeChecker.Error
import Control.Monad.Extra
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Control.Monad.Trans.State as S

-- | Fully resolved name within a package. We don't use
-- Qualified from DA.Daml.LF.Ast because that hides collisions
-- between module names and type names. Also, we implement
-- case-insensitive Eq and Ord instances for FRName.
--
-- This corresponds to the following section of the LF spec:
-- https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst#fully-resolved-name
newtype FRName = FRName [T.Text]

instance Eq FRName where
    (==) (FRName xs) (FRName ys) =
        map T.toLower xs == map T.toLower ys

instance Ord FRName where
    compare (FRName xs) (FRName ys) =
        compare (map T.toLower xs) (map T.toLower ys)

frModuleName :: ModuleName -> FRName
frModuleName =
    FRName . unModuleName

frTypeConName :: ModuleName -> TypeConName -> FRName
frTypeConName (ModuleName m) (TypeConName t) =
    FRName (m ++ t)

frVariantConName :: ModuleName -> TypeConName -> VariantConName -> FRName
frVariantConName (ModuleName m) (TypeConName t) (VariantConName v) =
    FRName (m ++ t ++ [v])

frFieldName :: ModuleName -> TypeConName -> FieldName -> FRName
frFieldName (ModuleName m) (TypeConName t) (FieldName f) =
    FRName (m ++ t ++ [f])

frChoiceName :: ModuleName -> TypeConName -> ChoiceName -> FRName
frChoiceName (ModuleName m) (TypeConName t) (ChoiceName c) =
    FRName (m ++ t ++ [c])

-- | Pack a fully resolved name into a single Text value
-- for the purposes of displaying / error messages.
packFRName :: FRName -> T.Text
packFRName (FRName parts) = T.intercalate "." parts

-- | State of the name collision checker. This is a
-- map from fully resolved names within a package
-- to their name sources. We update the map as we
-- go along.
newtype NCState = NCState (M.Map FRName NameSource)

-- | Where a name comes from, for the purposes of
-- the name collision check.
data NameSource
    = VariantCon ModuleName
    | RecordType ModuleName
    | Other

-- | Combine two name sources only if the respective name
-- collision is permitted. The only permitted collision
-- is a single collision between a variant constructor
-- name and a record type name defined in the same module.
combineNameSource :: NameSource -> NameSource -> Maybe NameSource
combineNameSource a b =
    case (a,b) of
        (VariantCon m1, RecordType m2) | m1 == m2 -> Just Other
        (RecordType m1, VariantCon m2) | m1 == m2 -> Just Other
        _ -> Nothing

-- | Initial name collision checker state.
initialState :: NCState
initialState = NCState M.empty

-- | Try to add a name to the NCState. Returns Nothing only
-- if the name results in a forbidden name collision.
addName :: FRName -> NameSource -> NCState -> Maybe NCState
addName name source (NCState map) =
    case M.lookup name map of
        Nothing -> Just (NCState (M.insert name source map))
        Just oldSource -> do
            newSource <- combineNameSource oldSource source
            Just (NCState (M.insert name newSource map))

checkName :: MonadGamma m => FRName -> NameSource -> S.StateT NCState m ()
checkName name source = do
    ncState <- S.get
    case addName name source ncState of
        Nothing ->
            throwWithContext (EForbiddenNameCollision (packFRName name))
        Just ncState' ->
            S.put ncState'

checkDataType :: MonadGamma m => ModuleName -> DefDataType -> S.StateT NCState m ()
checkDataType moduleName DefDataType{..} =
    case dataCons of
        DataRecord fields -> do
            checkName (frTypeConName moduleName dataTypeCon)
                (RecordType moduleName)
            forM_ fields $ \(fieldName, _) -> do
                checkName (frFieldName moduleName dataTypeCon fieldName) Other

        DataVariant constrs -> do
            checkName (frTypeConName moduleName dataTypeCon) Other
            forM_ constrs $ \(vconName, _) -> do
                checkName (frVariantConName moduleName dataTypeCon vconName)
                    (VariantCon moduleName)

        DataEnum constrs -> do
            checkName (frTypeConName moduleName dataTypeCon) Other
            forM_ constrs $ \vconName -> do
                checkName (frVariantConName moduleName dataTypeCon vconName)
                    Other

checkTemplate :: MonadGamma m => ModuleName -> Template -> S.StateT NCState m ()
checkTemplate moduleName Template{..} = do
    forM_ tplChoices $ \TemplateChoice{..} ->
        checkName (frChoiceName moduleName tplTypeCon chcName) Other

-- | Check whether a module satisfies the name collision condition
-- as specified in the DAML-LF spec.
checkModule :: MonadGamma m => Module -> m ()
checkModule mod0 =
    void . flip S.runStateT initialState $ do
        checkName (frModuleName (moduleName mod0)) Other
        forM_ (moduleDataTypes mod0) $ \dataType ->
            withContext (ContextDefDataType mod0 dataType) $
                checkDataType (moduleName mod0) dataType
        forM_ (moduleTemplates mod0) $ \tpl ->
            withContext (ContextTemplate mod0 tpl TPWhole) $
                checkTemplate (moduleName mod0) tpl
