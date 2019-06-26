-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE OverloadedStrings          #-}

-- | A Shake implementation of the compiler service.
--
--   There are two primary locations where data lives, and both of
--   these contain much the same data:
--
-- * The Shake database (inside 'shakeDb') stores a map of shake keys
--   to shake values. In our case, these are all of type 'Q' to 'A'.
--   During a single run all the values in the Shake database are consistent
--   so are used in conjunction with each other, e.g. in 'uses'.
--
-- * The 'Values' type stores a map of keys to values. These values are
--   always stored as real Haskell values, whereas Shake serialises all 'A' values
--   between runs. To deserialise a Shake value, we just consult Values.
--   Additionally, Values can be used in an inconsistent way, for example
--   useStale.
module Development.IDE.Core.Shake(
    IdeState,
    IdeRule, IdeResult, GetModificationTime(..),
    shakeOpen, shakeShut,
    shakeRun,
    shakeProfile,
    useStale,
    use, uses,
    use_, uses_,
    define, defineEarlyCutoff,
    getDiagnostics, unsafeClearDiagnostics,
    reportSeriousError,
    IsIdeGlobal, addIdeGlobal, getIdeGlobalState, getIdeGlobalAction,
    garbageCollect,
    setPriority,
    sendEvent,
    ideLogger,
    FileVersion(..)
    ) where

import           Development.Shake
import           Development.Shake.Database
import           Development.Shake.Classes
import           Development.Shake.Rule
import qualified Data.HashMap.Strict as HMap
import qualified Data.Map as Map
import qualified Data.ByteString.Char8 as BS
import           Data.Dynamic
import           Data.Maybe
import           Data.Either.Extra
import           Data.List.Extra
import qualified Data.Text as T
import Development.IDE.Types.Logger
import Language.Haskell.LSP.Diagnostics
import qualified Data.SortedList as SL
import           Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import           Control.Concurrent.Extra
import           Control.Exception
import           Control.DeepSeq
import           System.Time.Extra
import           Data.Typeable
import qualified Language.Haskell.LSP.Messages as LSP
import qualified Language.Haskell.LSP.Types as LSP
import           System.FilePath hiding (makeRelative)
import qualified Development.Shake as Shake
import           Control.Monad.Extra
import           Data.Time
import           GHC.Generics
import           System.IO.Unsafe
import           Numeric.Extra



-- information we stash inside the shakeExtra field
data ShakeExtras = ShakeExtras
    {eventer :: LSP.FromServerMessage -> IO ()
    ,logger :: Logger
    ,globals :: Var (HMap.HashMap TypeRep Dynamic)
    ,state :: Var Values
    ,diagnostics :: Var DiagnosticStore
    }

getShakeExtras :: Action ShakeExtras
getShakeExtras = do
    Just x <- getShakeExtra @ShakeExtras
    return x

getShakeExtrasRules :: Rules ShakeExtras
getShakeExtrasRules = do
    Just x <- getShakeExtraRules @ShakeExtras
    return x



class Typeable a => IsIdeGlobal a where

addIdeGlobal :: IsIdeGlobal a => a -> Rules ()
addIdeGlobal x@(typeOf -> ty) = do
    ShakeExtras{globals} <- getShakeExtrasRules
    liftIO $ modifyVar_ globals $ \mp -> case HMap.lookup ty mp of
        Just _ -> error $ "Can't addIdeGlobal twice on the same type, got " ++ show ty
        Nothing -> return $! HMap.insert ty (toDyn x) mp


getIdeGlobalExtras :: forall a . IsIdeGlobal a => ShakeExtras -> IO a
getIdeGlobalExtras ShakeExtras{globals} = do
    Just x <- HMap.lookup (typeRep (Proxy :: Proxy a)) <$> readVar globals
    return $ fromDyn x $ error "Serious error, corrupt globals"

getIdeGlobalAction :: forall a . IsIdeGlobal a => Action a
getIdeGlobalAction = liftIO . getIdeGlobalExtras =<< getShakeExtras

getIdeGlobalState :: forall a . IsIdeGlobal a => IdeState -> IO a
getIdeGlobalState = getIdeGlobalExtras . shakeExtras


-- | The state of the all values - nested so you can easily find all errors at a given file.
type Values = HMap.HashMap (NormalizedFilePath, Key) (Maybe Dynamic)

-- | Key type
data Key = forall k . (Typeable k, Hashable k, Eq k, Show k) => Key k

instance Show Key where
  show (Key k) = show k

instance Eq Key where
    Key k1 == Key k2 | Just k2' <- cast k2 = k1 == k2'
                     | otherwise = False

instance Hashable Key where
    hashWithSalt salt (Key key) = hashWithSalt salt key

-- | The result of an IDE operation. Warnings and errors are in the Diagnostic,
--   and a value is in the Maybe. For operations that throw an error you
--   expect a non-empty list of diagnostics, at least one of which is an error,
--   and a Nothing. For operations that succeed you expect perhaps some warnings
--   and a Just. For operations that depend on other failing operations you may
--   get empty diagnostics and a Nothing, to indicate this phase throws no fresh
--   errors but still failed.
--
--   A rule on a file should only return diagnostics for that given file. It should
--   not propagate diagnostic errors through multiple phases.
type IdeResult v = ([FileDiagnostic], Maybe v)

type IdeRule k v =
  ( Shake.RuleResult k ~ v
  , Show k
  , Typeable k
  , NFData k
  , Hashable k
  , Eq k
  , Show v
  , Typeable v
  , NFData v
  )


-- | A Shake database plus persistent store. Can be thought of as storing
--   mappings from @(FilePath, k)@ to @RuleResult k@.
data IdeState = IdeState
    {shakeDb :: ShakeDatabase
    ,shakeAbort :: Var (IO ()) -- close whoever was running last
    ,shakeClose :: IO ()
    ,shakeExtras :: ShakeExtras
    }

profileDir :: Maybe FilePath
profileDir = Nothing -- set to Just the directory you want profile reports to appear in


-- This is debugging code that generates a series of profiles, if the Boolean is true
shakeRunDatabaseProfile :: ShakeDatabase -> [Action a] -> IO [a]
shakeRunDatabaseProfile shakeDb acts = do
        (time, (res,_)) <- duration $ shakeRunDatabase shakeDb acts
        whenJust profileDir $ \dir -> do
            count <- modifyVar profileCounter $ \x -> let y = x+1 in return (y,y)
            let file = "ide-" ++ profileStartTime ++ "-" ++ takeEnd 5 ("0000" ++ show count) ++ "-" ++ showDP 2 time <.> "html"
            shakeProfileDatabase shakeDb $ dir </> file
        return res
    where

{-# NOINLINE profileStartTime #-}
profileStartTime :: String
profileStartTime = unsafePerformIO $ formatTime defaultTimeLocale "%Y%m%d-%H%M%S" <$> getCurrentTime

{-# NOINLINE profileCounter #-}
profileCounter :: Var Int
profileCounter = unsafePerformIO $ newVar 0

setValues :: IdeRule k v
          => Var Values
          -> k
          -> NormalizedFilePath
          -> Maybe v
          -> IO ()
setValues state key file val = modifyVar_ state $
    pure . HMap.insert (file, Key key) (fmap toDyn val)

-- | The outer Maybe is Nothing if this function hasn't been computed before
--   the inner Maybe is Nothing if the result of the previous computation failed to produce
--   a value
getValues :: forall k v. IdeRule k v => Var Values -> k -> NormalizedFilePath -> IO (Maybe (Maybe v))
getValues state key file = do
    vs <- readVar state
    return $ do
        v <- HMap.lookup (file, Key key) vs
        pure $ fmap (fromJust . fromDynamic @v) v

-- | Open a 'IdeState', should be shut using 'shakeShut'.
shakeOpen :: (LSP.FromServerMessage -> IO ()) -- ^ diagnostic handler
          -> Logger
          -> ShakeOptions
          -> Rules ()
          -> IO IdeState
shakeOpen eventer logger opts rules = do
    shakeExtras <- do
        globals <- newVar HMap.empty
        state <- newVar HMap.empty
        diagnostics <- newVar mempty
        pure ShakeExtras{..}
    (shakeDb, shakeClose) <- shakeOpenDatabase opts{shakeExtra = addShakeExtra shakeExtras $ shakeExtra opts} rules
    shakeAbort <- newVar $ return ()
    shakeDb <- shakeDb
    return IdeState{..}

shakeProfile :: IdeState -> FilePath -> IO ()
shakeProfile IdeState{..} = shakeProfileDatabase shakeDb

shakeShut :: IdeState -> IO ()
shakeShut IdeState{..} = withVar shakeAbort $ \stop -> do
    -- Shake gets unhappy if you try to close when there is a running
    -- request so we first abort that.
    stop
    shakeClose

-- | Spawn immediately, add an action to collect the results syncronously.
--   If you are already inside a call to shakeRun that will be aborted with an exception.
-- The callback will be fired as soon as the results are available
-- even if there are still other rules running while the IO action that is
-- being returned will wait for all rules to finish.
shakeRun :: IdeState -> [Action a] -> ([a] -> IO ()) -> IO (IO [a])
-- FIXME: If there is already a shakeRun queued up and waiting to send me a kill, I should probably
--        not even start, which would make issues with async exceptions less problematic.
shakeRun IdeState{shakeExtras=ShakeExtras{..}, ..} acts callback = modifyVar shakeAbort $ \stop -> do
    (stopTime,_) <- duration stop
    logDebug logger $ T.pack $ "Starting shakeRun (aborting the previous one took " ++ showDuration stopTime ++ ")"
    bar <- newBarrier
    start <- offsetTime
    let act = do
            res <- parallel acts
            liftIO $ callback res
            pure res
    thread <- forkFinally (shakeRunDatabaseProfile shakeDb [act]) $ \res -> do
        signalBarrier bar (mapRight head res)
        runTime <- start
        logDebug logger $ T.pack $
            "Finishing shakeRun (took " ++ showDuration runTime ++ ", " ++ (if isLeft res then "exception" else "completed") ++ ")"
    -- important: we send an async exception to the thread, then wait for it to die, before continuing
    return (do killThread thread; void $ waitBarrier bar, either throwIO return =<< waitBarrier bar)

-- | Use the last stale value, if it's ever been computed.
useStale
    :: IdeRule k v
    => IdeState -> k -> NormalizedFilePath -> IO (Maybe v)
useStale IdeState{shakeExtras=ShakeExtras{state}} k fp =
    join <$> getValues state k fp


getDiagnostics :: IdeState -> IO [FileDiagnostic]
getDiagnostics IdeState{shakeExtras = ShakeExtras{diagnostics}} = do
    val <- readVar diagnostics
    return $ getAllDiagnostics val

-- | FIXME: This function is temporary! Only required because the files of interest doesn't work
unsafeClearDiagnostics :: IdeState -> IO ()
unsafeClearDiagnostics IdeState{shakeExtras = ShakeExtras{diagnostics}} =
    writeVar diagnostics mempty

-- | Clear the results for all files that do not match the given predicate.
garbageCollect :: (NormalizedFilePath -> Bool) -> Action ()
garbageCollect keep = do
    ShakeExtras{state, diagnostics} <- getShakeExtras
    liftIO $
        do modifyVar_ state $ return . HMap.filterWithKey (\(file, _) _ -> keep file)
           modifyVar_ diagnostics $ return . filterDiagnostics keep

define
    :: IdeRule k v
    => (k -> NormalizedFilePath -> Action (IdeResult v)) -> Rules ()
define op = defineEarlyCutoff $ \k v -> (Nothing,) <$> op k v

use :: IdeRule k v
    => k -> NormalizedFilePath -> Action (Maybe v)
use key file = head <$> uses key [file]

use_ :: IdeRule k v => k -> NormalizedFilePath -> Action v
use_ key file = head <$> uses_ key [file]

uses_ :: IdeRule k v => k -> [NormalizedFilePath] -> Action [v]
uses_ key files = do
    res <- uses key files
    case sequence res of
        Nothing -> liftIO $ throwIO BadDependency
        Just v -> return v

reportSeriousError :: String -> Action ()
reportSeriousError t = do
    ShakeExtras{logger} <- getShakeExtras
    liftIO $ logSeriousError logger $ T.pack t


-- | When we depend on something that reported an error, and we fail as a direct result, throw BadDependency
--   which short-circuits the rest of the action
data BadDependency = BadDependency deriving Show
instance Exception BadDependency

isBadDependency :: SomeException -> Bool
isBadDependency x
    | Just (x :: ShakeException) <- fromException x = isBadDependency $ shakeExceptionInner x
    | Just (_ :: BadDependency) <- fromException x = True
    | otherwise = False

newtype Q k = Q (k, NormalizedFilePath)
    deriving (Eq,Hashable,NFData)

-- Using Database we don't need Binary instances for keys
instance Binary (Q k) where
    put _ = return ()
    get = fail "Binary.get not defined for type Development.IDE.Core.Shake.Q"

instance Show k => Show (Q k) where
    show (Q (k, file)) = show k ++ "; " ++ fromNormalizedFilePath file

-- | Invariant: the 'v' must be in normal form (fully evaluated).
--   Otherwise we keep repeatedly 'rnf'ing values taken from the Shake database
data A v = A (Maybe v) (Maybe BS.ByteString)
    deriving Show

instance NFData (A v) where rnf (A v x) = v `seq` rnf x

-- In the Shake database we only store one type of key/result pairs,
-- namely Q (question) / A (answer).
type instance RuleResult (Q k) = A (RuleResult k)


-- | Compute the value
uses :: IdeRule k v
    => k -> [NormalizedFilePath] -> Action [Maybe v]
uses key files = map (\(A value _) -> value) <$> apply (map (Q . (key,)) files)

defineEarlyCutoff
    :: IdeRule k v
    => (k -> NormalizedFilePath -> Action (Maybe BS.ByteString, IdeResult v))
    -> Rules ()
defineEarlyCutoff op = addBuiltinRule noLint noIdentity $ \(Q (key, file)) old mode -> do
    extras@ShakeExtras{state} <- getShakeExtras
    val <- case old of
        Just old | mode == RunDependenciesSame -> do
            v <- liftIO $ getValues state key file
            case v of
                Just v -> return $ Just $ RunResult ChangedNothing old $ A v (unwrap old)
                _ -> return Nothing
        _ -> return Nothing
    case val of
        Just res -> return res
        Nothing -> do
            (bs, (diags, res)) <- actionCatch
                (do v <- op key file; liftIO $ evaluate $ force v) $
                \(e :: SomeException) -> pure (Nothing, ([ideErrorText file $ T.pack $ show e | not $ isBadDependency e],Nothing))

            liftIO $ setValues state key file res
            updateFileDiagnostics file (Key key) extras $ map snd diags
            let eq = case (bs, fmap unwrap old) of
                    (Just a, Just (Just b)) -> a == b
                    _ -> False
            return $ RunResult
                (if eq then ChangedRecomputeSame else ChangedRecomputeDiff)
                (wrap bs)
                $ A res bs
    where
        wrap = maybe BS.empty (BS.cons '_')
        unwrap x = if BS.null x then Nothing else Just $ BS.tail x

updateFileDiagnostics ::
     NormalizedFilePath
  -> Key
  -> ShakeExtras
  -> [Diagnostic] -- ^ current results
  -> Action ()
updateFileDiagnostics fp k ShakeExtras{diagnostics, state} current = do
    (newDiags, oldDiags) <- liftIO $ do
        modTime <- join <$> getValues state GetModificationTime fp
        modifyVar diagnostics $ \old -> do
            let oldDiags = getFileDiagnostics fp old
            let newDiagsStore = setStageDiagnostics fp (vfsVersion =<< modTime) (T.pack $ show k) current old
            let newDiags = getFileDiagnostics fp newDiagsStore
            pure (newDiagsStore, (newDiags, oldDiags))
    when (newDiags /= oldDiags) $
        sendEvent $ publishDiagnosticsNotification fp newDiags

publishDiagnosticsNotification :: NormalizedFilePath -> [Diagnostic] -> LSP.FromServerMessage
publishDiagnosticsNotification fp diags =
    LSP.NotPublishDiagnostics $
    LSP.NotificationMessage "2.0" LSP.TextDocumentPublishDiagnostics $
    LSP.PublishDiagnosticsParams (fromNormalizedUri $ filePathToUri' fp) (List diags)

setPriority :: (Enum a) => a -> Action ()
setPriority p =
    deprioritize (fromIntegral . negate $ fromEnum p)

sendEvent :: LSP.FromServerMessage -> Action ()
sendEvent e = do
    ShakeExtras{eventer} <- getShakeExtras
    liftIO $ eventer e

ideLogger :: IdeState -> Logger
ideLogger IdeState{shakeExtras=ShakeExtras{logger}} = logger


data GetModificationTime = GetModificationTime
    deriving (Eq, Show, Generic)
instance Hashable GetModificationTime
instance NFData   GetModificationTime

-- | Get the modification time of a file.
type instance RuleResult GetModificationTime = FileVersion

data FileVersion = VFSVersion Int | ModificationTime UTCTime
    deriving (Show, Generic)

instance NFData FileVersion

vfsVersion :: FileVersion -> Maybe Int
vfsVersion (VFSVersion i) = Just i
vfsVersion (ModificationTime _) = Nothing



getDiagnosticsFromStore :: StoreItem -> [Diagnostic]
getDiagnosticsFromStore (StoreItem _ diags) = concatMap SL.fromSortedList $ Map.elems diags


-- | Sets the diagnostics for a file and compilation step
--   if you want to clear the diagnostics call this with an empty list
setStageDiagnostics ::
  NormalizedFilePath ->
  Maybe Int ->
  -- ^ the time that the file these diagnostics originate from was last edited
  T.Text ->
  [LSP.Diagnostic] ->
  DiagnosticStore ->
  DiagnosticStore
setStageDiagnostics fp timeM stage diags ds  =
    updateDiagnostics ds uri timeM diagsBySource
    where
        diagsBySource = Map.singleton (Just stage) (SL.toSortedList diags)
        uri = filePathToUri' fp

getAllDiagnostics ::
    DiagnosticStore ->
    [FileDiagnostic]
getAllDiagnostics =
    concatMap (\(k,v) -> map (fromUri k,) $ getDiagnosticsFromStore v) . Map.toList

getFileDiagnostics ::
    NormalizedFilePath ->
    DiagnosticStore ->
    [LSP.Diagnostic]
getFileDiagnostics fp ds =
    maybe [] getDiagnosticsFromStore $
    Map.lookup (filePathToUri' fp) ds

filterDiagnostics ::
    (NormalizedFilePath -> Bool) ->
    DiagnosticStore ->
    DiagnosticStore
filterDiagnostics keep =
    Map.filterWithKey (\uri _ -> maybe True (keep . toNormalizedFilePath) $ uriToFilePath' $ fromNormalizedUri uri)
