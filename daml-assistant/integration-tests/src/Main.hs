module Main (main) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Exception
import Control.Monad
import Data.Typeable
import Network.Socket
import System.Directory.Extra
import System.Environment
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Main
import Test.Tasty
import Test.Tasty.HUnit

import DA.Bazel.Runfiles
import DamlHelper

main :: IO ()
main =
    withTempDir $ \tmpDir -> do
    -- We manipulate global state via the working directory and
    -- the environment so running tests in parallel will cause trouble.
    setEnv "TASTY_NUM_THREADS" "1"
    oldPath <- getEnv "PATH"
    javaPath <- locateRunfiles "local_jdk/bin"
    let damlDir = tmpDir </> "daml"
    withEnv
        [ ("DAML_HOME", Just damlDir)
        , ("PATH", Just $ (damlDir </> "bin") <> ":" <> javaPath <> ":" <> oldPath)
        ] $ defaultMain (tests tmpDir)

tests :: FilePath -> TestTree
tests tmpDir = testGroup "Integration tests"
    [ testCase "install" $ do
          releaseTarball <- locateRunfiles (mainWorkspace </> "release" </> "sdk-release-tarball.tar.gz")
          createDirectory tarballDir
          callProcessQuiet "tar" ["xf", releaseTarball, "--strip-components=1", "-C", tarballDir]
          callProcessQuiet (tarballDir </> "install.sh") []
    , testCase "daml version" $ callProcessQuiet "daml" ["version"]
    , testCase "daml --help" $ callProcessQuiet "daml" ["--help"]
    , testCase "daml new --list" $ callProcessQuiet "daml" ["new", "--list"]
    , quickstartTests quickstartDir
    ]
    where quickstartDir = tmpDir </> "quickstart"
          tarballDir = tmpDir </> "tarball"

quickstartTests :: FilePath -> TestTree
quickstartTests quickstartDir = testGroup "quickstart"
    [ testCase "daml new" $
          callProcessQuiet "daml" ["new", quickstartDir]
    , testCase "daml package" $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["package", "daml/Main.daml", "target/daml/iou"]
    , testCase "daml test" $ withCurrentDirectory quickstartDir $
          callProcessQuiet "daml" ["test", "daml/Main.daml"]
    , testCase "sandbox startup" $
      withCurrentDirectory quickstartDir $
      withDevNull $ \devNull -> do
          p :: Int <- fromIntegral <$> getFreePort
          withCreateProcess ((proc "daml" ["sandbox", "--port", show p, "target/daml/iou.dar"]) { std_out = UseHandle devNull }) $
              \_ _ _ ph -> race_ (waitForProcess' "sandbox" [] ph) $ do
              waitForConnectionOnPort (threadDelay 100000) p
              addr : _ <- getAddrInfo
                  (Just socketHints)
                  (Just "127.0.0.1")
                  (Just $ show p)
              bracket
                  (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
                  close
                  (\s -> connect s (addrAddress addr))
    ]

-- | Like call process but hides stdout.
callProcessQuiet :: FilePath -> [String] -> IO ()
callProcessQuiet cmd args = do
    (exit, _out, err) <- readProcessWithExitCode cmd args ""
    hPutStr stderr err
    unless (exit == ExitSuccess) $ throwIO $ ProcessExitFailure exit cmd args

data ProcessExitFailure = ProcessExitFailure !ExitCode !FilePath ![String]
    deriving (Show, Typeable)

instance Exception ProcessExitFailure

-- This is slightly hacky: we need to find a free port but pass it to an
-- external process. Technically this port could be reused between us
-- getting it from the kernel and the external process listening
-- on that port but ports are usually not reused aggressively so this should
-- be fine and is certainly better than hardcoding the port.
getFreePort :: IO PortNumber
getFreePort = do
    addr : _ <- getAddrInfo
        (Just socketHints)
        (Just "127.0.0.1")
        (Just "0")
    bracket
        (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
        close
        (\s -> do bind s (addrAddress addr)
                  name <- getSocketName s
                  case name of
                      SockAddrInet p _ -> pure p
                      _ -> fail $ "Expected a SockAddrInet but got " <> show name)

socketHints :: AddrInfo
socketHints = defaultHints { addrFlags = [AI_NUMERICHOST, AI_NUMERICSERV], addrSocketType = Stream }

-- | Like waitForProcess' but throws ProcessExitFailure if the process fails to start.
waitForProcess' :: String -> [String] -> ProcessHandle -> IO ()
waitForProcess' cmd args ph = do
    e <- waitForProcess ph
    unless (e == ExitSuccess) $ throwIO $ ProcessExitFailure e cmd args

-- | Getting a dev-null handle in a cross-platform way seems to be somewhat tricky so we instead
-- use a temporary file.
withDevNull :: (Handle -> IO a) -> IO a
withDevNull a = withTempFile $ \f -> withFile f WriteMode a
