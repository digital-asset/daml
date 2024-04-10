package main

import (
	"log"
	"os"
	"os/exec"
	"path"
	"syscall"
)

func main() {
	// This works due to the expected sandbox layout:
	//
	// ./bazel-out/host/bin/external/build_stack_rules_proto/scala/compiler_plugin_distribute.jar
	// ./bazel-out/host/bin/external/build_stack_rules_proto/scala/linux_amd64_stripped
	// ./bazel-out/host/bin/external/build_stack_rules_proto/scala/linux_amd64_stripped/protoc-gen-scala

	jar := mustFindInSandbox(path.Dir(os.Args[0]), "compiler_plugin_distribute.jar")
	err, exitCode := run("external/local_jdk/bin/java", append([]string{"-jar", jar}, os.Args...), ".", nil)
	if err != nil {
		log.Printf("%v", err)
	}
	os.Exit(exitCode)
}

func mustFindInSandbox(dir, file string) string {
	attempts := 0
	for {
		// Just in case we have a bug that will loop forever in some random
		// filesystem pattern we haven't thought of
		if attempts > 1000 {
			log.Fatalf("Too many attempts to find %s within %s", file, dir)
		}
		if dir == "" {
			log.Fatalf("Failed to find %s within %s", file, dir)
		}
		abs := path.Join(dir, file)
		if exists(abs) {
			return abs
		}
		dir = path.Dir(dir)
		attempts++
	}
}

// exists - return true if a file entry exists
func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// run a command
func run(entrypoint string, args []string, dir string, env []string) (error, int) {
	cmd := exec.Command(entrypoint, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	cmd.Dir = dir
	err := cmd.Run()

	var exitCode int
	if err != nil {
		// try to get the exit code
		if exitError, ok := err.(*exec.ExitError); ok {
			ws := exitError.Sys().(syscall.WaitStatus)
			exitCode = ws.ExitStatus()
		} else {
			// This will happen (in OSX) if `name` is not available in $PATH,
			// in this situation, exit code could not be get, and stderr will be
			// empty string very likely, so we use the default fail code, and format err
			// to string and set to stderr
			log.Printf("Could not get exit code for failed program: %v, %v", entrypoint, args)
			exitCode = -1
		}
	} else {
		// success, exitCode should be 0 if go is ok
		ws := cmd.ProcessState.Sys().(syscall.WaitStatus)
		exitCode = ws.ExitStatus()
	}
	return err, exitCode
}
