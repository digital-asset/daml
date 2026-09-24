#include "tools/cpp/runfiles/runfiles.h"
#include <codecvt>
#include <iostream>
#include <locale>
#include <memory>
#include <unistd.h>
#if defined(_WIN32)
#include <windows.h>
#endif

using bazel::tools::cpp::runfiles::Runfiles;

/*
NOTE (MK): Why is this not just an sh_binary?
Good question! It turns out that on Windows, an
sh_binary creates a custom executable that then launches
bash. Somehow this seems to result in a double fork
(or the Windows-equivalent) thereof which results in
child processes not being childs of the original process.
This makes it really hard to kill a process including
all child processes which is really important since
otherwise you are left with a running JVM process
if you kill `daml sandbox`.
*/

int main(int argc, char **argv) {
    std::string error;
    std::unique_ptr<Runfiles> runfiles(Runfiles::Create(argv[0], &error));
    if (runfiles == nullptr) {
        std::cerr << "Failed to initialize runfiles: " << error << "\n";
        exit(1);
    }
    const char *java_home = getenv("JAVA_HOME");
    std::string path_prefix = "";
    // NOTE (MK) I don’t entirely understand when Bazel defines
    // JAVA_HOME and when it doesn’t but it looks like it defines
    // it in tests but not in genrules which is good enough for us.
    if (java_home) {
#if defined(_WIN32)
        path_prefix = std::string(java_home) + "/bin;";
#else
        path_prefix = std::string(java_home) + "/bin:";
#endif
    }
    std::string new_path = path_prefix + getenv("PATH");
#if defined(_WIN32)
    _putenv_s("PATH", new_path.c_str());
#else
    setenv("PATH", new_path.c_str(), 1);
#endif
    std::string path =
        runfiles->Rlocation("daml-sdk-{SDK_VERSION}/sdk/bin/daml");
#if defined(_WIN32)
    // To keep things as simple as possible, we simply take
    // the existing command line, strip everything up to the first space
    // and replace it. This is obviously not correct if there are spaces
    // in the path name but Bazel doesn’t like that either so
    // it should be good enough™.
    LPWSTR oldCmdLine = GetCommandLineW();
    wchar_t *index = wcschr(oldCmdLine, ' ');
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    std::wstring cmdLine = converter.from_bytes(path.c_str());
    cmdLine += std::wstring(index);
    const int MAX_CMDLINE_LENGTH = 32768;
    wchar_t result[MAX_CMDLINE_LENGTH];
    wcsncpy(result, cmdLine.c_str(), MAX_CMDLINE_LENGTH - 1);
    result[MAX_CMDLINE_LENGTH - 1] = 0;
    PROCESS_INFORMATION processInfo = {0};
    STARTUPINFOW startupInfo = {0};
    startupInfo.cb = sizeof(startupInfo);
    BOOL ok = CreateProcessW(NULL, result, NULL, NULL, TRUE, 0, NULL, NULL,
                             &startupInfo, &processInfo);
    if (!ok) {
        std::wcerr << "Cannot launch process: " << cmdLine << "\n";
        return GetLastError();
    }
    WaitForSingleObject(processInfo.hProcess, INFINITE);
    DWORD exit_code;
    GetExitCodeProcess(processInfo.hProcess, &exit_code);
    CloseHandle(processInfo.hProcess);
    CloseHandle(processInfo.hThread);
    return exit_code;

#else
    execvp(path.c_str(), argv);
#endif
}
