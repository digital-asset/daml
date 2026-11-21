#include "tools/cpp/runfiles/runfiles.h"
#include <codecvt>
#include <iostream>
#include <locale>
#include <memory>
#include <unistd.h>
#include <cstring>
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
    std::string java = "";
    // NOTE (MK) I don’t entirely understand when Bazel defines
    // JAVA_HOME and when it doesn’t but it looks like it defines
    // it in tests but not in genrules which is good enough for us.
    if (java_home) {
#if defined(_WIN32)
        java = std::string(java_home) + "/bin/java.exe";
#else
        java = std::string(java_home) + "/bin/java";
#endif
    }
    std::string path =
        runfiles->Rlocation("daml-sdk-{SDK_VERSION}/{JAR_NAME}");
#if defined(_WIN32)
    // To keep things as simple as possible, we simply take
    // the existing command line, strip everything up to the first space
    // and replace it. This is obviously not correct if there are spaces
    // in the path name but Bazel doesn’t like that either so
    // it should be good enough™.
    LPWSTR oldCmdLine = GetCommandLineW();
    wchar_t *index = wcschr(oldCmdLine, ' ');
    std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
    std::wstring cmdLine = converter.from_bytes(java.c_str());
    cmdLine += L" -jar ";
    cmdLine += converter.from_bytes(path.c_str());
    if (index) {
        cmdLine += std::wstring(index);
    }
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
    // we replace argv[0], 2 extra args for -jar and the path to the jar
    // and then the terminating NULL so 3 in total.
    char**const argv_ = (char**)malloc((argc + 3) * sizeof(char*));
    char* javaStr = new char[java.length() + 1];
    std::strcpy(javaStr, java.c_str());
    argv_[0] = javaStr;
    char* jarStr = new char[strlen("-jar") + 1];
    std::strcpy(jarStr, "-jar");
    argv_[1] = jarStr;
    char* pathStr = new char[path.length() + 1];
    std::strcpy(pathStr, path.c_str());
    argv_[2] = pathStr;
    for (int i = 1; i < argc; i++) {
        argv_[i + 2] = argv[i];
    }
    argv_[argc + 2] = NULL;
    execvp(java.c_str(), argv_);
#endif
}
