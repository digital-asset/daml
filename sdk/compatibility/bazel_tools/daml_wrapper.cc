#include "tools/cpp/runfiles/runfiles.h"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#if defined(_WIN32)
#include <codecvt>
#include <locale>
#include <windows.h>
#else
#include <climits>
#include <unistd.h>
#endif

using bazel::tools::cpp::runfiles::Runfiles;

namespace {

#if defined(_WIN32)
const char kPathSeparator = ';';
const char kJavaExecutable[] = "/bin/java.exe";
#else
const char kPathSeparator = ':';
const char kJavaExecutable[] = "/bin/java";
#endif

std::string parent_dir(const std::string &path, int levels) {
  std::string result = path;
  for (int i = 0; i < levels; ++i) {
    result = result.substr(0, result.find_last_of("/\\"));
  }
  return result;
}

std::string real_path(const std::string &path) {
#if defined(_WIN32)
  return path;
#else
  char resolved[PATH_MAX];
  return realpath(path.c_str(), resolved) ? std::string(resolved) : path;
#endif
}

void set_env(const std::string &name, const std::string &value) {
#if defined(_WIN32)
  _putenv_s(name.c_str(), value.c_str());
#else
  setenv(name.c_str(), value.c_str(), 1);
#endif
}

std::string current_repository_root() {
  std::string repository = BAZEL_CURRENT_REPOSITORY;
  return repository.empty() ? "_main" : repository;
}

std::string java_rlocation_path() {
  std::string javabase = COMPAT_JAVABASE;
  const std::string external_prefix = "external/";
  if (javabase.rfind(external_prefix, 0) == 0) {
    javabase = javabase.substr(external_prefix.size());
  }
  return javabase + kJavaExecutable;
}

#if defined(_WIN32)
int run_assistant(const std::string &assistant, char **) {
  LPWSTR old_command_line = GetCommandLineW();
  wchar_t *arguments = wcschr(old_command_line, ' ');
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  std::wstring command_line = converter.from_bytes(assistant.c_str());
  if (arguments != nullptr) {
    command_line += std::wstring(arguments);
  }
  const int kMaxCommandLineLength = 32768;
  wchar_t result[kMaxCommandLineLength];
  wcsncpy(result, command_line.c_str(), kMaxCommandLineLength - 1);
  result[kMaxCommandLineLength - 1] = 0;
  PROCESS_INFORMATION process_info = {0};
  STARTUPINFOW startup_info = {0};
  startup_info.cb = sizeof(startup_info);
  if (!CreateProcessW(NULL, result, NULL, NULL, TRUE, 0, NULL, NULL,
                      &startup_info, &process_info)) {
    std::wcerr << "Cannot launch process: " << command_line << "\n";
    return GetLastError();
  }
  WaitForSingleObject(process_info.hProcess, INFINITE);
  DWORD exit_code;
  GetExitCodeProcess(process_info.hProcess, &exit_code);
  CloseHandle(process_info.hProcess);
  CloseHandle(process_info.hThread);
  return exit_code;
}
#else
int run_assistant(const std::string &assistant, char **argv) {
  execvp(assistant.c_str(), argv);
  std::cerr << "Cannot exec " << assistant << "\n";
  return 1;
}
#endif

}

int main(int, char **argv) {
  std::string error;
  std::unique_ptr<Runfiles> runfiles(
      Runfiles::Create(argv[0], BAZEL_CURRENT_REPOSITORY, &error));
  if (runfiles == nullptr) {
    std::cerr << "Failed to initialize runfiles: " << error << "\n";
    return 1;
  }

  std::string java = real_path(runfiles->Rlocation(java_rlocation_path()));
  std::string java_home = parent_dir(java, 2);
  const char *path = getenv("PATH");
  set_env("JAVA_HOME", java_home);
  set_env("PATH", java_home + "/bin" + kPathSeparator + (path ? path : ""));

  std::string repository_root = current_repository_root() + "/";
#ifdef COMPAT_DPM_HOME_MARKER
  set_env("DPM_HOME",
          parent_dir(real_path(runfiles->Rlocation(repository_root +
                                                   COMPAT_DPM_HOME_MARKER)),
                     1));
#endif
  return run_assistant(runfiles->Rlocation(repository_root + COMPAT_ASSISTANT_PATH),
                       argv);
}
