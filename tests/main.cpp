#include <cstring>

#include "testing.hpp"

int main(int argc, char** argv) {
  const char* filter = argc > 1 ? argv[1] : nullptr;
  int failed = 0, ran = 0;
  for (const auto& c : t::registry()) {
    if (filter && !std::strstr(c.name, filter)) continue;
    ++ran;
    const int before = t::check_failures();
    bool ok = true;
    try {
      c.fn();
    } catch (const std::exception& e) {
      std::cerr << "    " << e.what() << "\n";
      ok = false;
    }
    if (t::check_failures() != before) ok = false;
    std::cout << (ok ? "[ PASS ] " : "[ FAIL ] ") << c.name << "\n";
    failed += !ok;
  }
  std::cout << "\n" << ran - failed << "/" << ran << " tests passed\n";
  return failed ? 1 : 0;
}
