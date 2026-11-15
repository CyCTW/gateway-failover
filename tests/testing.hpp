// Minimal self-contained test harness (no external dependency).
#pragma once

#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace t {

struct Case {
  const char* name;
  std::function<void()> fn;
};

inline std::vector<Case>& registry() {
  static std::vector<Case> r;
  return r;
}

struct Register {
  Register(const char* n, std::function<void()> f) { registry().push_back({n, std::move(f)}); }
};

struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

inline int& check_failures() {
  static int n = 0;
  return n;
}

}  // namespace t

#define T_CAT2(a, b) a##b
#define T_CAT(a, b) T_CAT2(a, b)
#define TEST(name)                                                       \
  static void T_CAT(test_fn_, __LINE__)();                               \
  static t::Register T_CAT(test_reg_, __LINE__)(name, T_CAT(test_fn_, __LINE__)); \
  static void T_CAT(test_fn_, __LINE__)()

#define CHECK(cond)                                                                       \
  do {                                                                                    \
    if (!(cond)) {                                                                        \
      std::cerr << "    CHECK failed: " #cond " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
      ++t::check_failures();                                                              \
    }                                                                                     \
  } while (0)

#define REQUIRE(cond)                                                                          \
  do {                                                                                         \
    if (!(cond)) {                                                                             \
      std::ostringstream os_;                                                                  \
      os_ << "REQUIRE failed: " #cond " (" << __FILE__ << ":" << __LINE__ << ")";              \
      throw t::Failure(os_.str());                                                             \
    }                                                                                          \
  } while (0)

#define CHECK_EQ(a, b)                                                                                 \
  do {                                                                                                 \
    const auto& va_ = (a);                                                                             \
    const auto& vb_ = (b);                                                                             \
    if (!(va_ == vb_)) {                                                                               \
      std::cerr << "    CHECK_EQ failed: " #a " == " #b " (" << va_ << " vs " << vb_ << ") (" << __FILE__ \
                << ":" << __LINE__ << ")\n";                                                           \
      ++t::check_failures();                                                                           \
    }                                                                                                  \
  } while (0)
