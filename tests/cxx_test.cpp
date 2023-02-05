#include <catch2/catch_test_macros.hpp>
#include "utils.h"

TEST_CASE("Bounds and data checks while pushing and popping") {
    using namespace MomentumX::Utils;
    StaticVector<int32_t, 3> sv;

    // [ -- ][ -- ][ -- ]
    REQUIRE_THROWS(sv.front());
    REQUIRE_THROWS(sv.back());
    REQUIRE_THROWS(sv.at(0));
    REQUIRE(sv.begin() == sv.end());
    REQUIRE(sv.size() == 0);
    REQUIRE(sv.capacity() == 3);

    // [ 10 ][ -- ][ -- ]
    sv.push_back(10);
    REQUIRE(sv.front() == 10);
    REQUIRE(sv.back() == 10);
    REQUIRE(sv.at(0) == 10);
    REQUIRE_THROWS(sv.at(1));
    REQUIRE(sv.begin() + 1 == sv.end());
    REQUIRE(sv.size() == 1);
    REQUIRE(sv.capacity() == 3);

    // [ 10 ][ 20 ][ -- ]
    sv.push_back(20);
    REQUIRE(sv.front() == 10);
    REQUIRE(sv.back() == 20);
    REQUIRE(sv.at(0) == 10);
    REQUIRE(sv.at(1) == 20);
    REQUIRE_THROWS(sv.at(2));
    REQUIRE(sv.begin() + 2 == sv.end());
    REQUIRE(sv.size() == 2);
    REQUIRE(sv.capacity() == 3);

    // [ 10 ][ 20 ][ 30 ]
    sv.push_back(30);
    REQUIRE(sv.front() == 10);
    REQUIRE(sv.back() == 30);
    REQUIRE(sv.at(0) == 10);
    REQUIRE(sv.at(1) == 20);
    REQUIRE(sv.at(2) == 30);
    REQUIRE_THROWS(sv.at(3));
    REQUIRE(sv.begin() + 3 == sv.end());
    REQUIRE(sv.size() == 3);
    REQUIRE(sv.capacity() == 3);

    // exception check
    REQUIRE_THROWS(sv.push_back(40));

    // [ 10 ][ 20 ][ -- ]
    sv.pop_back();
    REQUIRE(sv.front() == 10);
    REQUIRE(sv.back() == 20);
    REQUIRE(sv.at(0) == 10);
    REQUIRE(sv.at(1) == 20);
    REQUIRE_THROWS(sv.at(2));
    REQUIRE(sv.begin() + 2 == sv.end());
    REQUIRE(sv.size() == 2);
    REQUIRE(sv.capacity() == 3);

    // [ 10 ][ -- ][ -- ]
    sv.pop_back();
    REQUIRE(sv.front() == 10);
    REQUIRE(sv.back() == 10);
    REQUIRE(sv.at(0) == 10);
    REQUIRE_THROWS(sv.at(1));
    REQUIRE(sv.begin() + 1 == sv.end());
    REQUIRE(sv.size() == 1);
    REQUIRE(sv.capacity() == 3);

    // [ -- ][ -- ][ -- ]
    sv.pop_back();
    REQUIRE_THROWS(sv.front());
    REQUIRE_THROWS(sv.back());
    REQUIRE_THROWS(sv.at(0));
    REQUIRE(sv.begin() == sv.end());
    REQUIRE(sv.size() == 0);
    REQUIRE(sv.capacity() == 3);

    // exception check
    REQUIRE_THROWS(sv.pop_back());
}

TEST_CASE("Iterator check") {
    using namespace MomentumX::Utils;
    StaticVector<int32_t, 10> sv;
    std::vector<int32_t> copied;

    for (int32_t i = 0; i < 50; i += 10) {
        sv.push_back(i);
    }

    for (auto i : sv) {
        copied.push_back(i);
    }

    REQUIRE(copied.size() == 5);
    REQUIRE(copied.at(0) == 0);
    REQUIRE(copied.at(1) == 10);
    REQUIRE(copied.at(2) == 20);
    REQUIRE(copied.at(3) == 30);
    REQUIRE(copied.at(4) == 40);
}