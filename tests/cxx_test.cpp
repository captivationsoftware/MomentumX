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

TEST_CASE("Remove check") {
    using namespace MomentumX::Utils;
    StaticVector<int32_t, 10> sv;

    sv.push_back(10);
    sv.push_back(11);
    sv.push_back(12);
    sv.push_back(13);
    sv.push_back(14);
    sv.push_back(15);

    // Verify test setup
    REQUIRE(sv.size() == 6);
    REQUIRE(sv.at(0) == 10);
    REQUIRE(sv.at(1) == 11);
    REQUIRE(sv.at(2) == 12);
    REQUIRE(sv.at(3) == 13);
    REQUIRE(sv.at(4) == 14);
    REQUIRE(sv.at(5) == 15);

    // First element test
    sv.erase(sv.begin());
    REQUIRE(sv.size() == 5);
    REQUIRE(sv.at(0) == 11);
    REQUIRE(sv.at(1) == 12);
    REQUIRE(sv.at(2) == 13);
    REQUIRE(sv.at(3) == 14);
    REQUIRE(sv.at(4) == 15);

    // Middle element test
    sv.erase(sv.begin() + 2);
    REQUIRE(sv.size() == 4);
    REQUIRE(sv.at(0) == 11);
    REQUIRE(sv.at(1) == 12);
    REQUIRE(sv.at(2) == 14);
    REQUIRE(sv.at(3) == 15);

    // Last element
    sv.erase(sv.end() - 1);
    REQUIRE(sv.size() == 3);
    REQUIRE(sv.at(0) == 11);
    REQUIRE(sv.at(1) == 12);
    REQUIRE(sv.at(2) == 14);

    // Setup full vector
    sv.push_back(20);
    sv.push_back(21);
    sv.push_back(22);
    sv.push_back(23);
    sv.push_back(24);
    sv.push_back(25);
    sv.push_back(26);
    REQUIRE(sv.size() == 10);

    // Last element when full
    sv.erase(sv.end() - 1);
    REQUIRE(sv.size() == 9);
    REQUIRE(sv.at(0) == 11);
    REQUIRE(sv.at(1) == 12);
    REQUIRE(sv.at(2) == 14);
    REQUIRE(sv.at(3) == 20);
    REQUIRE(sv.at(4) == 21);
    REQUIRE(sv.at(5) == 22);
    REQUIRE(sv.at(6) == 23);
    REQUIRE(sv.at(7) == 24);
    REQUIRE(sv.at(8) == 25);
}