cmake_minimum_required(VERSION 3.15...3.25)
project(momentumx_static)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)

FetchContent_Declare(boost
  URL      https://archives.boost.io/release/1.81.0/source/boost_1_81_0.tar.gz
  URL_HASH SHA256=205666dea9f6a7cfed87c7a6dfbeb52a2c1b9de55712c9c1a87735d7181452b6
)
FetchContent_MakeAvailable(boost)

FetchContent_Declare(json
  URL      https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
  URL_HASH SHA256=8c4b26bf4b422252e13f332bc5e388ec0ab5c3443d24399acb675e68278d341f
)
FetchContent_MakeAvailable(json)

set(SRCS
    buffer.cpp
    buffer.h
    context.cpp
    context.h
    control.cpp
    control.h
    stream.cpp
    stream.h
    inspector.h
    utils.h
    mx_c_api.cpp
    mx_c_api.h
)

add_library(momentumx_static STATIC ${SRCS})
target_include_directories(momentumx_static SYSTEM PRIVATE ${boost_SOURCE_DIR})
target_include_directories(momentumx_static PRIVATE .)
target_link_libraries(momentumx_static PRIVATE rt nlohmann_json::nlohmann_json)

install(TARGETS momentumx_static DESTINATION lib)
install(FILES mx_c_api.h DESTINATION include)
