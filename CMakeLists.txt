project(momentumx)

cmake_minimum_required(VERSION 3.15...3.25)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(SKBUILD)
  # Scikit-Build does not add your site-packages to the search path
  # automatically, so we need to add it _or_ the pybind11 specific directory
  # here.
  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -c
            "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE _tmp_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ECHO STDOUT)
  list(APPEND CMAKE_PREFIX_PATH "${_tmp_dir}")
endif()

find_package(Threads REQUIRED)

include(FetchContent)
FetchContent_Declare(
  json
  URL      https://github.com/nlohmann/json/releases/download/v3.11.2/json.tar.xz
  URL_HASH SHA256=8c4b26bf4b422252e13f332bc5e388ec0ab5c3443d24399acb675e68278d341f  # latest as of 07 Mar, 2023
)
FetchContent_MakeAvailable(json)

find_package(pybind11 CONFIG REQUIRED)

set(SRCS
    ext/buffer.cpp
    ext/buffer.h
    ext/context.cpp
    ext/context.h
    ext/control.cpp
    ext/control.h
    ext/inspector.h
    ext/stream.cpp
    ext/stream.h
    ext/utils.h
)

pybind11_add_module(_mx MODULE ${SRCS} ext/binding.cpp)
target_include_directories(_mx PRIVATE ext)
target_link_libraries(_mx PRIVATE rt Threads::Threads nlohmann_json::nlohmann_json)

install(TARGETS _mx DESTINATION .)

add_custom_command(
    TARGET _mx
    POST_BUILD
    COMMAND stubgen -o . -m _mx
    BYPRODUCTS _mx.pyi
    WORKING_DIRECTORY $<TARGET_FILE_DIR:_mx>
    VERBATIM
)

install(FILES $<TARGET_FILE_DIR:_mx>/_mx.pyi DESTINATION .)