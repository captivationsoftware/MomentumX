project(momentumx)

cmake_minimum_required(VERSION 3.15...3.25)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# project(${SKBUILD_PROJECT_NAME} VERSION ${SKBUILD_PROJECT_VERSION})

# include(ext)

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

find_package(pybind11 CONFIG REQUIRED)

set(SRCS
    ext/buffer.cpp
    ext/buffer.h
    ext/context.cpp
    ext/context.h
    ext/momentumx.cpp
    ext/momentumx.h
    ext/stream.cpp
    ext/stream.h
    ext/utils.h
)

pybind11_add_module(_mx MODULE ${SRCS} ext/binding.cpp)
target_include_directories(_mx PRIVATE ext)
target_link_libraries(_mx PRIVATE rt)

install(TARGETS _mx DESTINATION .)

target_link_libraries(_mx PRIVATE rt)

add_custom_command(
    TARGET _mx
    POST_BUILD
    COMMAND stubgen -o . -m _mx
    BYPRODUCTS _mx.pyi
    WORKING_DIRECTORY $<TARGET_FILE_DIR:_mx>
    VERBATIM
)

install(FILES $<TARGET_FILE_DIR:_mx>/_mx.pyi DESTINATION .)

# Enable C++ unit tests

include(FetchContent)

FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        2ab20a0e008845e02bd06248e61ca6e5ad1aba33 # v3.3.1
)

FetchContent_MakeAvailable(Catch2)

enable_testing()
include(CTest)
add_executable(mx_test  tests/cxx_test.cpp)
target_include_directories(mx_test PRIVATE ext)
target_link_libraries(mx_test PRIVATE Catch2::Catch2WithMain)