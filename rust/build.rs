use cmake::Config;
use std::{env, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=cpp/CMakeLists.txt");
    println!("cargo:rerun-if-env-changed=MOMENTUMX_BUILD_DIR");

    // Fast path: caller already has a built lib directory (e.g. `make install`
    // in the monorepo sets MOMENTUMX_BUILD_DIR to the installed package's lib/).
    if let Ok(dir) = env::var("MOMENTUMX_BUILD_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
        link_libs();
        return;
    }

    // Build from source via cmake.
    // Published crate:  C++ source is staged in cpp/ by the release workflow.
    // Monorepo fallback: use the repo root (requires pybind11 in PATH/venv).
    let src = if Path::new("cpp/CMakeLists.txt").exists()
        && Path::new("cpp/buffer.cpp").exists()
    {
        "cpp"
    } else {
        "../"
    };

    let dst = Config::new(src)
        .define("BUILD_PYTHON", "OFF")
        .define("BUILD_TESTS", "OFF")
        .define("BUILD_EXAMPLES", "OFF")
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    link_libs();
}

fn link_libs() {
    println!("cargo:rustc-link-lib=static=momentumx_static");
    println!("cargo:rustc-link-lib=rt");
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
