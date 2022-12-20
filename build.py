from pybind11.setup_helpers import Pybind11Extension, build_ext

print("Doing the stuffs")

def build(setup_kwargs):
    ext_modules = [
        Pybind11Extension("momentumx", [
            "momentumx/ext/binding.cpp",
            "momentumx/ext/buffer.h",
            "momentumx/ext/context.h",
            "momentumx/ext/momentumx.cpp",
            "momentumx/ext/momentumx.h",
            "momentumx/ext/stream.h",
            "momentumx/ext/utils.h",
        ]),
    ]
    setup_kwargs.update({
        "ext_modules": ext_modules,
        "cmd_class": {"build_ext": build_ext},
        "zip_safe": False,
    })