# Delta Kernel Rust Sharing Wrapper

This adds a thin python bindings via maturin and pyo3 for delta-kernel-rust.

## usage
To build the wheel locally you will need to create a venv and set up the deps:

    cd [delta-sharing-root]/python/delta-kernel-rust-sharing-wrapper # if you're not already there
    python3 -m venv .venv
    source .venv/bin/activate
    pip install maturin
    pip freeze
    maturin develop

Now it should generate the wheel file, and you'll be able to use the delta_sharing_kernel_rust_wrapper library in python.
