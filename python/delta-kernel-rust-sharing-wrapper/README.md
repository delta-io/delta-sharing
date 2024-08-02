# Delta Kernel Rust Sharing Wrapper

This adds thin python bindings via maturin and pyo3.

## Manual Installation
To test it out you will need to create a venv and set up the deps:

    cd [delta-sharing-root]/python/delta-kernel-rust-sharing-wrapper # if you're not already there
    python3 -m venv .venv
    source .venv/bin/activate
    pip install maturin
    maturin develop
