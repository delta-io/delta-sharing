# Delta Kernel Python

This adds thin python bindings via maturin and pyo3.

You can see how it would be used in the test.py file.

## usage
To test it out you will need to create a venv and set up the deps:

    cd [delta-sharing-root]/python/delta-kernel-python # if you're not already there
    python3 -m venv .venv
    source .venv/bin/activate
    pip install maturin
    pip freeze
    maturin develop
    pip install pyarrow
    pip install pandas
    python test.py file:///[full/path/to]/delta-kernel-rs/kernel/tests/data/table-with-dv-small/
