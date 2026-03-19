from setuptools import setup, find_packages

setup(
    name="delta-sharing-cli",
    version="0.1.0",
    description="Delta Sharing CLI — interact with a Delta Sharing server",
    py_modules=["cli"],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "delta-sharing=cli:main",
        ],
    },
)