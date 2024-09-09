import setuptools

from ducksearch.__version__ import __version__

with open(file="README.md", mode="r", encoding="utf-8") as fh:
    long_description = fh.read()

base_packages = [
    "pandas >= 2.2.1",
    "duckdb == 1.0.0",
    "pyarrow >= 16.1.0",
    "tqdm >= 4.66.4",
    "joblib >= 1.4.2",
]

eval = ["ranx >= 0.3.16", "beir >= 2.0.0"]

dev = [
    "sqlfluff >= 3.1.0",
    "ruff >= 0.4.9",
    "pytest-cov >= 5.0.0",
    "pytest >= 8.2.1",
    "harlequin >= 1.24.0",
]


setuptools.setup(
    name="ducksearch",
    version=f"{__version__}",
    license="MIT",
    author="LightOn",
    description="DuckSearch: A Python library for efficient search in large collections of text data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lightonai/ducksearch",
    keywords=[],
    packages=setuptools.find_packages(),
    install_requires=base_packages,
    extras_require={
        "eval": base_packages + eval,
        "dev": base_packages + dev + eval,
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
