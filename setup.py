import setuptools

setuptools.setup(
    name="bloomberg_py_wrapper",
    version="0.0.1",
    author="tebbb",
    author_email="",
    description="A Python Wrapper to simplify the use of Bloomberg's API",
    url="https://github.com/tebbb/bloomberg_py_wrapper",
    packages=[bloomberg_py_wrapper],
    install_requires=[
        "pandas",
        "blpapi",
        "signal",
        "datetime",
    ],
    extras_require={
        "Asynchronous": ["threading"],
        "Subscribtion": ["threading"]
    },
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: GNUv3",
        "Operating System :: OS Independent",
    ],
)
