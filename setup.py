import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="diarios",
    version="0.0.1",
    author="Guilherme Lambais and Henrik Sigstad",
    author_email="h.sigstad@gmail.com",
    description="Extract information from Brazilian official diaries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hsigstad/diarios",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
