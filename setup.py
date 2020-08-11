import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="link",
    version="0.0.1",
    author="Christoph Blessing",
    author_email="chris24.blessing@gmail.com",
    description="A package that lets you link DataJoint schemas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cblessing24/link",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
    python_requires=">=3.8",
    install_requires=["datajoint"],
)
