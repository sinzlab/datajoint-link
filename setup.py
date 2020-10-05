import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datajoint-link",
    version="0.0.1",
    author="Christoph Blessing",
    author_email="chris.blessing@protonmail.com",
    description="A tool for linking two DataJoint tables located on different database servers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sinzlab/link",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Database",
    ],
    python_requires=">=3.8",
    install_requires=["datajoint"],
)
