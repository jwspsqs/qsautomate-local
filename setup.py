from setuptools import find_packages, setup


def parse_requirements(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line and not line.startswith("#")]


with open("README.md", "r", encoding="utf-8", errors="ignore") as fh:
    long_description = fh.read()

version = {}
with open("qsautomate/_version.py", encoding="utf-8") as fp:
    exec(fp.read(), version)

setup(
    name="QSAutomate",
    version=version["__version__"],
    description="Orchestrate algorithmic trading strategies.",
    author="Jason Strimpel",
    author_email="jason@quantscience.io",
    long_description=long_description,
    url="https://github.com/quant-science/QSAutomarch",
    packages=find_packages(),
    install_requires=parse_requirements("requirements.txt"),
    python_requires=">=3.9",
    extras_require={},
)
