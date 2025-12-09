from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name="itch-parser",
    version="1.0.0",
    author="Rasoul Eshghi",
    author_email="cfte.mehr@gmail.com",
    description="NASDAQ ITCH binary file parser",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rasoul-ehsghi",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.7",
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "pytables>=3.6.0",
        "pytz>=2020.1",
        "matplotlib>=3.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "black>=20.8b1",
            "flake8>=3.8.0",
            "mypy>=0.790",
        ],
    },
    keywords="NASDAQ ITCH parser market-data financial-data HDF5",
)