from setuptools import find_packages
from setuptools import setup

setup(
    name="with-cloud-blob",
    description="",
    long_description=open("README.md").read(),  # no "with..." will do for setup.py
    long_description_content_type="text/markdown; charset=UTF-8; variant=GFM",
    license="MIT",
    author="Kyrylo Shpytsya",
    author_email="kshpitsa@gmail.com",
    url="https://github.com/kshpytsya/with-cloud-blob",
    setup_requires=["setuptools_scm"],
    use_scm_version=True,
    python_requires=">=3.7, <3.8",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={"console_scripts": ["with-cloud-blob = with_cloud_blob._cli:main"]},
    classifiers=[
        "Development Status :: 1 - Planning",
        # "Development Status :: 3 - Alpha",
        # "Development Status :: 4 - Beta"
        # "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        # "Operating System :: MacOS :: MacOS X",
        # "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.7",
        "Topic :: Security :: Cryptography",
        "Topic :: System :: Installation/Setup",
        "Topic :: System :: Software Distribution",
        "Topic :: System :: Systems Administration",
    ],
    install_requires=[
        "atomicwrites>=1.2.1,<2",
        "click>=7.0,<8",
        "click-log>=0.3.2,<1",
        "filelock>=3.0.8,<4",
        "implements>=0.1.4,<1",
        "jsonschema>=3.0.2,<4",
        "pynacl>=1.3.0,<2",
        "py-ubjson>=0.14.0,<1",

    ],
    extras_require={
        "aws": [
            "boto3>=1.9.210,<2",
            "python_dynamodb_lock>=0.9.1,<1",
        ],
    },
)
