from setuptools import find_packages, setup

setup(
    name="did_it_hail",
    packages=find_packages(exclude=["did_it_hail_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
