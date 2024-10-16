from setuptools import find_packages, setup

setup(
    name="turn_on_schedules_in_bulk",
    packages=find_packages(exclude=["turn_on_schedules_in_bulk_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
