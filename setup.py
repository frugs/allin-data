import setuptools

VERSION = "0.0.1"

setuptools.setup(
    name="leaderboarddata",
    packages=setuptools.find_packages(),
    version=VERSION,
    description="Back end service for SC2 ladder ranking leaderboard",
    author="Hugo Wainwright",
    author_email="wainwrighthugo@gmail.com",
    url="https://github.com/frugs/allin-data",
    keywords=["sc2", "MMR"],
    classifiers=[],
    install_requires=[
        "sc2gamedata",
        "retryfallback",
        "pyrebase",
        "flask",
        "google-cloud-datastore",
    ],
)
