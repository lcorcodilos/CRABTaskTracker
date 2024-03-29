from setuptools import setup

setup(
    name="CRABTaskTracker",
    version="1.0",
    author = "Lucas Corcodilos",
    author_email = "corcodilos.lucas@gmail.com",
    license = "gpl-3.0",
    keywords = "crab3 task track",
    url = "https://github.com/lcorcodilos/CRABTaskTracker",
    long_description=open('README.md','r').read(),
    scripts=['crab_status.py']
)
