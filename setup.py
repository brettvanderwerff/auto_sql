import setuptools

description= 'auto_sql is a memory aware csv to sqlite converter.'

with open("README.md", "r") as read_obj:
    long_description = read_obj.read()

setuptools.setup(
    name="auto_sql",
    version= "0.0.1",
    author="Brett Vanderwerff",
    author_email="brett.vanderwerff@gmail.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/brettvanderwerff/auto_sql",
    packages=setuptools.find_packages(),
    install_requires=[
              'pandas',
              'psutil',
          ],
    classifiers=(
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha"
        ))
