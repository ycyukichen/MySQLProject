from setuptools import setup, find_packages
from typing import List

Hyphen_e = '-e .'
# def get_requirements(filename: str) -> List[str]:
#     requirements = []
#     with open(filename) as obj:
#         requirements = obj.readlines()
#         requirements = [req.replace("\n", "") for req in requirements]
        
#         if Hyphen_e in requirements:
#             requirements.remove(Hyphen_e)
    
#     return requirements
    
setup(
    name="MySQL",
    version="0.1.0",
    author="Yu-Yu",
    author_email="yc0688a@american.edu",
    long_description=open('README.md').read(),
    url=f'https://github.com/ycyukichen/MySQLProject.git',
    # install_requires=get_requirements("requirements.txt"),
    package_dir={"": "src"},
    packages=find_packages(where="src")
)
