import os
import sys

from setuptools import setup, find_packages

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    os.system('python setup.py bdist_wheel upload')
    print("Now tag me :)")
    print("  git tag -a {0} -m 'version {0}'".format(__import__('pynamodb').__version__))
    print("  git push --tags")
    sys.exit()

install_requires = [
    'Delorean',
    'six',
    'botocore>=1.0.0'
]

setup(
    name='pynamodb',
    version=__import__('pynamodb').__version__,
    packages=find_packages(),
    url='http://jlafon.io/pynamodb.html',
    author='Jharrod LaFon',
    author_email='jlafon@eyesopen.com',
    description='A Pythonic Interface to DynamoDB',
    long_description=open('README.rst').read(),
    zip_safe=False,
    license='MIT',
    keywords='python dynamodb amazon',
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'License :: OSI Approved :: MIT License',
    ],
)
