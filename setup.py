import os
import sys

from setuptools import setup, find_packages


def find_stubs(package):
    stubs = []
    for root, dirs, files in os.walk(package):
        for f in files:
            path = os.path.join(root, f).replace(package + os.sep, '', 1)
            if path.endswith('.pyi') or path.endswith('py.typed'):
                stubs.append(path)
    return {package: stubs}


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    os.system('python setup.py bdist_wheel upload')
    print("Now tag me :)")
    print("  git tag -a {0} -m 'version {0}'".format(__import__('pynamodb').__version__))
    print("  git push --tags")
    sys.exit()

install_requires = [
    'six',
    'botocore>=1.12.54',
    'python-dateutil>=2.1,<3.0.0',
]

setup(
    name='pynamodb',
    version=__import__('pynamodb').__version__,
    packages=find_packages(exclude=('tests',)),
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
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
    ],
    extras_require={
        'signals': ['blinker>=1.3,<2.0'],
    },
    package_data=find_stubs('pynamodb'),
)
