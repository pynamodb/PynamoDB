from setuptools import setup, find_packages


install_requires = [
    'botocore>=1.12.54',
    'typing-extensions>=3.7; python_version<"3.8"'
]

setup(
    name='pynamodax',
    version=__import__('pynamodax').__version__,
    packages=find_packages(exclude=('examples', 'tests', 'tests.integration',)),
    url='http://jlafon.io/pynamodb.html',
    project_urls={
        'Source': 'https://github.com/pynamodb/PynamoDB',
    },
    author='Jharrod LaFon',
    author_email='jlafon@eyesopen.com',
    description='A Pythonic Interface to DynamoDB',
    long_description=open('README.rst').read(),
    zip_safe=False,
    license='MIT',
    keywords='python dynamodb amazon dax',
    python_requires=">=3.7",
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
    ],
    extras_require={
        'signals': ['blinker>=1.3,<2.0'],
        'dax': ['amazon-dax-client>=2.0.0,<3.0.0']
    },
    package_data={'pynamodax': ['py.typed']},
)
