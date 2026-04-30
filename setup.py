from setuptools import setup, find_packages


install_requires = [
    'botocore>=1.12.54',
    'typing-extensions>=4; python_version<"3.11"',
]

setup(
    name='pynamodb',
    version=__import__('pynamodb').__version__,
    packages=find_packages(exclude=('examples', 'tests', 'typing_tests', 'tests.integration',)),
    url='http://jlafon.io/pynamodb.html',
    project_urls={
        'Source': 'https://github.com/pynamodb/PynamoDB',
    },
    author='Jharrod LaFon',
    author_email='jlafon@eyesopen.com',
    description='A Pythonic Interface to DynamoDB',
    long_description=open('README.rst').read(),
    long_description_content_type='text/x-rst',
    zip_safe=False,
    license='MIT',
    keywords='python dynamodb amazon',
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
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'License :: OSI Approved :: MIT License',
    ],
    extras_require={
        'signals': ['blinker>=1.3,<2.0'],
    },
    package_data={'pynamodb': ['py.typed']},
)
