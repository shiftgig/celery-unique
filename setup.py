from setuptools import setup


def long_description():
    """
    Attempt to read README.rst (which is generated during deploy-time),
    otherwise, use README.md.
    """
    try:
        return open('README.rst').read()
    except IOError:
        return open('README.md').read()


setup(
    name='celery-unique',
    url='https://github.com/sgrepo/celery-unique',
    author='Tyler Hendrickson, Shiftgig Inc',
    author_email='tyler@shiftgig.com',
    description='celery-unique adds argument-based unique constraints to Celery tasks',
    long_description=long_description(),
    py_modules=['celery_unique'],
    install_requires=[
        'celery',
        'redis',
    ],
    use_scm_version={
        'version_scheme': 'post-release',
    },
    setup_requires=['setuptools_scm'],
    test_suite='tests',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing',
    ]
)
