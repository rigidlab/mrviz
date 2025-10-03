from setuptools import setup, find_packages

setup(
    name='mrviz',                     # changed package name
    version='0.1.0',
    package_dir={'': 'src'},  # Tell setuptools to look in src/
    packages=find_packages(where='src'),
    install_requires=[
        'click>=8.0.0',
        'bokeh>=2.4.0'
    ],
    entry_points={
        'console_scripts': [
            'mrviz = mrviz.cli:main'  # changed entry point
        ]
    },
    python_requires='>=3.8',
    author='rigidlab',
    description='Mobile Robot Visualization',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)