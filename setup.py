from setuptools import setup, find_packages

setup(
    name='ps-energy-dl',
    version='0.0.1',
    packages=find_packages(),
    url='https://github.com/balusai-itv/ps-energy-dl',
    license='',
    author='ITVERSITY',
    author_email='siri.sagi@itversity.in',
    description='Loading the required data to the database from the source website',
    install_requirements=[
        'pandas==1.3.2',
        'camelot-py[cv]==0.10.1',
        'psycopg2-binary==2.9.1',
        'SQLAlchemy==1.4.23',
        'python-dotenv>=0.5.1',
        'numpy==1.21.1',
        'requests~=2.26.0',
        'beautifulsoup4~=4.10.0'],
    package_data={'': ['*']},
    entry_points={
        'console_scripts': [
            'ps_dl = app.ps_energy_dl:main',
        ],
    },
    extras_require={},
    zip_safe=False,
)
