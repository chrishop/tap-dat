from setuptools import setup, find_packages

setup(
    name = 'tap-dat',
    version = '0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires = [
        'click',
        'astropy',
	    'astroquery', 
        'numpy', 
        'mysql-connector-python'
    ],
    entry_points={
        'console_scripts': [
            'tap-dat = tap_dat.__main__:main'
        ]
    }
)
