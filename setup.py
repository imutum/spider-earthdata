import setuptools

with open("README.md") as f:
    long_description = f.read()

package_name = "spided"

with open(f"./src/{package_name}/__init__.py") as f:
    for line in f.readlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            version = line.split(delim)[1]
            break
    else:
        print("Can't find version! Stop Here!")
        exit(1)


setuptools.setup(
    name='spider-earthdata',  #应用名
    author='mondayfirst',
    author_email="",
    version=version,  #版本号
    description="This is a Package for downloading earthdata from nasa",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages("src"),  #包括在安装包内的Python包
    package_dir={"": "src"},
    package_data={package_name: [
        "*",
    ]},
    include_package_data=True,  #启用清单文件MANIFEST.in,包含数据文件
    # exclude_package_data={'docs': ['1.txt']},  #排除文件
    install_requires=[  #自动安装依赖
        "numpy",
        "pandas",
        "requests",
    ],
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 1 - alpha",
        "Programming Language :: Python :: 3.6+",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
    ],
)
