"""
Spark EventLog Parser - 安装配置
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="spark-eventlog-parser",
    version="1.0.0",
    author="Asp BigData Team",
    description="Spark EventLog解析工具，用于分析大规模Spark作业性能",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "pyyaml>=5.4.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    entry_points={
        'console_scripts': [
            'parse-spark-logs=parse_spark_logs:main',
        ],
    },
)

