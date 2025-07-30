"""
Setup configuration for badge-allocator package
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read requirements from requirements.txt
def read_requirements(filename):
    """Read requirements from file"""
    requirements_path = Path(__file__).parent / filename
    if requirements_path.exists():
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [
                line.strip() 
                for line in f 
                if line.strip() and not line.startswith('#') and not line.startswith('-r')
            ]
    return []

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = ""
if readme_path.exists():
    with open(readme_path, 'r', encoding='utf-8') as f:
        long_description = f.read()

setup(
    name="badge-allocator",
    version="0.1.0",
    description="AWS Lambda-based badge evaluation system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    
    # Package discovery
    packages=find_packages(),
    python_requires=">=3.8",
    
    # Runtime dependencies
    install_requires=read_requirements("requirements.txt"),
    
    # Optional dependencies
    extras_require={
        "dev": read_requirements("requirements-dev.txt")[1:],  # Skip -r requirements.txt line
        "lambda": read_requirements("requirements-lambda.txt"),
    },
    
    # Entry points (if you want command-line tools)
    entry_points={
        "console_scripts": [
            # "badge-client=badge_system.queue_client:main",
        ],
    },
    
    # Metadata
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)