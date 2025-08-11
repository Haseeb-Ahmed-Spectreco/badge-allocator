#!/usr/bin/env python3
"""
Deployment script to create AWS Lambda package
Run this to create a deployment-ready zip file
"""

import os
import shutil
import zipfile
import subprocess
import sys


def create_lambda_package():
    """Create deployment package for AWS Lambda"""
    
    print("🚀 Creating AWS Lambda deployment package...")
    
    # Clean up previous builds
    if os.path.exists('lambda_package'):
        shutil.rmtree('lambda_package')
    if os.path.exists('lambda_deployment.zip'):
        os.remove('lambda_deployment.zip')
    
    # Create package directory
    os.makedirs('lambda_package', exist_ok=True)
    
    print("📦 Installing dependencies...")
    # Install dependencies to package directory
    try:
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'install',
            '-r', 'requirements.txt',
            '-t', 'lambda_package',
            '--no-cache-dir',
            '--upgrade'
        ])
        print("✅ Dependencies installed")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False
    
    print("📁 Copying source code...")
    
    # Copy main.py (Lambda entry point)
    shutil.copy2('main.py', 'lambda_package/')
    print("✅ Copied main.py")
    
    # Copy badge_system package
    source_dirs = ['badge_system', 'aws', 'core', 'client', 'watcher']
    for dir_name in source_dirs:
        if os.path.exists(dir_name):
            dest_path = os.path.join('lambda_package', dir_name)
            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
            shutil.copytree(dir_name, dest_path)
            print(f"✅ Copied {dir_name}")
    
    # Copy any additional Python files
    for file in os.listdir('.'):
        if file.endswith('.py') and file not in ['main.py', 'deploy.py']:
            shutil.copy2(file, 'lambda_package/')
            print(f"✅ Copied {file}")
    
    print("🗜️  Creating ZIP package...")
    
    # Create deployment ZIP
    with zipfile.ZipFile('lambda_deployment.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('lambda_package'):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, 'lambda_package')
                zipf.write(file_path, arc_name)
    
    # Get package size
    package_size = os.path.getsize('lambda_deployment.zip') / (1024 * 1024)
    
    print("✅ Package created successfully!")
    print(f"📊 Package size: {package_size:.2f} MB")
    
    if package_size > 50:
        print("⚠️  Package exceeds 50MB - consider using Lambda Layers or S3")
    
    # Clean up temporary directory
    shutil.rmtree('lambda_package')
    
    print("\n🎯 Next steps:")
    print("1. Upload lambda_deployment.zip to AWS Lambda")
    print("2. Set handler to: main.lambda_handler")
    print("3. Set timeout to 5+ minutes")
    print("4. Set memory to 512+ MB")
    print("5. Configure environment variables")
    
    return True


if __name__ == "__main__":
    success = create_lambda_package()
    if success:
        print("\n🎉 Deployment package ready!")
    else:
        print("\n❌ Package creation failed!")
        sys.exit(1)