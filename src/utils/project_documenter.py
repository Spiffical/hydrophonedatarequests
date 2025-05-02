#!/usr/bin/env python3
"""
project_documenter.py - Utility script to document project structure and file contents
"""
import os
import datetime

def is_binary_file(file_path):
    """Check if a file is binary."""
    try:
        with open(file_path, 'tr') as check_file:
            check_file.readline()
            return False
    except UnicodeDecodeError:
        return True

def get_project_structure(start_path='.', output_file=None, ignore_dirs=None):
    """
    Document the project structure and file contents.
    
    Args:
        start_path (str): The starting directory path
        output_file (str): Path to the output documentation file
        ignore_dirs (list): List of directory names to ignore
    """
    if ignore_dirs is None:
        ignore_dirs = ['.git', '.venv', '__pycache__', 'downloads']
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Write header
        f.write(f"Project Documentation\n")
        f.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        # Document project structure
        f.write("Project Structure:\n")
        f.write("=" * 20 + "\n\n")
        
        for root, dirs, files in os.walk(start_path):
            # Skip ignored directories
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            
            level = root.replace(start_path, '').count(os.sep)
            indent = '  ' * level
            f.write(f"{indent}{os.path.basename(root)}/\n")
            
            sub_indent = '  ' * (level + 1)
            for file in sorted(files):
                f.write(f"{sub_indent}{file}\n")
        
        f.write("\n\nFile Contents:\n")
        f.write("=" * 20 + "\n\n")
        
        # Document file contents
        for root, dirs, files in os.walk(start_path):
            # Skip ignored directories
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            
            for file in sorted(files):
                file_path = os.path.join(root, file)
                
                # Skip binary files
                if is_binary_file(file_path):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as source_file:
                        f.write(f"\nFile: {file_path}\n")
                        f.write("-" * (len(file_path) + 6) + "\n")
                        f.write(source_file.read())
                        f.write("\n\n")
                except Exception as e:
                    f.write(f"\nError reading {file_path}: {str(e)}\n")

def main():
    """Main entry point."""
    # Get the project root directory (assuming this script is in src/utils)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_file = os.path.join(project_root, "project_documentation.txt")
    
    print(f"Generating project documentation in: {output_file}")
    get_project_structure(project_root, output_file)
    print("Documentation complete!")

if __name__ == "__main__":
    main() 