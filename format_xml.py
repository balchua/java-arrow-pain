#!/usr/bin/env python3
"""
Format XML files with proper indentation using built-in xml.etree.
Handles large files efficiently.
"""

import sys
from pathlib import Path
import xml.etree.ElementTree as ET

def indent(elem, level=0, indent_str="    "):
    """Recursively add indentation to XML elements."""
    i = "\n" + level * indent_str
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + indent_str
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for child in elem:
            indent(child, level + 1, indent_str)
        if not child.tail or not child.tail.strip():
            child.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def format_xml(input_file: Path, indent_size: int = 4) -> None:
    """Format an XML file with proper indentation in-place."""
    print(f"Formatting {input_file.name}... ", end="", flush=True)
    
    temp_file = input_file.with_suffix(input_file.suffix + '.tmp')
    
    try:
        # Parse XML
        tree = ET.parse(str(input_file))
        root = tree.getroot()
        
        # Add indentation
        indent(root, 0, " " * indent_size)
        
        # Write to temp file
        tree.write(
            str(temp_file),
            encoding='UTF-8',
            xml_declaration=True
        )
        
        # Replace original
        temp_file.replace(input_file)
        
        size_mb = input_file.stat().st_size / (1024 * 1024)
        print(f"✓ ({size_mb:.1f} MB)")
        
    except Exception as e:
        print(f"✗ Failed: {e}")
        if temp_file.exists():
            temp_file.unlink()
        raise

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: format_xml.py <file1.xml> [file2.xml] ...")
        sys.exit(1)
    
    print("XML Formatter")
    print("─" * 60)
    
    success = 0
    failed = 0
    
    for arg in sys.argv[1:]:
        file_path = Path(arg)
        if not file_path.is_file():
            print(f"✗ File not found: {file_path}")
            failed += 1
            continue
        
        try:
            format_xml(file_path)
            success += 1
        except Exception:
            failed += 1
    
    print("─" * 60)
    print(f"Complete: {success} formatted, {failed} failed")
