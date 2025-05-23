import json
import re
import sys

def convert_to_notebook(input_file, output_file):
    # Define the notebook structure
    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    # Read the Python file
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Add a title from the main docstring
    main_docstring = re.search(r'^"""(.*?)"""', content, re.DOTALL)
    if main_docstring:
        doc_lines = main_docstring.group(1).strip().split('\n')
        title = doc_lines[0]
        description = '\n'.join(doc_lines[1:]).strip()
        
        notebook["cells"].append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"# {title}\n\n{description}"]
        })
    
    # Find all section headers
    sections = re.findall(r'# =+\s*\n# (Section \d+: .+?)\s*\n# =+\s*\n"""(.*?)"""(.*?)(?=\n# =+|$)', 
                         content, re.DOTALL)
    
    # Process imports first
    imports = re.search(r'import.*?(?=\n# =+)', content, re.DOTALL)
    if imports:
        notebook["cells"].append({
            "cell_type": "code",
            "metadata": {},
            "source": imports.group(0),
            "execution_count": None,
            "outputs": []
        })
    
    # Process each section
    for title, doc, code in sections:
        # Add markdown cell with section title and description
        notebook["cells"].append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"## {title}\n\n{doc.strip()}"]
        })
        
        # Add code cell with section code
        notebook["cells"].append({
            "cell_type": "code",
            "metadata": {},
            "source": code.strip(),
            "execution_count": None,
            "outputs": []
        })
    
    # Write the notebook to a file
    with open(output_file, 'w') as f:
        json.dump(notebook, f, indent=2)
    
    print(f"Notebook created: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python py_to_notebook.py input.py output.ipynb")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_to_notebook(input_file, output_file) 