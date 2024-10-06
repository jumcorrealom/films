import os

def print_directory_structure(start_path='.', exclude_folders=None):
    if exclude_folders is None:
        exclude_folders = []

    for root, dirs, files in os.walk(start_path):
        # Excluir carpetas especificadas
        dirs[:] = [d for d in dirs if d not in exclude_folders]
        
        level = root.replace(start_path, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{subindent}{f}')

# Llamada al script para excluir la carpeta 'venv'
print_directory_structure('.', exclude_folders=['venv'])
