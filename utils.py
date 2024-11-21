import os

def get_unique_filename(filename: str, extension: str = "csv") -> str:
        version = 1
        while os.path.exists(f"{filename}_v{version}.{extension}"):
            version += 1
        return f"{filename}_v{version}.{extension}"

def create_folder(folder_name: str = "output") -> str:
    """Create output folder if it doesn't exist and return path"""
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name