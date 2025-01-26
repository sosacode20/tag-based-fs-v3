from pathlib import Path
import sys

# This code is for adding the `src` directory to the sys.path
# and handling the problems of sibling packages imports
directory = str(Path(__file__).parent.parent.absolute())
if directory not in sys.path:
    print("Appending to path")
    sys.path.append(str(directory))
