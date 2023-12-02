
""" 
    This module is used to run the package as a script.
    Usage:
        python -m catalogos_capes
"""
import fire
from .datasets import run


if __name__ == "__main__":
    fire.Fire(run)
