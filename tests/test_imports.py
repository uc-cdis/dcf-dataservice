from importlib import import_module
import os
from pathlib import Path


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_imports():
    scripts_dir = os.path.join(CURRENT_DIR, "../scripts/")
    scripts = [f.stem for f in Path(scripts_dir).glob("*.py")]
    for script in scripts:
        if script == "google_compose_upload":
            print(
                "WARNING: skipping 'google_compose_upload.py' module. It's outdated and not used. Would need to fix 'urllib2' and 'Queue' imports."
            )
            continue
        print(f"Importing: {script}.py")
        import_module(f"scripts.{script}")
