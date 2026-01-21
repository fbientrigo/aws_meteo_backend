import os
import shutil

APP_DIR = "app"
FILES_TO_MOVE = ["main.py", "s3_helpers.py"]
DIRS_TO_MOVE = ["routers", "lib"]

def move_files():
    if not os.path.exists(APP_DIR):
        os.makedirs(APP_DIR)
        print(f"Created {APP_DIR}/")

    for f in FILES_TO_MOVE:
        if os.path.exists(f):
            dest = os.path.join(APP_DIR, f)
            shutil.move(f, dest)
            print(f"Moved {f} -> {dest}")
        else:
            print(f"Skipped {f} (not found)")

    for d in DIRS_TO_MOVE:
        if os.path.exists(d):
            dest = os.path.join(APP_DIR, d)
            # handle if dest exists (merge or skip?)
            if os.path.exists(dest):
                 print(f"Warning: {dest} exists. Skipping overwrite.")
            else:
                 shutil.move(d, dest)
                 print(f"Moved directory {d} -> {dest}")
        else:
             print(f"Skipped directory {d} (not found)")

    # Also make sure app is a package
    with open(os.path.join(APP_DIR, "__init__.py"), "w") as f:
        pass
    print(f"Created {APP_DIR}/__init__.py")

if __name__ == "__main__":
    move_files()
