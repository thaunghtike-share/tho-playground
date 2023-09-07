import os
import subprocess

def create_and_push_files():
    for i in range(1, 6):
        filename = f"t_file_{i}.txt"
        with open(filename, "w") as file:
            file.write(f"This is file {i}\n")
        print(f"Created {filename}")

        # Add the created file to the Git repository
        subprocess.run(["git", "add", filename])

    # Commit the changes
    subprocess.run(["git", "commit", "-m", "Add files"])

    # Push the changes to the repository
    subprocess.run(["git", "push"])

if __name__ == '__main__':
    create_and_push_files()
