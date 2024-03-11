import os
import re
import random

replace_vals = [50, 100]


def replace_cpu_values(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    def replace_random(match):
        return '"cpu"={}'.format(random.choice(replace_vals))

    updated_content = re.sub(r'"cpu"=\d+', replace_random, content)

    with open(file_path, 'w') as file:
        file.write(updated_content)

def main():
    directory_path = '/var/bigdata/storm/plans-executed/'

    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.query'):  # Change the extension if needed
                file_path = os.path.join(root, file)
                replace_cpu_values(file_path)

if __name__ == "__main__":
    main()
