from tqdm import tqdm
import os


local_file_path = '../../../../../user_prop_file/common-config.properties.local'
target_file_path = '../../stream-common/src/main/resources/filter/common-config.properties.prod'


def copy_local_prop_2_dev(source_file, target_file):
    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    print("Copy Success !")

    with tqdm(total=len(lines), desc="Copying Config", ncols=100, colour='#008000') as pbar:
        with open(target_file, 'w', encoding='utf-8') as f:
            for line in lines:
                f.write(line)
                pbar.update(1)


def clean_file_dev(file_path):
    with tqdm(total=1, desc="Cleaning Config", ncols=100, colour='#008000') as pbar:
        with open(file_path, 'w', encoding='utf-8') as f:
            pass
        pbar.update(1)
    print("Clean Success !")


def check_process_config(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        print(f"{file_path} has content. Cleaning the file.")
        clean_file_dev(file_path)
    else:
        print(f"{file_path} is empty or does not exist. Copying local properties.")
        copy_local_prop_2_dev(local_file_path, file_path)


if __name__ == '__main__':
    check_process_config(target_file_path)
