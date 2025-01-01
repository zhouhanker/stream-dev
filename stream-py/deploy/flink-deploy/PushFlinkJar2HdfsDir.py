import os
from pathlib import Path

maven_clean_commod = "mvn -DskipTests clean"
maven_package_command = "mvn -DskipTests package -P prod"


def get_project_root_path():
    current_file_path = os.path.abspath(__file__)
    root_path = os.path.dirname(current_file_path)
    # 项目的根目录
    return str(Path(root_path).parent.parent.parent)


def find_java_class_name_path(class_name: str):
    """
    获取项目的java类的路径
    :param class_name:
    :return:
    """
    for root, dirs, files in os.walk(get_project_root_path()):
        for file in files:
            if file.endswith(".java") and file[:-5] == class_name:
                return root
    return None


if __name__ == '__main__':
    print("--- get_project_root_path ---")
    print(find_java_class_name_path("DbusLogDataProcess2Kafka"))
