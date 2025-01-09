import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from hdfs import InsecureClient
from fabric import Connection

maven_clean_command = "mvn -DskipTests clean -P prod"
maven_package_command = "mvn package -Dfile.encoding=UTF-8 -DskipTests=true -P prod"
hdfs_root_path = "http://cdh01:9870"


def run_command(command):
    """
    用于在不同系统上执行命令的函数
    """
    system_platform = platform.system()
    try:
        if system_platform == "Windows":
            subprocess.run(command, shell=True, check=True)
        elif system_platform == "Linux" or system_platform == "Darwin":
            subprocess.run(command, shell=True, check=True)
        else:
            print(f"不支持的操作系统: {system_platform}")
    except subprocess.CalledProcessError as e:
        print(f"执行命令出现错误: {e}")


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


def get_hdfs_client():
    """
    获取hdfs客户端
    :return:
    """
    return InsecureClient(hdfs_root_path, user="root", root="/")


def find_hdfs_jars_is_exist(hdfs_jar_path: str):
    """
    判断hdfs上是否存在jar包
    :param hdfs_jar_path:
    :return:
    """
    hdfs_cli = get_hdfs_client()
    return hdfs_cli.status(hdfs_jar_path, strict=False)


def get_remote_server_client():
    return Connection(host="cdh01", user="root", connect_kwargs={"password": "zh1028,./"})


def clean_local_maven_project():
    if len(sys.argv) > 1:
        class_name = sys.argv[1].split(".")[-1]
        print(find_java_class_name_path(class_name))
        project_root = get_project_root_path()
        # 执行清理命令
        subprocess.run(maven_clean_command, shell=True, check=True, cwd=project_root)
        time.sleep(1)
        # 执行打包命令
        subprocess.run(maven_package_command, shell=True, check=True, cwd=project_root)
        model_path = Path(find_java_class_name_path(class_name)).parent.parent.parent.parent.parent
        model_path = Path(str(model_path) + "/target")
        old_jar_name_path = ""
        new_jar_name_path = str(model_path) + "/" + class_name + ".jar"
        for item in model_path.iterdir():
            if item.is_file() and item.name.endswith("with-dependencies.jar"):
                old_jar_name_path = item.absolute()
        return str(old_jar_name_path) + "&" + str(new_jar_name_path)
    return None


def main():
    jar_path = clean_local_maven_project()
    jar_path = jar_path.split("&")[0]
    print(jar_path)
    if jar_path:
        hdfs_jar_path = "/flink-jars/" + os.path.basename(jar_path)
        if find_hdfs_jars_is_exist(hdfs_jar_path):
            print("jar包已经存在 ")
        else:
            hdfs_cli = get_hdfs_client()
            print("======================================Start======================================")
            print("===================================开始上传jar包===================================")
            hdfs_cli.upload("/flink-jars/", jar_path)
            time.sleep(3)
            print("Jar Path As -> " + hdfs_root_path + hdfs_jar_path)
            print("===================================上传jar包成功===================================")
            # hdfs_cli.rename(hdfs_jar_path, "/flink-jars/" + sys.argv[1].split(".")[-1] + ".jar")
            # print("==================================重命名jar包成功==================================")
            # print("Jar Path As -> " + hdfs_root_path + "/flink-jars/" + sys.argv[1].split(".")[-1] + ".jar")
            # print("=======================================END=======================================")
    else:
        print("没有找到jar包")


def upload_jar_remote_server():
    conn = get_remote_server_client()
    jar_path = clean_local_maven_project()
    local_jar_path = jar_path.split("&")[0]
    try:
        conn.put(local_jar_path, "/opt/soft/flink-1.17.1/local_jars/")
        print("===================================上传jar包到远程服务器成功===================================")
        conn.run("mv /opt/soft/flink-1.17.1/local_jars/"+os.path.basename(local_jar_path)+" /opt/soft/flink-1.17.1/local_jars/"+sys.argv[1].split(".")[-1] + ".jar")
        conn.run("hdfs dfs -put /opt/soft/flink-1.17.1/local_jars/" + sys.argv[1].split(".")[-1]+".jar" + " /flink-jars/")
        print("=====================================上传jar包到hdfs成功=====================================")
    except Exception as e:
        print(f"上传jar包到远程服务器失败: {e}")


if __name__ == '__main__':
    main()
    # upload_jar_remote_server()
    # print(clean_local_maven_project())
    # print(maven_package_command)
    # print(get_project_root_path())
