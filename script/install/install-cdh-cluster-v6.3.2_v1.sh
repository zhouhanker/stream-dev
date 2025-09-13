#!/bin/bash

# CDH 6.3.2
# 用法: ./install-cdh-aliyun-v2.3.1.sh <mysql_host> <root_password> <cdh_host1> <cdh_host2> <cdh_host3> [cdh_host4 cdh_host5]
# Author Eric
# version: v1.0.0
# You /opt/pkg dir must have cdh6.3.2-install-pkg.tar.gz pkg

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=8"
SCP_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=8"

#------------------------------
# 参数校验
#------------------------------
if [ $# -lt 5 ]; then
  echo "help: $0 mysql_host password cdh_host1 cdh_host2 cdh_host3 [cdh_host4 cdh_host5]"
  exit 1
fi

MYSQL_HOST="$1"
PASSWORD="$2"
shift 2
CDH_HOSTS=("$@")
CDH_COUNT=$#

log_info "Start CDH 6.3.2 Cluster"
log_info "MySQL Node: $MYSQL_HOST"
log_info "CDH Node: ${CDH_HOSTS[*]}"
log_info "Node Number: $CDH_COUNT"

# root 检查
if [ "$(id -u)" != "0" ]; then
  log_error "This sc Must root Run"
fi

# 目录 & 资源
WORK_DIR="/opt/cdh-install"
PKG_FILE="/opt/pkg/cdh6.3.2-install-pkg.tar.gz"
mkdir -p "$WORK_DIR"
[ -f "$PKG_FILE" ] || log_error "res pkg $PKG_FILE is not"

#------------------------------
# 0. 预处理：尽量提供 sshpass（避免未修复repo时 yum 失败）
#------------------------------
ensure_sshpass() {
  if command -v sshpass >/dev/null 2>&1; then
    return 0
  fi
  # 尝试从资源包解压后目录安装本地RPM（需要稍后解压，先记录意图）
  if ls "$WORK_DIR"/sshpass-*.rpm >/dev/null 2>&1; then
    log_info "local install sshpass RPM..."
    rpm -Uvh --force --nodeps "$WORK_DIR"/sshpass-*.rpm >/dev/null 2>&1 || true
    if command -v sshpass >/dev/null 2>&1; then
      log_info "sshpass is install"
      return 0
    fi
  fi
  return 1
}

#------------------------------
# 0.1 先做基础连通性检查（本机侧），不依赖 sshpass
#------------------------------
log_info "base check network node local & remote "
for host in "${CDH_HOSTS[@]}"; do
  if ! ping -c1 -W1 "$host" >/dev/null 2>&1; then
    log_error "this local not $host,please check node network and DNS Setting"
  fi
done
log_info "base Remote www.baidu.com"
if ! ping -c1 -W2 www.baidu.com >/dev/null 2>&1; then
  log_error "please check node network and DNS Setting"
fi

#------------------------------
# 函数：写阿里云 Yum 源（CentOS7 vault + EPEL）
#------------------------------
write_aliyun_repo() {
  cat >/etc/yum.repos.d/CentOS-Base.repo <<'EOFREPO'
[base]
name=CentOS-7 - Base - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/os/$basearch/
gpgcheck=0
enabled=1

[updates]
name=CentOS-7 - Updates - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/updates/$basearch/
gpgcheck=0
enabled=1

[extras]
name=CentOS-7 - Extras - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/extras/$basearch/
gpgcheck=0
enabled=1

[centosplus]
name=CentOS-7 - Plus - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/centosplus/$basearch/
gpgcheck=0
enabled=0

[epel]
name=EPEL-7 - Aliyun
baseurl=http://mirrors.aliyun.com/epel/7/$basearch/
enabled=1
gpgcheck=0
EOFREPO

  if [ -f /etc/yum/pluginconf.d/fastestmirror.conf ]; then
    sed -ri 's/^enabled\s*=\s*1/enabled=0/' /etc/yum/pluginconf.d/fastestmirror.conf || true
  fi
}

#------------------------------
# 函数：修复本机 Yum 源（阿里云）
#------------------------------
fix_local_yum_repo() {
  log_info "Fix local Yum CentOS 7 vault + EPEL..."
  mkdir -p /etc/yum.repos.d/backup && cp -a /etc/yum.repos.d/*.repo /etc/yum.repos.d/backup/ 2>/dev/null || true
  write_aliyun_repo
  yum clean all || true
  (yum makecache fast || yum makecache) || true
}

#------------------------------
# 函数：远程修复 Yum 源（阿里云）
#------------------------------
fix_remote_yum_repo() {
  local host="$1"
  log_info "[$host] Fix YumRepo CentOS 7 vault + EPEL..."
  ssh $SSH_OPTS "$host" "bash -s" <<'EOF'
set -euo pipefail
mkdir -p /etc/yum.repos.d/backup && cp -a /etc/yum.repos.d/*.repo /etc/yum.repos.d/backup/ 2>/dev/null || true
cat >/etc/yum.repos.d/CentOS-Base.repo <<'EOFREPO'
[base]
name=CentOS-7 - Base - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/os/$basearch/
gpgcheck=0
enabled=1

[updates]
name=CentOS-7 - Updates - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/updates/$basearch/
gpgcheck=0
enabled=1

[extras]
name=CentOS-7 - Extras - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/extras/$basearch/
gpgcheck=0
enabled=1

[centosplus]
name=CentOS-7 - Plus - Aliyun Vault
baseurl=http://mirrors.aliyun.com/centos-vault/7.9.2009/centosplus/$basearch/
gpgcheck=0
enabled=0

[epel]
name=EPEL-7 - Aliyun
baseurl=http://mirrors.aliyun.com/epel/7/$basearch/
enabled=1
gpgcheck=0
EOFREPO
if [ -f /etc/yum/pluginconf.d/fastestmirror.conf ]; then
  sed -ri 's/^enabled\s*=\s*1/enabled=0/' /etc/yum/pluginconf.d/fastestmirror.conf || true
fi
yum clean all || true
(yum makecache fast || yum makecache) || true
EOF
}

#------------------------------
# 解压资源包（后续本地安装 sshpass）
#------------------------------
log_info "Decompress resource..."
tar -zxvf "$PKG_FILE" -C "$WORK_DIR" --strip-components=1

JDK_RPM="$WORK_DIR/oracle-j2sdk1.8-1.8.0+update181-1.x86_64.rpm"
CM_TAR="$WORK_DIR/cm6.3.1-redhat7.tar.gz"
MYSQL_PKG_DIR="$WORK_DIR/mysql57pkg"
[ -f "$JDK_RPM" ] || log_error "$(basename "$JDK_RPM") not resource"
[ -f "$CM_TAR" ] || log_error "$(basename "$CM_TAR") not resource"
[ -d "$MYSQL_PKG_DIR" ] || log_error "MySQL Pkg "

# 再次尝试提供 sshpass（本地RPM）
if ! ensure_sshpass; then
  log_warn "sshpass please fix"
  fix_local_yum_repo
  yum install -y sshpass
fi

#------------------------------
# 1. 配置 SSH 免密（从本机 -> 所有节点）
#------------------------------
log_info "Setting SSH login..."
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
fi
for host in "${CDH_HOSTS[@]}"; do
  log_info "Setting $host local..."
  sshpass -p "$PASSWORD" ssh-copy-id $SSH_OPTS root@"$host" || log_error "ssh-copy-id to $host Error"
done

#------------------------------
# 1.1 高级连通性检查：各节点之间两两互 ping + 外网
#------------------------------
log_info "network check strat ..."
for src in "${CDH_HOSTS[@]}"; do
  for dst in "${CDH_HOSTS[@]}"; do
    if [ "$src" != "$dst" ]; then
      if ! sshpass -p "$PASSWORD" ssh $SSH_OPTS root@"$src" "ping -c1 -W1 $dst >/dev/null 2>&1"; then
        log_error "$src not ping  $dst please check DNS"
      fi
    fi
  done
done
log_info "network check end ..."

log_info "To www.baidu.com"
for src in "${CDH_HOSTS[@]}"; do
  if ! sshpass -p "$PASSWORD" ssh $SSH_OPTS root@"$src" "ping -c1 -W2 www.baidu.com >/dev/null 2>&1"; then
    log_error "$src not ping to www.baidu.com"
  fi
done
log_info "check success"

#------------------------------
# 1.2 修复所有节点 Yum 源
#------------------------------
log_info "Start Fix local node remote yum to aliyun cloud"
for host in "${CDH_HOSTS[@]}"; do
  set +e
  fix_remote_yum_repo "$host"
  rc=$?
  set -e
  if [ $rc -ne 0 ]; then
    log_warn "[$host] Fix Yum Error /etc/yum.repos.d/"
  fi
done

#------------------------------
# 2. 关闭防火墙和 SELinux
#------------------------------
log_info "close SELinux..."
for host in "${CDH_HOSTS[@]}"; do
  ssh $SSH_OPTS "$host" "systemctl stop firewalld >/dev/null 2>&1 || true; systemctl disable firewalld >/dev/null 2>&1 || true"
  ssh $SSH_OPTS "$host" "setenforce 0 >/dev/null 2>&1 || true; sed -ri 's/^SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config || true"
done

#------------------------------
# 3. 安装 JDK
#------------------------------
log_info "install Oracle JDK1.8..."
for host in "${CDH_HOSTS[@]}"; do
  scp $SCP_OPTS "$JDK_RPM" "$host:/tmp/"
  ssh $SSH_OPTS "$host" "rpm -ivh /tmp/$(basename "$JDK_RPM") || true; rm -f /tmp/$(basename "$JDK_RPM")"
  ssh $SSH_OPTS "$host" "echo 'export JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera' > /etc/profile.d/java.sh; echo 'export PATH=\$JAVA_HOME/bin:\$PATH' >> /etc/profile.d/java.sh"
done

#------------------------------
# 4. 安装 MySQL (在指定节点)
#------------------------------
log_info "At $MYSQL_HOST meta node install MySQL..."
ssh $SSH_OPTS "$MYSQL_HOST" "mkdir -p /tmp/mysql-pkgs"
scp $SCP_OPTS "$MYSQL_PKG_DIR"/* "$MYSQL_HOST:/tmp/mysql-pkgs/"
scp $SCP_OPTS "$MYSQL_PKG_DIR/mysql-connector-java-5.1.27-bin.jar" "$MYSQL_HOST:/tmp/"

ssh $SSH_OPTS "$MYSQL_HOST" "bash -s" <<EOF
set -euo pipefail
rpm -qa | grep -i mariadb | xargs -r rpm -e --nodeps 2>/dev/null || true
yum install -y net-tools libaio numactl-libs || true
yum localinstall -y /tmp/mysql-pkgs/*.rpm --nogpgcheck
BIN=\$(command -v mysqld || true)
[ -n "\$BIN" ] || { echo "mysqld 不存在，请检查 MySQL 安装"; exit 2; }
\$BIN --initialize --user=mysql
systemctl daemon-reload || true
systemctl enable mysqld || true
systemctl start mysqld

TEMP_PASSWORD=\$(grep 'temporary password' /var/log/mysqld.log | awk '{print \$NF}')
mysqladmin -uroot -p"\$TEMP_PASSWORD" password "$PASSWORD"

# 允许远程
grep -q '^\[mysqld\]' /etc/my.cnf 2>/dev/null || echo "[mysqld]" >> /etc/my.cnf
if grep -q '^bind-address' /etc/my.cnf 2>/dev/null; then
  sed -ri 's/^[[:space:]]*bind-address.*/bind-address = 0.0.0.0/' /etc/my.cnf
else
  echo "bind-address = 0.0.0.0" >> /etc/my.cnf
fi
systemctl restart mysqld

mysql -uroot -p"$PASSWORD" -e "
  CREATE DATABASE IF NOT EXISTS scm DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
  CREATE DATABASE IF NOT EXISTS hive DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
  CREATE DATABASE IF NOT EXISTS oozie DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
  CREATE DATABASE IF NOT EXISTS hue DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;
  GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY '$PASSWORD' WITH GRANT OPTION;
  FLUSH PRIVILEGES;
"

mkdir -p /usr/share/java/
cp -f /tmp/mysql-connector-java-5.1.27-bin.jar /usr/share/java/mysql-connector-java.jar
chmod 644 /usr/share/java/mysql-connector-java.jar
rm -f /tmp/mysql-connector-java-5.1.27-bin.jar
rm -rf /tmp/mysql-pkgs
EOF

#------------------------------
# 5. 分发 MySQL Connector 到其他节点（先到本机，再推各节点）
#------------------------------
log_info "scp MySQL Connector to all node..."
scp $SCP_OPTS "$MYSQL_HOST:/usr/share/java/mysql-connector-java.jar" "/tmp/mysql-connector-java.jar"
[ -f /tmp/mysql-connector-java.jar ] || log_error "从 $MYSQL_HOST get mysql-connector-java.jar err"
for host in "${CDH_HOSTS[@]}"; do
  if [ "$host" != "$MYSQL_HOST" ]; then
    ssh $SSH_OPTS "$host" "mkdir -p /usr/share/java/"
    scp $SCP_OPTS "/tmp/mysql-connector-java.jar" "$host:/usr/share/java/"
    ssh $SSH_OPTS "$host" "chmod 644 /usr/share/java/mysql-connector-java.jar"
  fi
done

#------------------------------
# 6. 安装 Cloudera Manager
#------------------------------
log_info "install Cloudera Manager..."
CM_DIR="/opt/cloudera-manager"
mkdir -p "$CM_DIR"
tar -zxvf "$CM_TAR" -C "$CM_DIR"

DAEMONS_RPM="$CM_DIR/cm6.3.1/RPMS/x86_64/cloudera-manager-daemons-6.3.1-1466458.el7.x86_64.rpm"
AGENT_RPM="$CM_DIR/cm6.3.1/RPMS/x86_64/cloudera-manager-agent-6.3.1-1466458.el7.x86_64.rpm"
SERVER_RPM="$CM_DIR/cm6.3.1/RPMS/x86_64/cloudera-manager-server-6.3.1-1466458.el7.x86_64.rpm"

for host in "${CDH_HOSTS[@]}"; do
  scp $SCP_OPTS "$DAEMONS_RPM" "$host:/tmp/"
  ssh $SSH_OPTS "$host" "yum -y localinstall /tmp/$(basename "$DAEMONS_RPM") --nogpgcheck && rm -f /tmp/$(basename "$DAEMONS_RPM")"
done

for host in "${CDH_HOSTS[@]}"; do
  ssh $SSH_OPTS "$host" "yum install -y bind-utils psmisc cyrus-sasl-plain cyrus-sasl-gssapi fuse portmap fuse-libs httpd mod_ssl openssl-devel python-psycopg2 MySQL-python || true"
done

for host in "${CDH_HOSTS[@]}"; do
  scp $SCP_OPTS "$AGENT_RPM" "$host:/tmp/"
  ssh $SSH_OPTS "$host" "yum -y localinstall /tmp/$(basename "$AGENT_RPM") --nogpgcheck && rm -f /tmp/$(basename "$AGENT_RPM")"
  ssh $SSH_OPTS "$host" "[ -f /etc/cloudera-scm-agent/config.ini ] || { echo 'not /etc/cloudera-scm-agent/config.ini，agent install error'; exit 3; }"
  ssh $SSH_OPTS "$host" "sed -ri 's/^server_host=.*/server_host=${CDH_HOSTS[0]}/' /etc/cloudera-scm-agent/config.ini"
done

scp $SCP_OPTS "$SERVER_RPM" "${CDH_HOSTS[0]}:/tmp/"
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "yum -y localinstall /tmp/$(basename "$SERVER_RPM") --nogpgcheck && rm -f /tmp/$(basename "$SERVER_RPM")"
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "[ -d /etc/cloudera-scm-server ] || { echo 'not /etc/cloudera-scm-server，server install error'; exit 4; }"

#------------------------------
# 7. 配置数据库连接（Server 节点）
#------------------------------
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "bash -s" <<EOF
set -euo pipefail
cat > /etc/cloudera-scm-server/db.properties <<EOP
com.cloudera.cmf.db.type=mysql
com.cloudera.cmf.db.host=$MYSQL_HOST:3306
com.cloudera.cmf.db.name=scm
com.cloudera.cmf.db.user=root
com.cloudera.cmf.db.password=$PASSWORD
com.cloudera.cmf.db.setupType=EXTERNAL
EOP
EOF

#------------------------------
# 8. 配置 CDH parcel（Server 节点）
#------------------------------
log_info "Setting CDH parcel..."
PARCEL_DIR="/opt/cloudera/parcel-repo"
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "mkdir -p $PARCEL_DIR"
scp $SCP_OPTS "$WORK_DIR/CDH-6.3.2-1.cdh6.3.2.p0.1605554-el7.parcel" "${CDH_HOSTS[0]}:$PARCEL_DIR/"
scp $SCP_OPTS "$WORK_DIR/CDH-6.3.2-1.cdh6.3.2.p0.1605554-el7.parcel.sha1" "${CDH_HOSTS[0]}:$PARCEL_DIR/"
scp $SCP_OPTS "$WORK_DIR/manifest.json" "${CDH_HOSTS[0]}:$PARCEL_DIR/"
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "cd $PARCEL_DIR && mv -f CDH-6.3.2-1.cdh6.3.2.p0.1605554-el7.parcel.sha1 CDH-6.3.2-1.cdh6.3.2.p0.1605554-el7.parcel.sha || true"
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "chown -R cloudera-scm:cloudera-scm /opt/cloudera"

#------------------------------
# 9. 调优系统参数
#------------------------------
log_info "Setting System..."
for host in "${CDH_HOSTS[@]}"; do
  ssh $SSH_OPTS "$host" "sysctl vm.swappiness=10 || true"
  ssh $SSH_OPTS "$host" "grep -q 'vm.swappiness' /etc/sysctl.conf && sed -ri 's/^vm\.swappiness=.*/vm.swappiness=10/' /etc/sysctl.conf || echo 'vm.swappiness=10' >> /etc/sysctl.conf"
  ssh $SSH_OPTS "$host" "echo never > /sys/kernel/mm/transparent_hugepage/defrag || true"
  ssh $SSH_OPTS "$host" "echo never > /sys/kernel/mm/transparent_hugepage/enabled || true"
  ssh $SSH_OPTS "$host" "grep -q transparent_hugepage /etc/rc.local || { echo 'echo never > /sys/kernel/mm/transparent_hugepage/defrag' >> /etc/rc.local; echo 'echo never > /sys/kernel/mm/transparent_hugepage/enabled' >> /etc/rc.local; }"
  ssh $SSH_OPTS "$host" "chmod +x /etc/rc.d/rc.local || true"
done

#------------------------------
# 10. 启动服务
#------------------------------
log_info "Start Cloudera Manager Service..."
ssh $SSH_OPTS "${CDH_HOSTS[0]}" "systemctl start cloudera-scm-server"
log_info "Wait Cloudera Manager Server start 120s..."
sleep 120
for host in "${CDH_HOSTS[@]}"; do
  ssh $SSH_OPTS "$host" "systemctl start cloudera-scm-agent"
done

log_info "CDH Install Success!"
log_info "Please Call http://${CDH_HOSTS[0]}:7180 Login (admin/admin)"
