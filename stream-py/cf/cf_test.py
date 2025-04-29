import sys

from public_func import ip_pool, create_cf_session

# ether_scan_label = 'https://etherscan.io/accounts/label/aave'
ip_list = ip_pool(data_type='spider')
for i in ip_list:
    print(i)

sys.exit(1)


global eth_list
for i in ip_pool(data_type='spider'):
    sess = create_cf_session()
    resp = sess.get(url='https://cn.etherscan.com/token/0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE', proxies=i,
                    timeout=10)
    if resp.status_code == 200:
        eth_list.append(i)
        print(i)
    else:
        print("请求失败", i)


