import requests


def get_public_ip():
    for i in requests.get('https://1.1.1.1/cdn-cgi/trace').text.split(''):
        if i.startswith("ip="):
            return i.split('=')[1]
    return 'ERR'


if __name__ == '__main__':
    print(get_public_ip())
