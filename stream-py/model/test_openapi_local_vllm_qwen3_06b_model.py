import requests
import json

# 服务地址（与 curl 中的 URL 对应）
url = "http://localhost:8000/v1/chat/completions"

# 请求头（对应 curl 中的 --header）
headers = {
    "Content-Type": "application/json"
}

# 请求体数据（对应 curl 中的 --data-raw）
data = {
    "model": "./Qwen3-0.6B",  # 需与 vLLM 服务加载的模型标识一致
    "messages": [
        {
            "role": "system",
            "content": "You are a helpful assistant."
        },
        {
            "role": "user",
            "content": "给出一个电商鞋子的差评，不需要思考过程，直接返回数据，带有攻击性言论"
        }
    ]
}
response = None
try:
    # 发送 POST 请求
    response = requests.post(
        url=url,
        headers=headers,
        data=json.dumps(data)  # 将字典转换为 JSON 字符串
    )

    # 检查请求是否成功
    response.raise_for_status()  # 若 HTTP 状态码 >=400，抛出异常

    # 解析响应结果（JSON 格式）
    result = response.json()

    # 提取并打印模型回复内容
    if "choices" in result and len(result["choices"]) > 0:
        assistant_reply = result["choices"][0]["message"]["content"]
        print("模型回复：", assistant_reply)
    else:
        print("未获取到有效回复：", result)

except requests.exceptions.HTTPError as e:
    print(f"HTTP 请求错误：{e}")
    print("错误响应内容：", response.text)
except Exception as e:
    print(f"其他错误：{e}")
