import random
import math

search_list = ['运动套装', '瑜伽服', '背心', '斜挎包', '休闲衫', '运动头带', '发圈']


def generate_random_list(original_list, min_length=1, max_length=None):
    if max_length is None:
        max_length = len(original_list)
    length = random.randint(min_length, max(max_length, min_length))
    return random.sample(original_list, k=length)


if __name__ == '__main__':
    print(generate_random_list(search_list))
