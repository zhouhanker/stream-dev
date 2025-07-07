import spacy
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
import re
from tqdm import tqdm

# 加载中文语言模型
try:
    nlp = spacy.load("zh_core_web_sm")
except OSError:
    print("模型未找到，请先安装：python -m spacy download zh_core_web_sm")
    exit(1)

def read_novel(file_path):
    """读取小说文本"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 不存在")
        return None
    except Exception as e:
        print(f"错误：读取文件时发生错误 - {e}")
        return None

def extract_characters(text, top_n=100):
    """提取小说中的人物名称"""
    print("正在提取人物名称...")
    # 使用命名实体识别提取人名
    doc = nlp(text)
    characters = [ent.text for ent in doc.ents if ent.label_ == 'PERSON']

    # 统计人物出现频率
    character_counts = Counter(characters)

    # 过滤单字人名和过于常见的非人名（可根据需要扩展此列表）
    common_non_persons = {'的', '了', '在', '是', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会', '着', '没有', '看', '好', '自己', '这'}
    filtered_characters = [name for name in character_counts if len(name) > 1 and name not in common_non_persons]

    # 获取出现频率最高的top_n个人物
    top_characters = [name for name, count in character_counts.most_common(top_n) if name in filtered_characters]

    print(f"共提取出 {len(top_characters)} 个主要人物")
    return top_characters

def extract_relationships(text, characters):
    """提取人物关系"""
    print("正在提取人物关系...")
    relationships = defaultdict(int)

    # 分割文本为句子
    sentences = re.split(r'[。！？\n]', text)

    # 分析每个句子中的人物关系
    for sentence in tqdm(sentences, desc="分析句子"):
        if not sentence.strip():
            continue

        # 检查句子中出现的人物
        sentence_characters = [char for char in characters if char in sentence]

        # 如果句子中出现多个不同人物，则认为他们之间有关系
        if len(sentence_characters) > 1:
            # 对人物进行排序，确保无论顺序如何都能得到相同的关系对
            sentence_characters.sort()
            for i in range(len(sentence_characters)):
                for j in range(i + 1, len(sentence_characters)):
                    pair = (sentence_characters[i], sentence_characters[j])
                    relationships[pair] += 1

    return relationships

def analyze_relationship_types(text, characters):
    """分析人物关系类型（如亲属、朋友、敌对）"""
    print("正在分析人物关系类型...")
    relationship_types = defaultdict(dict)

    # 定义关系关键词词典
    relationship_keywords = {
        '亲属': ['父亲', '母亲', '儿子', '女儿', '兄弟', '姐妹', '哥哥', '弟弟', '姐姐', '妹妹', '丈夫', '妻子', '爸', '妈', '爹', '娘', '爷爷', '奶奶', '外公', '外婆'],
        '朋友': ['朋友', '好友', '兄弟', '姐妹', '哥们', '姐们', '闺蜜', '发小'],
        '敌对': ['敌人', '对手', '仇人', '对抗', '竞争', '较量'],
        '同事': ['同事', '搭档', '伙伴', '合作', '一起工作'],
        '师徒': ['师傅', '徒弟', '师父', '老师', '学生']
    }

    # 分割文本为句子
    sentences = re.split(r'[。！？\n]', text)

    # 分析每个句子中的人物关系类型
    for sentence in tqdm(sentences, desc="分析关系类型"):
        if not sentence.strip():
            continue

        # 检查句子中出现的人物
        sentence_characters = [char for char in characters if char in sentence]

        if len(sentence_characters) > 1:
            # 检查句子中是否有关键词表明关系类型
            for rel_type, keywords in relationship_keywords.items():
                if any(kw in sentence for kw in keywords):
                    # 找出关键词连接的两个人物
                    for i in range(len(sentence_characters)):
                        for j in range(i + 1, len(sentence_characters)):
                            # 检查关键词是否在两个人物之间
                            pos1 = sentence.find(sentence_characters[i])
                            pos2 = sentence.find(sentence_characters[j])
                            start = min(pos1, pos2)
                            end = max(pos1, pos2)

                            if any(kw in sentence[start:end] for kw in keywords):
                                pair = (sentence_characters[i], sentence_characters[j])
                                # 记录关系类型和关键词
                                if rel_type not in relationship_types[pair]:
                                    relationship_types[pair][rel_type] = 0
                                relationship_types[pair][rel_type] += 1

    return relationship_types

def build_relationship_graph(relationships, relationship_types=None):
    """构建人物关系图"""
    print("正在构建关系图...")
    G = nx.Graph()

    # 添加节点（人物）和边（关系）
    for (char1, char2), weight in relationships.items():
        G.add_edge(char1, char2, weight=weight)

    # 添加关系类型作为边的属性
    if relationship_types:
        for pair, rels in relationship_types.items():
            if pair in G.edges:
                main_rel = max(rels, key=rels.get)
                G.edges[pair]['type'] = main_rel

    return G

def visualize_relationship_graph(G, output_file='relationship_graph.png'):
    """可视化人物关系图"""
    print(f"正在生成关系图并保存到 {output_file}...")

    plt.figure(figsize=(15, 15))

    node_size = [G.degree(node) * 200 for node in G.nodes]

    edge_width = [G[u][v]['weight'] * 0.1 for u, v in G.edges]

    edge_colors = []
    if 'type' in list(G.edges.data())[0][2]:
        rel_color_map = {
            '亲属': 'red',
            '朋友': 'green',
            '敌对': 'blue',
            '同事': 'purple',
            '师徒': 'orange'
        }
        for u, v, data in G.edges.data():
            edge_colors.append(rel_color_map.get(data.get('type', 'gray'), 'gray'))
    else:
        edge_colors = 'gray'

    pos = nx.spring_layout(G, k=0.3, iterations=50)
    nx.draw_networkx_nodes(G, pos, node_size=node_size, alpha=0.7, node_color='lightblue')
    nx.draw_networkx_edges(G, pos, width=edge_width, alpha=0.5, edge_color=edge_colors)
    nx.draw_networkx_labels(G, pos, font_size=12, font_family='SimHei')

    if edge_colors != 'gray':
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor=color, label=rel_type)
                           for rel_type, color in rel_color_map.items()]
        plt.legend(handles=legend_elements, loc='upper right')

    plt.axis('off')
    plt.title('小说人物关系图', fontsize=15, fontfamily='SimHei')
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"关系图已保存到 {output_file}")

def analyze_community(G):
    """分析人物社区（派系）"""
    print("正在分析人物社区...")
    try:
        from community import community_louvain
    except ImportError:
        print("警告：未安装python-louvain库，无法进行社区分析")
        return None

    # 使用Louvain算法进行社区检测
    partition = community_louvain.best_partition(G)

    # 组织社区
    communities = defaultdict(list)
    for node, comm_id in partition.items():
        communities[comm_id].append(node)

    # 按社区大小排序
    sorted_communities = sorted(communities.values(), key=len, reverse=True)

    print(f"共发现 {len(sorted_communities)} 个社区")
    for i, comm in enumerate(sorted_communities):
        print(f"社区 {i+1} ({len(comm)}人): {', '.join(comm[:5])}{'...' if len(comm) > 5 else ''}")

    return sorted_communities

def main():
    # 文件路径
    file_path = input("请输入小说文件路径: ")

    # 读取小说内容
    text = read_novel(file_path)
    if not text:
        return

    # 提取人物
    characters = extract_characters(text)

    # 提取关系
    relationships = extract_relationships(text, characters)

    # 分析关系类型
    relationship_types = analyze_relationship_types(text, characters)

    # 构建关系图
    G = build_relationship_graph(relationships, relationship_types)

    # 可视化关系图
    visualize_relationship_graph(G)

    # 分析社区
    analyze_community(G)

    # 输出人物关系表格
    print("\n主要人物关系表:")
    print("-" * 50)
    print(f"{'人物A':<15}{'人物B':<15}{'关系强度':<15}{'关系类型':<15}")
    print("-" * 50)

    # 按关系强度排序
    sorted_rels = sorted(relationships.items(), key=lambda x: x[1], reverse=True)

    for i, ((char1, char2), weight) in enumerate(sorted_rels[:20]):  # 显示前20个关系
        rel_type = relationship_types.get((char1, char2), {}).copy()
        if not rel_type and (char2, char1) in relationship_types:
            rel_type = relationship_types.get((char2, char1), {})

        main_rel = max(rel_type, key=rel_type.get) if rel_type else "未知"
        print(f"{char1:<15}{char2:<15}{weight:<15}{main_rel:<15}")

if __name__ == "__main__":
    main()