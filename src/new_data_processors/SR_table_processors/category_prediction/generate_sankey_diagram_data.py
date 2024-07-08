from ast import literal_eval
from collections import defaultdict

import pandas as pd


def parse_markdown_list(markdown):
    lines = markdown.strip().split("\n")
    stack = []
    root = defaultdict(lambda: {"count": 0, "children": defaultdict()})
    
    for line in lines:
        indent_level = (len(line) - len(line.lstrip())) // 4
        item = line.strip("- ").strip()
        while len(stack) > indent_level:
            stack.pop()
            
        if stack:
            current = stack[-1]["children"]
        else:
            current = root

        if item not in current:
            current[item] = {"count": 0, "children": defaultdict()}
        current[item]["count"] += 1
        stack.append(current[item])
    return root


def update_values_from_csv(structure, csv_data):
    for categories in csv_data.iterrows():
        category_list = literal_eval(categories[1][0])
        value = 1 / len(category_list) if len(category_list) > 1 else 1
        for category in category_list:
            # Navigate the structure to find the corresponding category
            for parent, parent_data in structure.items():
                if category in parent_data["children"]:
                    parent_data["children"][category]["count"] += value


def generate_sankey_data(structure, step=0, parent=None):
    data = []
    for key, value in structure.items():
        if parent is not None:
            data.append(
                {
                    "Source": parent,
                    "Dest": key,
                    "Value": value["count"],
                    "Step from": step,
                    "Step to": step + 1,
                }
            )
        if value["children"]:
            data.extend(generate_sankey_data(value["children"], step + 1, key))
    return data


def main() -> None:
    with open(
        "/Users/gymate1/Desktop/drop/python/kalauz/mindmap/SR_cause_categories.md", "r"
    ) as file:
        markdown_list = file.read()
    parsed_structure = parse_markdown_list(markdown_list)

    csv_df = pd.read_csv("/Users/gymate1/Downloads/kalauz_speed_restrictions.csv", header=None)
    update_values_from_csv(parsed_structure, csv_df)
    sankey_data = generate_sankey_data(parsed_structure)
    df = pd.DataFrame(
        sankey_data, columns=["Source", "Dest", "Value", "Step from", "Step to"]
    )
    csv_output = df.to_csv(index=False)
    print(csv_output)


if __name__ == "__main__":
    main()
