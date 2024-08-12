from ast import literal_eval
from collections import defaultdict
from typing import Any

import pandas as pd

# future: use plotly.graph_objects instead of Flourish


def parse_markdown_list(markdown: str) -> defaultdict[Any, dict[str, int | defaultdict]]:
    lines = markdown.strip().split("\n")
    stack: list[dict[str, int] | defaultdict] = []
    root: defaultdict[Any, dict[str, int | defaultdict]] = defaultdict(lambda: {"count": 0, "children": defaultdict()})
    lines = skip_frontmatter(lines)
    
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


def skip_frontmatter(lines: list[str]) -> list[str]:
    if lines[0].startswith("---"):
        end_index = 0
        for i, line in enumerate(lines):
            if line.startswith("---") and i != 0:
                end_index = i + 1
                break
        lines = lines[end_index:]
    return lines


def update_counts(category: str, structure: dict, path: list) -> bool:
    if category in structure:
        structure[category]["count"] += 1
        for parent in path:
            parent["count"] += 1
        return True
    for key, value in structure.items():
        if update_counts(category, value["children"], path + [value]):
            return True
    return False


def update_values_from_csv(structure: dict, csv_data: pd.DataFrame) -> None:
    for categories in csv_data.iterrows():
        category_list = literal_eval(categories[1][0])
        for category in category_list:
            update_counts(category, structure, [])


def generate_sankey_data(structure: dict, step: int = 0, parent: str | None = None) -> list:
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


def prune_sankey_data(data: list, max_depth: int) -> list:
    return [row for row in data if row["Step to"] <= max_depth]


def main(markdown_file_path: str, csv_file_path: str, max_depth: int) -> None:
    with open(markdown_file_path, "r") as file:
        markdown_list = file.read()
    parsed_structure = parse_markdown_list(markdown_list)

    csv_df = pd.read_csv(csv_file_path, header=None)
    update_values_from_csv(parsed_structure, csv_df)
    sankey_data = generate_sankey_data(parsed_structure)
    pruned_sankey_data = prune_sankey_data(sankey_data, max_depth)
    df = pd.DataFrame(
        pruned_sankey_data, columns=["Source", "Dest", "Value", "Step from", "Step to"]
    )
    csv_output = df.to_csv(index=False)
    with open("/Users/gymate1/Downloads/sankey_data.csv", "w") as file:
        file.write(csv_output)


if __name__ == "__main__":
    main(
        markdown_file_path="/Users/gymate1/Desktop/drop/python/kalauz/mindmap/SR_cause_categories.md",
        csv_file_path="/Users/gymate1/Downloads/kalauz_speed_restrictions.csv",
        max_depth=4,
    )
