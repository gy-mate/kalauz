import os
import re

import numpy as np
from sklearn.exceptions import NotFittedError  # type: ignore
from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
from sklearn.linear_model import SGDClassifier  # type: ignore
from sklearn.multioutput import MultiOutputClassifier
from sklearn.pipeline import Pipeline, make_pipeline  # type: ignore

from src.new_data_processors.common import DataProcessor


def get_categories() -> list[str]:
    mindmap_location = os.path.join(os.getcwd(), "mindmap", "SR_cause_categories.md")
    with open(mindmap_location, "r") as file:
        content = file.read()
        categories = re.findall(
            pattern=r"(?<=- ).*$", flags=re.MULTILINE, string=content
        )
    return categories


def create_pipeline() -> Pipeline:
    vectorizer = TfidfVectorizer()
    classifier = MultiOutputClassifier(SGDClassifier())
    return make_pipeline(vectorizer, classifier)


class CategoryPredictor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

        self.CATEGORIES = get_categories()
        self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES = 25
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8

        self.pipeline = create_pipeline()
        self.texts_and_categories: set[tuple[str, list[str]]] = set()
        
        self.import_existing_data()

    def import_existing_data(self) -> None:
        try:
            with open(
                os.path.join(
                    os.getcwd(),
                    "data",
                    "05_knowledge",
                    "SR_cause_text_classification_knowledge.csv",
                ),
                "r",
            ) as file:
                for line in file.readlines():
                    text, unsplit_categories = line.split(",")
                    categories = [str(category) for category in unsplit_categories.split(", ")]
                    self.texts_and_categories.add((str(text), categories))
        except FileNotFoundError:
            self.logger.warn("No existing cause categorization data found! Starting from scratch...")

    def predict_category(self, text: str) -> list[str]:
        return self.user_input_for_category(text)

    def user_input_for_category(self, text: str) -> list[str]:
        input_categories: list[str] = []
        try:
            print(
                f"Please categorize the following text: '{text}'\n"
                f"The possible categories are:\n\n"
            )
            for category_id, category in enumerate(self.CATEGORIES):
                print(f"#{category_id + 1}\t\t{category}")
            category_id_input = input(
                "\nSelect one or more categories by entering their numbers separated by ', ': "
            )
            input_categories = [
                self.CATEGORIES[int(category_id) - 1]
                for category_id in category_id_input.split(", ")
            ]
            return input_categories
        finally:
            self.train_new_data(text, input_categories)

    def train_new_data(self, text: str, categories: list[str]) -> None:
        self.texts_and_categories.add((text, categories))

        if len(self.texts_and_categories) < self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            self.logger.warn(
                "I don't have enough data to train the model yet. Going to the next text..."
            )
            return
        if len(self.texts_and_categories) == self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            assert isinstance(self.pipeline, Pipeline)
            self.pipeline.fit(self.texts_and_categories)
            self.save_knowledge()
            create_pipeline()
        elif len(self.texts_and_categories) > self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            pass
        else:
            raise NotImplementedError

    def __del__(self) -> None:
        self.save_knowledge()

    def save_knowledge(self) -> None:
        with open(
            os.path.join(
                os.getcwd(),
                "data",
                "05_knowledge",
                "SR_cause_text_classification_knowledge.onnx",
            ),
            "wb",
        ) as file:
            file.write()
