import csv
import os
import re

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
from sklearn.linear_model import SGDClassifier  # type: ignore
from sklearn.metrics import hamming_loss  # type: ignore
from sklearn.model_selection import train_test_split  # type: ignore
from sklearn.multioutput import MultiOutputClassifier  # type: ignore
from sklearn.pipeline import Pipeline, make_pipeline  # type: ignore
from sklearn.preprocessing import MultiLabelBinarizer  # type: ignore

from src.new_data_processors.common import DataProcessor


def get_categories() -> list[str]:
    mindmap_location = os.path.join(os.getcwd(), "mindmap", "SR_cause_categories.md")
    with open(mindmap_location, "r") as file:
        content = file.read()
        categories = re.findall(
            pattern=r"(?<=- ).*$", flags=re.MULTILINE, string=content
        )
    return sorted(categories)


def create_pipeline() -> Pipeline:
    vectorizer = TfidfVectorizer()
    classifier = MultiOutputClassifier(SGDClassifier())
    return make_pipeline(vectorizer, classifier)


class CategoryPredictor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        
        self.CATEGORIES = get_categories()
        self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES = 25
        self.SEED = 146
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8
        
    def __enter__(self) -> "CategoryPredictor":
        self.pipeline = create_pipeline()
        self.texts_and_categories: dict[str, list[str | list[str]]] = {
            "texts": [],
            "categories": [],
        }
        self.import_existing_data()
        return self

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
                reader = csv.reader(file, delimiter=";")
                for row in reader:
                    text, all_categories = row
                    categories = all_categories.split(", ")
                    self.texts_and_categories["texts"].append(text)
                    self.texts_and_categories["categories"].append(categories)
            label_binarizer = MultiLabelBinarizer()
            self.check_hamming_loss(label_binarizer)
            binarized_categories = label_binarizer.fit_transform(
                self.texts_and_categories["categories"]
            )
            self.pipeline.fit(self.texts_and_categories["texts"], binarized_categories)
        except FileNotFoundError:
            self.logger.warn(
                "No existing cause categorization data found! Starting from scratch..."
            )

    def check_hamming_loss(
        self,
        label_binarizer: MultiLabelBinarizer,
    ) -> None:
        texts_train, texts_test, categories_train, categories_test = train_test_split(
            self.texts_and_categories["texts"],
            self.texts_and_categories["categories"],
            test_size=0.33,
            random_state=self.SEED,
        )
        binarized_categories_train = label_binarizer.fit_transform(categories_train)
        binarized_categories_test = label_binarizer.transform(categories_test)
        self.pipeline.fit(texts_train, binarized_categories_train)
        test_predictions = self.pipeline.predict(texts_test)
        
        current_hamming_loss = hamming_loss(binarized_categories_test, test_predictions)
        match current_hamming_loss:
            case loss if loss < 0.1:
                current_hamming_loss_quality = "excellent"
            case loss if loss < 0.2:
                current_hamming_loss_quality = "good"
            case loss if loss < 0.3:
                current_hamming_loss_quality = "acceptable"
            case _:
                current_hamming_loss_quality = "poor"
        self.logger.info(
            f"Initial Hamming loss: {current_hamming_loss:.3f}. "
            f"That's {current_hamming_loss_quality}!"
        )

    def predict_category(self, text: str) -> list[str]:
        self.pipeline.predict([text])
        probabilities: np.ndarray = self.pipeline.predict_proba([text])
        category_confidences = {}
        for i, category in enumerate(self.CATEGORIES):
            confidence = min(probabilities[i][0])
            category_confidences[category] = confidence
        return category_confidences
        
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
        self.texts_and_categories["texts"].append(text)
        self.texts_and_categories["categories"].append(categories)

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

    def __exit__(self, exc_type: None, exc_value: None, traceback: None) -> None:
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
