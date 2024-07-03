import csv
import os
import re

import numpy as np
from rich.console import Console
from rich.markdown import Markdown
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
    return categories


def create_pipeline() -> Pipeline:
    vectorizer = TfidfVectorizer()
    classifier = MultiOutputClassifier(SGDClassifier(loss="log_loss"))
    return make_pipeline(vectorizer, classifier)


def clear_terminal():
    os.system("clear||cls")


class CategoryPredictor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

        self.CATEGORIES = get_categories()
        with open(
            os.path.join(
                os.getcwd(),
                "mindmap",
                "SR_cause_categories.md",
            ),
            "r",
        ) as file:
            categories_markdown = file.read()
        self.CATEGORIES_MARKDOWN = Markdown(categories_markdown)
        self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES = 25
        self.SEED = 146
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8

        self.label_binarizer = MultiLabelBinarizer()
        self.markdown_console = Console()

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
            self.check_hamming_loss(self.label_binarizer)
            self.fit_all_data()
        except FileNotFoundError:
            self.logger.warn(
                "No existing cause categorization data found! Starting from scratch..."
            )

    def fit_all_data(self) -> None:
        binarized_categories: np.ndarray = self.label_binarizer.fit_transform(
            self.texts_and_categories["categories"]
        )
        self.pipeline.fit(self.texts_and_categories["texts"], binarized_categories)

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
        probabilities: np.ndarray = self.pipeline.predict_proba([text])
        for probability in probabilities:
            if max(probability.tolist()[0]) < self.HIGH_CONFIDENCE_THRESHOLD:
                self.logger.warn(
                    f"The prediction for '{text}' is not confident enough. Asking for user input..."
                )
                return self.user_input_for_category(text)
        predictions: np.ndarray = self.pipeline.predict([text])
        original_categories = self.label_binarizer.inverse_transform(predictions)
        return [category[0] for category in original_categories]

    def user_input_for_category(self, text: str) -> list[str]:
        input_categories: list[str] = []
        try:
            clear_terminal()
            print(f"The possible categories are:\n")
            self.markdown_console.print(self.CATEGORIES_MARKDOWN)
            print(
                f"\nPlease mark the following text with one or more of the categories above:\n"
                f"'{text}'"
            )
            category_id_input = input(
                "\nEnter one or more categories (or their IDs) separated by ', ': "
            )
            input_categories = category_id_input.split(", ")
            if isinstance(input_categories[0], str):
                return input_categories
            elif isinstance(input_categories[0], int):
                return [
                    self.CATEGORIES[int(category_id) - 1]
                    for category_id in input_categories
                ]
            else:
                raise ValueError("Invalid input!")
        finally:
            self.train_new_data(text, input_categories)

    def train_new_data(self, text: str, categories: list[str]) -> None:
        self.texts_and_categories["texts"].append(text)
        self.texts_and_categories["categories"].append(categories)

        if (
            len(self.texts_and_categories["texts"])
            < self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES
        ):
            self.logger.warn(
                "I don't have enough data to train the model yet. Going to the next text..."
            )
            return
        elif (
            len(self.texts_and_categories["texts"])
            == self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES
        ):
            self.fit_all_data()
            self.__exit__(None, None, None)
            self.__enter__()
        else:
            vectorized_text = self.pipeline.named_steps["tfidfvectorizer"].transform(
                [text]
            )
            binarized_categories = self.label_binarizer.fit_transform(categories)
            self.pipeline.named_steps["multioutputclassifier"].estimator.partial_fit(
                vectorized_text, binarized_categories
            )
            self.logger.info("Model partially trained with a new entry!")

    def __exit__(self, exc_type: None, exc_value: None, traceback: None) -> None:
        if not exc_type and not exc_value and not traceback:
            self.save_knowledge()

    def save_knowledge(self) -> None:
        with open(
            os.path.join(
                os.getcwd(),
                "data",
                "05_knowledge",
                "SR_cause_text_classification_knowledge.csv",
            ),
            "w",
        ) as file:
            writer = csv.writer(file, delimiter=";")
            for text, categories in zip(
                self.texts_and_categories["texts"],
                self.texts_and_categories["categories"],
            ):
                writer.writerow([text, ", ".join(categories)])
        self.logger.info("Knowledge saved successfully!")
