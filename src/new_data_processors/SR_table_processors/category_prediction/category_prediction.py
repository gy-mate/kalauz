import csv
from difflib import SequenceMatcher
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

from src.new_data_processors.SR_table_processors.category_prediction.text_similarities import (
    TextSimilarity,
)
from src.new_data_processors.SR_table_processors.category_prediction.training_data import (
    TrainingData,
)
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
        self.TEXT_SIMILARITY_THRESHOLD = 0.7
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8

        self.label_binarizer = MultiLabelBinarizer()
        self.markdown_console = Console()

    def __enter__(self) -> "CategoryPredictor":
        self.pipeline = create_pipeline()
        self.training_data: list[TrainingData] = []
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
                    text: str
                    all_categories: str
                    text, all_categories = row
                    categories = [
                        category.lower() for category in all_categories.split(", ")
                    ]
                    if self.all_categories_are_valid(categories):
                        self.training_data.append(TrainingData(text, categories))
                    else:
                        raise ValueError(
                            f"Categories {[category if category not in self.CATEGORIES else None for category in categories]} "
                            f"are invalid among the existing data!"
                        )
            self.check_hamming_loss()
            self.fit_all_data()
        except FileNotFoundError:
            self.logger.warn(
                "No existing cause categorization data found! Starting from scratch..."
            )

    def fit_all_data(self) -> None:
        binarized_categories: np.ndarray = self.label_binarizer.fit_transform(
            [training_data.categories for training_data in self.training_data]
        )
        self.pipeline.fit(
            [training_data.text for training_data in self.training_data],
            binarized_categories,
        )

    def check_hamming_loss(self) -> None:
        texts_train, texts_test, categories_train, categories_test = train_test_split(
            [training_data.text for training_data in self.training_data],
            [training_data.categories for training_data in self.training_data],
            test_size=0.33,
            random_state=self.SEED,
        )
        binarized_categories_train = self.label_binarizer.fit_transform(
            categories_train
        )
        binarized_categories_test = self.label_binarizer.transform(categories_test)
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

    def predict_category(self, text: str) -> str:
        match self.text_similarity(text):
            case TextSimilarity.Same:
                for pair in self.training_data:
                    if pair.text == text:
                        return str(pair.categories)
                raise ValueError(
                    "The same text somehow isn't in the training data! This is unexpected."
                )
            case TextSimilarity.Different:
                self.logger.warn(
                    f"The text '{text}' is greatly different from all texts in the training data."
                )
                return str(self.user_input_for_category(text))
            case TextSimilarity.Similar:
                return str(self.get_predictions_or_input(text))

    def get_predictions_or_input(self, text: str) -> list[str]:
        probabilities: np.ndarray = self.pipeline.predict_proba([text])
        for probability in probabilities:
            if max(probability.tolist()[0]) < self.HIGH_CONFIDENCE_THRESHOLD:
                self.logger.warn(
                    f"The prediction for '{text}' is not confident enough."
                )
                return self.user_input_for_category(text)
        predictions: np.ndarray = self.pipeline.predict([text])
        if not predictions.any():
            self.logger.warn(f"The prediction for '{text}' returned no categories.")
            return self.user_input_for_category(text)
        original_categories: list[tuple] = self.label_binarizer.inverse_transform(
            predictions
        )
        return [category[0] for category in original_categories]

    def text_similarity(self, text: str) -> TextSimilarity:
        texts_to_check = [pair.text for pair in self.training_data]
        maximum_similarity = 0.0
        for text_to_check in texts_to_check:
            if (
                similarity := SequenceMatcher(None, text, text_to_check).ratio()
            ) > maximum_similarity:
                if similarity == 1.0:
                    return TextSimilarity.Same
                maximum_similarity = similarity
        match maximum_similarity:
            case similarity if similarity >= self.TEXT_SIMILARITY_THRESHOLD:
                return TextSimilarity.Similar
            case _:
                return TextSimilarity.Different

    def user_input_for_category(self, text: str) -> list[str]:
        self.logger.info("Asking for user input...")
        # TODO: uncomment the lines below in production
        # clear_terminal()
        # print(f"\nThe possible categories are:\n")
        # self.markdown_console.print(self.CATEGORIES_MARKDOWN)
        print(
            f"\nPlease mark the following text with one or more of the categories above:\n"
            f"'{text}'"
        )
        while True:
            try:
                category_id_input = input(
                    "\nEnter one or more categories separated by ', ': "
                )
                input_categories = [
                    category.strip().lower()
                    for category in category_id_input.split(", ")
                ]
                if self.all_categories_are_valid(input_categories):
                    print()
                    self.train_with_new_data(text, input_categories)
                    return input_categories
                elif not input_categories:
                    self.logger.warn("No categories were provided!")
                else:
                    raise ValueError
            except (UnicodeDecodeError, ValueError):
                self.logger.warn("At least one input category is invalid!")

    def all_categories_are_valid(self, input_categories: list[str]) -> bool:
        return all(category in self.CATEGORIES for category in input_categories)

    def train_with_new_data(self, text: str, categories: list[str]) -> None:
        self.training_data.append(TrainingData(text, categories))

        if len(self.training_data) < self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            self.logger.warn(
                "I don't have enough data to train the model yet. Going to the next text..."
            )
            return
        elif len(self.training_data) == self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            self.export_and_import_knowledge()
        else:
            for category in categories:
                self.add_new_categories_to_model(category)
            self.partially_train_with_new_data(categories, text)
            self.logger.info("Model partially trained with a new entry!")

    def partially_train_with_new_data(self, categories: list[str], text: str) -> None:
        binarized_categories = self.label_binarizer.transform([categories])
        vectorized_text = self.pipeline.named_steps["tfidfvectorizer"].transform([text])
        classifiers = self.pipeline.named_steps["multioutputclassifier"].estimators_
        for i, classifier in enumerate(classifiers):
            classifier.partial_fit(
                vectorized_text,
                binarized_categories[:, i],
                classes=np.array([0, 1]),
            )

    def add_new_categories_to_model(self, category: str) -> None:
        if category not in self.label_binarizer.classes_:
            self.label_binarizer.fit([pair.categories for pair in self.training_data])
            new_classifier = SGDClassifier(loss="log_loss")
            self.pipeline.named_steps["multioutputclassifier"].estimators_.append(
                new_classifier
            )

    def export_and_import_knowledge(self) -> None:
        self.fit_all_data()
        self.__exit__(None, None, None)
        self.__enter__()

    def __exit__(self, exc_type: None, exc_value: None, traceback: None) -> None:
        self.save_knowledge()

    def save_knowledge(self) -> None:
        # TODO: switch to append mode
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
                [training_data.text for training_data in self.training_data],
                [training_data.categories for training_data in self.training_data],
            ):
                if text and categories:
                    writer.writerow([text, ", ".join(categories)])
        self.logger.info("Cause text categorization knowledge saved successfully!")
