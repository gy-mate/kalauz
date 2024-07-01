import os
import re

import numpy as np
from onnxruntime import InferenceSession  # type: ignore
from skl2onnx import to_onnx  # type: ignore
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


class CategoryPredictor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

        self.CATEGORIES = get_categories()
        self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES = 25
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8

        self.imported_session_or_created_pipeline: InferenceSession | Pipeline
        self.import_session_or_create_pipeline()
        self.texts_and_categories: set[tuple[str, list[str]]] = set()

    def import_session_or_create_pipeline(self) -> None:
        try:
            with open(
                os.path.join(
                    os.getcwd(),
                    "data",
                    "05_knowledge",
                    "SR_cause_text_classification_knowledge.onnx",
                ),
                "rb",
            ) as file:
                onnx_data = file.read()
            self.imported_session_or_created_pipeline = InferenceSession(onnx_data)
        except FileNotFoundError:
            vectorizer = TfidfVectorizer()
            classifier = MultiOutputClassifier(SGDClassifier())
            # future: report false positive to JetBrains developers
            # noinspection PyAttributeOutsideInit
            self.imported_session_or_created_pipeline = make_pipeline(
                vectorizer, classifier
            )

    def predict_category(self, text: str) -> list[str]:
        if isinstance(self.imported_session_or_created_pipeline, Pipeline):
            return self.user_input_for_category(text)
        elif isinstance(self.imported_session_or_created_pipeline, InferenceSession):
            input_name = self.imported_session_or_created_pipeline.get_inputs()[0].name
            label_name = self.imported_session_or_created_pipeline.get_outputs()[0].name

            data = np.array([text])
            predictions = self.imported_session_or_created_pipeline.run(
                [label_name], {input_name: data.astype(np.str_)}
            )[0]
            high_confidence_categories = [
                self.CATEGORIES[i]
                for i, score in enumerate(predictions[0])
                if score >= self.HIGH_CONFIDENCE_THRESHOLD
            ]
            return high_confidence_categories or self.user_input_for_category(text)

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
            assert isinstance(self.imported_session_or_created_pipeline, Pipeline)
            self.imported_session_or_created_pipeline.fit(self.texts_and_categories)
            self.save_knowledge()
            self.import_session_or_create_pipeline()
        elif len(self.texts_and_categories) > self.MINIMUM_NUMBER_OF_TRAINING_SAMPLES:
            assert isinstance(
                self.imported_session_or_created_pipeline, InferenceSession
            )
            pass
        else:
            raise NotImplementedError

    def __del__(self) -> None:
        self.save_knowledge()

    def save_knowledge(self) -> None:
        onnx_data = to_onnx(
            self.imported_session_or_created_pipeline,
            x[:1].astype(np.float32),
            target_opset=12,
        )
        with open(
            os.path.join(
                os.getcwd(),
                "data",
                "05_knowledge",
                "SR_cause_text_classification_knowledge.onnx",
            ),
            "wb",
        ) as file:
            file.write(onnx_data.SerializeToString())
