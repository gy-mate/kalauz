import os
import pickle
import re

from sklearn.exceptions import NotFittedError
from sklearn.feature_extraction.text import CountVectorizer  # type: ignore
from sklearn.linear_model import LogisticRegression  # type: ignore


def input_categories(text: str) -> None:
    category = input(
        f"I'm not sure about the category of '{text}'. Enter the correct category: "
    )


class CategoryPredictor:
    def __init__(self):
        try:
            with open(
                os.path.join(
                    os.getcwd(),
                    "data",
                    "05_knowledge",
                    "decision-tree-classification-knowledge.pkl",
                ),
                "rb",
            ) as file:
                self.vectorizer, self.classifier = pickle.load(file)
        except FileNotFoundError:
            self.vectorizer = CountVectorizer()
            self.classifier = LogisticRegression()

    def predict_category(self, text: str) -> tuple[str, str, str]:
        mindmap_location = os.path.join(
            os.getcwd(), "mindmap", "SR_cause_categories.md"
        )
        with open(mindmap_location, "r") as file:
            content = file.read()
            categories = re.findall(
                pattern=r"(?<=- ).*$", flags=re.MULTILINE, string=content
            )

        try:
            predicted_categories = self.classifier.predict(self.vectorizer.transform([text]))
            confidence = self.classifier.predict_proba(
                self.vectorizer.transform([text])
            ).max()
            if confidence < 0.8:
                input_categories(text)
            else:
                choice = input(
                    f"The predicted categories are: {predicted_categories}. Do you accept this category? (y/n): "
                )
        except NotFittedError:
            input_categories(text)

        x = self.vectorizer.transform(texts)
        y = categories

        # Train the classifier
        self.classifier.fit(x, y)
    
    def dump_knowledge(self) -> None:
        with open(
            os.path.join(
                os.getcwd(),
                "data",
                "05_knowledge",
                "decision-tree-classification-knowledge.pkl",
            ),
            "wb",
        ) as file:
            pickle.dump((self.vectorizer, self.classifier), file)
