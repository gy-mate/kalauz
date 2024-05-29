import pickle
from sklearn.feature_extraction.text import CountVectorizer  # type: ignore
from sklearn.linear_model import LogisticRegression  # type: ignore


def predict_category(text: str) -> str:
    texts: list[str] = []
    categories: list[str] = []
    category = ""
    try:
        try:
            with open("existing-categorisation-knowledge.pkl", "rb") as file:
                vectorizer, classifier = pickle.load(file)
        except FileNotFoundError:
            vectorizer = CountVectorizer()
            classifier = LogisticRegression()

        while True:
            predicted_category = (
                classifier.predict(vectorizer.transform([text]))[0] if texts else None
            )
            confidence = (
                classifier.predict_proba(vectorizer.transform([text])).max()
                if texts
                else 0
            )
            if confidence < 0.8 or not texts:
                category = input(f"I'm not sure about the category of '{text}'. Enter the correct category: ")
                return category
            else:
                choice = input(f"The predicted category is: {predicted_category}. Do you accept this category? (y/n): ")
                if choice.lower() == "y":
                    category = predicted_category
                    return category
                else:
                    category = input("Enter the correct category for the text: ")
                    return category
    finally:
        texts.append(text)
        categories.append(category)

        # Prepare training data
        x = vectorizer.transform(texts)
        y = categories

        # Train the classifier
        classifier.fit(x, y)

        with open("existing-categorisation-knowledge.pkl", "wb") as file:
            pickle.dump((vectorizer, classifier), file)
