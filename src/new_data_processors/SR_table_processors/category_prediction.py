import pickle
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression


def predict_category():
    # Load existing knowledge if available
    try:
        with open("knowledge.pkl", "rb") as file:
            vectorizer, classifier = pickle.load(file)
    except FileNotFoundError:
        vectorizer = CountVectorizer()
        classifier = LogisticRegression()

    texts = []
    categories = []

    while True:
        # Ask for text input
        text = input("Enter some text: ")

        # Predict the category for the latest text
        predicted_category = (
            classifier.predict(vectorizer.transform([text]))[0] if texts else None
        )

        # Ask for user input if the confidence is low or it's the first text input
        confidence = (
            classifier.predict_proba(vectorizer.transform([text])).max() if texts else 0
        )
        if confidence < 0.8 or not texts:
            print("I'm not sure about the category. Can you help me?")
            # Ask for category input
            category = input("Enter the correct category for the text: ")
        else:
            print("The predicted category is:", predicted_category)
            # Ask if the user wants to accept the prediction
            choice = input("Do you accept this category? (yes/no): ")
            if choice.lower() == "yes":
                category = predicted_category
            else:
                # Ask for category input
                category = input("Enter the correct category for the text: ")

        texts.append(text)
        categories.append(category)

        # Check if there are at least two unique categories
        unique_categories = set(categories)
        if len(unique_categories) < 2:
            print("Please provide at least two different categories.")
            continue

        # Prepare training data
        x = vectorizer.transform(texts)
        y = categories

        # Train the classifier
        classifier.fit(x, y)

        # Ask if the user wants to continue
        choice = input("Do you want to continue? (yes/no): ")
        if choice.lower() != "yes":
            break

    # Save the learned knowledge
    with open("knowledge.pkl", "wb") as file:
        pickle.dump((vectorizer, classifier), file)
