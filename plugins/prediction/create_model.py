import numpy as np
import tensorflow as tf
from transformers import TFBertModel, BertTokenizer


# Define the text encoder using BERT
def create_text_encoder(text_model_name, max_seq_length=512):
    text_input_ids = tf.keras.layers.Input(
        shape=(max_seq_length,), dtype=tf.int32, name="input_ids"
    )
    text_attention_mask = tf.keras.layers.Input(
        shape=(max_seq_length,), dtype=tf.int32, name="attention_mask"
    )

    text_model = TFBertModel.from_pretrained(text_model_name)
    # text_model.trainable = True
    text_outputs = text_model(text_input_ids, attention_mask=text_attention_mask)
    # (batch_size, sequence_length, hidden_size)[:, 0, :], Gets CLS token which represents the entire sequence
    text_embeddings = text_outputs[0][:, 0, :]
    # Create a Keras model
    text_encoder = tf.keras.Model(
        inputs=[text_input_ids, text_attention_mask],
        outputs=text_embeddings,
        name="text_encoder",
    )

    return text_encoder


# Combine both encoders and define the final model
def create_combined_model(
    text_model_name, structured_input_dim, combined_hidden_dim, output_dim
):
    text_encoder = create_text_encoder(text_model_name)

    structured_input = tf.keras.layers.Input(
        shape=(structured_input_dim,), name="structured_input"
    )
    text_input = text_encoder.input
    text_embeddings = text_encoder.output

    combined = tf.keras.layers.concatenate([text_embeddings, structured_input])

    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    # combined = tf.keras.layers.Dropout(0.3)(combined)
    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    # combined = tf.keras.layers.Dropout(0.3)(combined)

    output = tf.keras.layers.Dense(output_dim, activation="sigmoid")(combined)

    model = tf.keras.Model(inputs=[*text_input, structured_input], outputs=output)
    print(model.summary())
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])

    return model


if __name__ == "__main__":
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

    # Example text data
    texts = [
        "Buying this is a great idea.",
        "Selling this is a great idea.",
        "Buying this is a bad idea.",
        "Selling this is a bad idea.",
        "Buy this now.",
        "Sell this now.",
        "The price will increase.",
        "The price will decrease.",
    ] * 300
    tokenized_texts = tokenizer(
        texts,
        padding="max_length",
        truncation=True,
        max_length=512,
        return_attention_mask=True,
        return_tensors="tf",
    )
    input_ids = tokenized_texts["input_ids"]
    attention_mask = tokenized_texts["attention_mask"]

    # Example structured data
    structured_data = np.zeros((len(texts), 10))
    structured_data[::2, :] = 1

    # Example labels
    labels = np.array([1, 0] * int(len(texts) / 2))

    # Create the combined model
    combined_model = create_combined_model(
        "bert-base-uncased",
        structured_input_dim=10,
        combined_hidden_dim=128,
        output_dim=1,
    )

    # Train the model with the sampled training dataset and validate with the sampled validation dataset
    combined_model.fit(
        [input_ids, attention_mask, structured_data],
        labels,
        batch_size=1,
        epochs=70,
        steps_per_epoch=10,
        shuffle=True,
        validation_split=0.05,
    )

    # Make predictions
    predictions = combined_model.predict([input_ids, attention_mask, structured_data])
    print(predictions)

    # add, commit, and push, ya ding dong
    # 1. decide which data to get
    # 2. get data
    # 3. test
