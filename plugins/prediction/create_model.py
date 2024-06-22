import numpy as np
import tensorflow as tf
from transformers import BertTokenizer
from transformers import TFBertModel


# Define the text encoder using BERT
def create_text_encoder(text_model_name, max_seq_length=512):
    text_input_ids = tf.keras.layers.Input(
        shape=(max_seq_length,), dtype=tf.int32, name="input_ids"
    )
    text_attention_mask = tf.keras.layers.Input(
        shape=(max_seq_length,), dtype=tf.int32, name="attention_mask"
    )

    text_model = TFBertModel.from_pretrained(text_model_name)
    text_outputs = text_model(text_input_ids, attention_mask=text_attention_mask)
    text_embeddings = text_outputs[0][:, 0, :]

    text_encoder = tf.keras.Model(
        inputs=[text_input_ids, text_attention_mask],
        outputs=text_embeddings,
        name="text_encoder",
    )

    return text_encoder


# Combine both encoders and define the final model
def create_combined_model(
    text_encoder, structured_input_dim, combined_hidden_dim, output_dim
):
    structured_input = tf.keras.layers.Input(
        shape=(structured_input_dim,), name="structured_input"
    )

    text_input_ids = text_encoder.input[0]
    text_attention_mask = text_encoder.input[1]
    text_embeddings = text_encoder.output

    combined = tf.keras.layers.concatenate([text_embeddings, structured_input])

    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(0.3)(combined)
    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(0.3)(combined)

    output = tf.keras.layers.Dense(output_dim, activation="sigmoid")(combined)

    model = tf.keras.Model(
        inputs=[text_input_ids, text_attention_mask, structured_input], outputs=output
    )
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])

    return model


if __name__ == "__main__":
    # Initialize tokenizer
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

    # Example text data
    texts = [
        "This is a sample sentence.",
        "Another example sentence.",
    ]  # implies a batch size of 2
    structured_data = np.random.rand(2, 10)  # random 2 x 10 data

    # Tokenize the text data
    encoded_inputs = tokenizer(
        texts,
        padding="max_length",
        truncation=True,
        max_length=512,
        return_tensors="tf",
    )
    input_ids = encoded_inputs["input_ids"]
    attention_mask = encoded_inputs["attention_mask"]

    # Create the text encoder
    text_encoder = create_text_encoder("bert-base-uncased", max_seq_length=512)

    # Create the combined model
    combined_model = create_combined_model(
        text_encoder, structured_input_dim=10, combined_hidden_dim=64, output_dim=1
    )

    # Train the model (example)
    combined_model.fit(
        [input_ids, attention_mask, structured_data], np.array([1, 0]), epochs=1
    )

    # Make predictions
    predictions = combined_model.predict([input_ids, attention_mask, structured_data])
    print(predictions)
