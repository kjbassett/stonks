import tensorflow as tf
from plugins.decorator import plugin
from transformers import TFBertModel


# Define the text encoder using BERT
def get_text_embedding(text_model, input_ids, attention_mask):
    text_outputs = text_model(input_ids, attention_mask=attention_mask)
    last_hidden_state = text_outputs.last_hidden_state
    return tf.keras.layers.Flatten()(last_hidden_state)


@plugin()
def create_combined_model(
    model_name: str,
    num_texts: str,
    structured_input_dim: int,
    combined_hidden_dim: int,
    output_dim: int,
    text_model_name: str = "bert-base-uncased",
    output_activation: str = "sigmoid",
    dropout_rate: float = 0.3,
):
    text_encoder = TFBertModel.from_pretrained(text_model_name)

    # One input layer for all tokenized text inputs and structured numerical input
    input_layer = tf.keras.layers.Input(
        shape=(512 * 2 * num_texts + structured_input_dim,),
        dtype=tf.float32,
        name="combined_input",
    )

    # for each text input, get corresponding slices from the input layer for the input ids and attention masks
    all_text_embeddings = []
    for i in range(num_texts):
        slice_start = i * 512 * 2
        # first 512 tokens are input ids, next 512 are attention masks
        input_ids = tf.cast(
            input_layer[:, slice_start : slice_start + 512],
            dtype=tf.int32,
            name=f"input_ids_{i}",
        )
        attention_mask = tf.cast(
            input_layer[:, slice_start + 512 : slice_start + 512 * 2],
            dtype=tf.int32,
            name=f"attention_mask_{i}",
        )

        embedding = get_text_embedding(text_encoder, input_ids, attention_mask)
        all_text_embeddings.append(embedding)

    structured_input = tf.identity(
        input_layer[:, -structured_input_dim:], name="structured_input"
    )

    # combine all text embeddings and the structured input into a single input layer for the combined model
    combined = tf.keras.layers.Concatenate()([*all_text_embeddings, structured_input])

    # combined model is a fully connected neural network
    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(dropout_rate)(combined)
    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(dropout_rate)(combined)

    output = tf.keras.layers.Dense(output_dim, activation=output_activation)(combined)

    model = tf.keras.Model(inputs=input_layer, outputs=output)
    model.summary()
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])

    model.save(f"{model_name}.h5")

    return model
