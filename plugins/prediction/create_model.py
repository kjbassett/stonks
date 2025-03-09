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
    num_texts: int,
    structured_input_dim: int,
    n_hidden_layers: int,
    combined_hidden_dim: int,
    output_dim: int,
    text_model_name: str = "M-FAC/bert-tiny-finetuned-mrpc",  # distilbert-base-uncased, M-FAC/bert-tiny-finetuned-mrpc, bert-base-uncased
    output_activation: str = "linear",
    dropout_rate: float = 0.3,
):
    # Load the pre-trained text encoder (BERT)
    text_encoder = TFBertModel.from_pretrained(text_model_name, from_pt=True)
    text_encoder.trainable = False  # freeze the pre-trained text encoder

    # Structured numerical input
    structured_input = tf.keras.layers.Input(shape=(structured_input_dim,), name="structured_input")

    # Textual inputs (input IDs and attention masks for each text)
    inputs = [structured_input]
    text_embeddings = []
    for i in range(num_texts):
        text_input = tf.keras.layers.Input(shape=(512,), dtype=tf.int32, name=f"input_ids_{i + 1}")
        attention_mask = tf.keras.layers.Input(shape=(512,), dtype=tf.int32, name=f"attention_mask_{i + 1}")
        inputs.extend([text_input, attention_mask])

        # Get embedding for each text input
        embedding = text_encoder(input_ids=text_input, attention_mask=attention_mask).last_hidden_state  # pooler_output
        text_embeddings.append(tf.keras.layers.Flatten()(embedding))

    # combine all text embeddings and the structured input into a single input layer for the combined model
    combined = tf.keras.layers.Concatenate()([structured_input, *text_embeddings])

    # Fully connected layers
    for _ in range(n_hidden_layers):
        combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
        combined = tf.keras.layers.Dropout(dropout_rate)(combined)

    output = tf.keras.layers.Dense(output_dim, activation=output_activation)(combined)

    model = tf.keras.Model(inputs=inputs, outputs=output)
    model.summary()
    optimizer = tf.keras.optimizers.Adam()
    model.compile(optimizer=optimizer, loss=smape)

    tf.keras.utils.plot_model(
        model,
        to_file="model.png",
        show_shapes=True,
        show_layer_names=True,
        expand_nested=True,
    )

    return model


def smape(y_true, y_pred):
    """
    Calculate Symmetric Mean Absolute Percentage Error (sMAPE).

    Parameters:
    - y_true: tf.Tensor
        The true values.
    - y_pred: tf.Tensor
        The predicted values.

    Returns:
    - smape: tf.Tensor
        The sMAPE value.
    """
    return tf.reduce_mean(2 * tf.abs(y_true - y_pred) / (tf.abs(y_true) + tf.abs(y_pred))) * 100