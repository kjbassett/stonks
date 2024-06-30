import numpy as np
import tensorflow as tf
from icecream import ic
from transformers import TFBertModel, BertTokenizer


# Define the text encoder using BERT
def get_text_embedding(text_model, input_ids, attention_mask):
    text_outputs = text_model(input_ids, attention_mask=attention_mask)
    last_hidden_state = text_outputs.last_hidden_state
    return tf.keras.layers.Flatten()(last_hidden_state)


# Combine both encoders and define the final model
def create_combined_model(
    text_model_name, num_texts, structured_input_dim, combined_hidden_dim, output_dim
):
    text_encoder = TFBertModel.from_pretrained(text_model_name)

    input_layer = tf.keras.layers.Input(
        shape=(512 * 2 * num_texts + structured_input_dim,),
        dtype=tf.float32,
        name="combined_input",
    )

    # variable number of text inputs
    all_text_embeddings = []
    for i in range(num_texts):
        slice_start = i * 512 * 2
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

    combined = tf.keras.layers.Concatenate()([*all_text_embeddings, structured_input])

    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(0.3)(combined)
    combined = tf.keras.layers.Dense(combined_hidden_dim, activation="relu")(combined)
    combined = tf.keras.layers.Dropout(0.3)(combined)

    output = tf.keras.layers.Dense(output_dim, activation="sigmoid")(combined)

    model = tf.keras.Model(inputs=input_layer, outputs=output)
    model.summary()
    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])

    return model


if __name__ == "__main__":
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")

    # some fake data for testing
    texts = [
        ["Buying this is a great idea."] * 3,
        ["Selling this is a great idea."] * 3,
        ["Buying this is a bad idea."] * 3,
        ["Selling this is a bad idea."] * 3,
        ["Buy this now."] * 3,
        ["Sell this now."] * 3,
        ["The price will increase."] * 3,
        ["The price will decrease."] * 3,
    ] * 300

    all_inputs = []
    buy = True
    for text_group in texts:
        group_inputs = np.array([])
        for text in text_group:
            tokenized_text = tokenizer(
                text,
                padding="max_length",
                truncation=True,
                max_length=512,
                return_attention_mask=True,
                return_tensors="tf",
            )
            input_ids = tokenized_text["input_ids"]
            attention_mask = tokenized_text["attention_mask"]
            group_inputs = np.append(group_inputs, input_ids)
            group_inputs = np.append(group_inputs, attention_mask)
        if buy:
            group_inputs = np.append(group_inputs, np.ones(10))
        else:
            group_inputs = np.append(group_inputs, np.zeros(10))
        buy = not buy
        all_inputs.append(group_inputs)
    all_inputs = np.array(all_inputs)
    ic(all_inputs)
    ic(all_inputs.shape)

    # Example labels
    labels = np.array([1, 0] * int(len(texts) / 2))

    # Create the combined model
    combined_model = create_combined_model(
        "bert-base-uncased",
        len(texts[0]),
        structured_input_dim=10,
        combined_hidden_dim=128,
        output_dim=1,
    )

    ic(combined_model.inputs)

    # Train the model with the sampled training dataset and validate with the sampled validation dataset
    combined_model.fit(
        all_inputs,
        labels,
        batch_size=1,
        epochs=70,
        steps_per_epoch=10,
        shuffle=True,
        validation_split=0.25,
    )

    # Make predictions
    predictions = combined_model.predict([all_inputs])
    ic(predictions)

    # add, commit, and push, ya ding dong
    # 1. decide which data to get
    # 2. get data
    # 3. test
