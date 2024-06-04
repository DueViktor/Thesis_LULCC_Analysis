"""
Main script for calculating embeddings for the chips.

"""

from collections import defaultdict
from typing import Dict

import numpy as np
import pacmap
import pandas as pd
from sklearn.cluster import KMeans

from config import COUNTRY_COLORS
from src.DataBaseManager import DBMS


def generate(area="Denmark", verbose=True):
    assert area in ["Denmark", "Estonia", "Netherlands", "Israel"], "Invalid area"

    # get all chips
    DB = DBMS()

    if verbose:
        print(f"Reading data for {area}...")
    chip_graphs = DB.read(
        "GET_CHIP_GRAPH",
        {"_YEAR_FROM_": "2016", "_YEAR_TO_": "2023", "_AREA_": area},
    )

    if verbose:
        print("Number of chips: ", chip_graphs.shape[0])

    # Find the distinct chips
    distinct_chip_ids = chip_graphs[["area", "chipid"]].drop_duplicates()

    if verbose:
        print("Number of distinct chips: ", distinct_chip_ids.shape[0])

    embeddings = defaultdict(dict)
    if verbose:
        print(f"Calculating embeddings for {area}...")
    for k, row in distinct_chip_ids.iterrows():
        chip = row["chipid"]
        country = row["area"]

        view = chip_graphs[
            (chip_graphs["chipid"] == chip) & (chip_graphs["area"] == country)
        ]

        # get graph
        A = format_chip_graph(view)

        # SVD
        embeddings[country][chip] = A

    return embeddings


def format_chip_graph(chip_graph_df: pd.DataFrame):
    """Takes a dataframe of a chip graph and formats it into a matrix representing the embeddings for the chip."""
    lulc_categories = [
        "Grass",
        "Crops",
        "Wind Turbine",
        "Snow & Ice",
        "Trees",
        "Solar Panel",
        "Bare ground",
        "Flooded vegetation",
        "Shrub & Scrub",
        "Built Area",
    ]

    A = np.zeros((len(lulc_categories), len(lulc_categories)))
    for i, lc1 in enumerate(lulc_categories):
        for j, lc2 in enumerate(lulc_categories):
            # Category is the same
            if i == j:
                A[i, j] = 0
                continue

            view = chip_graph_df[
                (chip_graph_df["lulc_category_from"] == lc1)
                & (chip_graph_df["lulc_category_to"] == lc2)
            ]

            # No changes
            if view.shape[0] == 0:
                A[i, j] = 0

            # Look up the area from category i to category j
            else:
                A[i, j] = view["changed_area"].sum()

    return A


def scale(X: np.ndarray, verbose=True) -> np.ndarray:
    """
    Scales the input data to have a mean of 0 and a standard deviation of 1.

    This function scales the input data to have a mean of 0 and a standard deviation of 1. It then returns the scaled data.

    Parameters:
    - The data to be scaled. This should be a 2D NumPy array where each row represents an observation and each column represents a feature.
    """
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    X_scaled: np.ndarray = scaler.fit_transform(X)

    if verbose:
        print(f"Scaled data from {X.shape} to {X_scaled.shape}.")
        print(
            f"The scaled data has a mean of {np.mean(X_scaled):.2f} and a standard deviation of {np.std(X_scaled):.2f}."
        )

    return X_scaled


def project(X: np.ndarray, n_components=5, verbose=True) -> np.ndarray:
    """
    Projects high-dimensional data into 5D space using PaCMAP for dimension reduction.

    This function reduces the dimensionality of the input data using PaCMAP, a manifold learning technique. It then returns the dimensionally reduced data.

    Parameters:
    - The high-dimensional data to be projected. This should be a 2D NumPy array where each row represents an observation and each column represents a feature.
    - n_components: The number of components to project the data into. Default value is 2.
    """
    embedding = pacmap.PaCMAP(
        n_components=n_components, n_neighbors=None, MN_ratio=0.5, FP_ratio=2.0, random_state=42
    )
    X_transformed: np.ndarray = embedding.fit_transform(X, init="pca")

    if verbose:
        print(f"Projected data into from {X.shape} to {X_transformed.shape}.")

    return X_transformed


def cluster(X_transformed: np.ndarray, n_clusters=5, verbose=True) -> KMeans:
    """
    Clusters high-dimensional data into specified number of clusters using PaCMAP for dimension reduction followed by k-means clustering.

    This function first reduces the dimensionality of the input data using PaCMAP, a manifold learning technique. It then applies k-means clustering to the dimensionally reduced data to form the specified number of clusters.

    Parameters:
    - n_clusters: The number of clusters to form. Default value is 5.
    - verbose: If True, prints the number of clusters trained by the k-means algorithm. Default value is True.

    Returns:
    - KMeans: An instance of sklearn's KMeans class after fitting it to the clustered data. This object contains information about the cluster centers and labels.
    """
    kmeans = KMeans(n_clusters=n_clusters,random_state=42, init="k-means++")
    kmeans.fit(X_transformed)

    if verbose:
        print(
            f"Trained kmeans with {n_clusters} clusters on data with shape {X_transformed.shape}."
        )

    return kmeans


def dict_to_matrix(embeddings: Dict[str, Dict[str, np.ndarray]]) -> np.ndarray:
    colors = []
    matrix_list = []
    index_2_chip = dict()
    colmap = COUNTRY_COLORS
    ix = 0
    for country, chips in embeddings.items():
        for chip, embedding in chips.items():
            colors.append(colmap[country])
            matrix_list.append(embedding)

            index_2_chip[ix] = (country, chip)
            ix += 1

    matrices = np.array(matrix_list)

    # Step 1: Flatten the matrices

    X = matrices.reshape(
        matrices.shape[0], -1
    )  # Reshape from (n, 10, 10) to (n, 100) where n is distinct chips

    return X, index_2_chip, colors
