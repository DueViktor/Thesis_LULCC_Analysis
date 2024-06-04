from collections import defaultdict
from typing import Dict

import numpy as np
import pandas as pd

from src.DataBaseManager import DBMS


def LULC_FROM(country: str):
    assert country in [
        "Denmark",
        "Estonia",
        "Netherlands",
        "Israel",
    ], "Country not in the list"

    DB = DBMS()
    chip_graphs = DB.read(
        "GET_CHIP_GRAPH",
        {"_AREA_": country, "_YEAR_FROM_": "2016", "_YEAR_TO_": "2023"},
    )

    return chip_graphs


COUNTRY_LULC = {
    "Denmark": LULC_FROM("Denmark"),
    "Estonia": LULC_FROM("Estonia"),
    "Netherlands": LULC_FROM("Netherlands"),
    "Israel": LULC_FROM("Israel"),
}


def generate_cluster_LC(cluster_labels, index_2_chip) -> Dict[int, pd.DataFrame]:
    """Generate the LC dataframe for each cluster."""

    cluster_LCs = {}
    for cluster_id in set(cluster_labels):
        cluster_indexes = [i for i, x in enumerate(cluster_labels) if x == cluster_id]
        cluster_LC = get_cluster_LC(cluster_indexes, index_2_chip)
        cluster_LCs[cluster_id] = cluster_LC

    return cluster_LCs


def get_cluster_LC(cluster_indexes, index_2_chip):
    change_df = pd.DataFrame(
        columns=["lulc_category_from", "lulc_category_to", "changed_area"]
    )

    for index in cluster_indexes:
        country, chip_id = index_2_chip[index]

        lulc_df = COUNTRY_LULC[country]
        lulc_df = lulc_df[lulc_df["chipid"] == chip_id]

        change_df = pd.concat([change_df, lulc_df])

    change_df = (
        change_df[["lulc_category_from", "lulc_category_to", "changed_area"]]
        .groupby(["lulc_category_from", "lulc_category_to"])
        .sum()
        .reset_index()
    )
    return change_df


def generate_cluster_TFIDF(
    cluster_LCs: Dict[int, pd.DataFrame],
) -> Dict[int, pd.DataFrame]:
    """Generate the TFIDF dataframe for each cluster.

    The formula for classic tf-idf is:
    tfidf = tf * idf
    where:
    tf = TF(t,d) = number of times term t appears in document d
    idf = IDF(t) = log( (1 + N) / (1 + df(d, t)) ) + 1

    In our case, we can use the following logic:
    A document is a cluster
    A term is the combination of a LULC category from and to
    A term frequency is the area change of a LULC category in a cluster
    An inverse document frequency is the area change of a LULC category across all clusters
    df(d, t) is the number of clusters that have a change in the LULC category

    The formula for our case is:
    tfidf = changed_area * idf = changed_area * log( 1+len(cluster_dfs) / ( 1 + df(d, t) ) ) + 1
    """

    def idf(df_d_t: int, num_of_clusters: int):
        # log( 1+len(cluster_dfs) / ( 1 + df(d, t) ) ) + 1
        return np.log(1 + num_of_clusters / (1 + df_d_t)) + 1

    def tfidf(changed_area: float, df_d_t: int, num_of_clusters: int):
        # changed_area
        return np.log(changed_area) * idf(df_d_t, num_of_clusters)

    # count the number of clusters that have a change in the LULC category
    from_to_count = defaultdict(int)

    for cluster_idx, cluster_df in cluster_LCs.items():
        total_changed_area = cluster_df["changed_area"].sum()
        for _, row in cluster_df.iterrows():
            # add the area change to the count
            from_to_count[(row["lulc_category_from"], row["lulc_category_to"])] += (
                row["changed_area"] / total_changed_area
            )  # normalize by the total changed area for the cluster

    # count the number of clusters
    num_of_clusters = len(cluster_LCs.keys())

    tfidf_scores = pd.DataFrame(
        columns=["lulc_category_from", "lulc_category_to", "tfidf_score"]
    )

    cluster_scores = {}

    for cluster_idx, cluster_df in cluster_LCs.items():
        cluster_specific_scores = pd.DataFrame(
            columns=["lulc_category_from", "lulc_category_to", "tfidf_score"]
        )
        for _, row in cluster_df.iterrows():
            tfidf_score = tfidf(
                changed_area=row["changed_area"],
                df_d_t=from_to_count[
                    (row["lulc_category_from"], row["lulc_category_to"])
                ],
                num_of_clusters=num_of_clusters,
            )

            # use concat to append the row to the dataframe
            tfidf_scores = pd.concat(
                [
                    tfidf_scores,
                    pd.DataFrame(
                        {
                            "lulc_category_from": [row["lulc_category_from"]],
                            "lulc_category_to": [row["lulc_category_to"]],
                            "tfidf_score": [tfidf_score],
                            "changed_area": [row["changed_area"]],
                        }
                    ),
                ]
            )

            cluster_specific_scores = pd.concat(
                [
                    cluster_specific_scores,
                    pd.DataFrame(
                        {
                            "lulc_category_from": [row["lulc_category_from"]],
                            "lulc_category_to": [row["lulc_category_to"]],
                            "tfidf_score": [tfidf_score],
                            "changed_area": [row["changed_area"]],
                        }
                    ),
                ]
            )

        cluster_scores[cluster_idx] = cluster_specific_scores

    return cluster_scores, tfidf_scores, from_to_count
