from collections import Counter
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from config import CLUSTER_COLORS, COUNTRY_COLORS, PLOTS_DIR
from src.plotting.sankey import plot_sankey


def projection_and_clusters(
    X_transformed: np.ndarray, projection_colors: List[str], cluster_colors: List[int]
) -> None:
    plt.style.use("fivethirtyeight")

    # inverse COUNTRY_COLORS to get country names
    country_colors = {v: k for k, v in COUNTRY_COLORS.items()}

    projection_colors_set = set(projection_colors)

    fig, axes = plt.subplots(1, 2, figsize=(16, 9))

    for color in projection_colors_set:
        country = country_colors[color]
        indices = [i for i, x in enumerate(projection_colors) if x == color]

        axes[0].scatter(
            X_transformed[indices, 0],
            X_transformed[indices, 1],
            c=color,
            label=country,
            s=5,
        )

    axes[0].set_title("Projection of Points")
    axes[0].grid(True)
    handles, labels = axes[0].get_legend_handles_labels()
    axes[0].legend(handles, labels, loc="upper right", markerscale=5)
    plt.tight_layout()

    # Plot clusters. The label is the cluster_colors.
    for color in set(cluster_colors):
        indices = [i for i, x in enumerate(cluster_colors) if x == color]
        axes[1].scatter(
            X_transformed[indices, 0],
            X_transformed[indices, 1],
            label=f"Cluster {color}",
            c=CLUSTER_COLORS[str(color)],
            s=5,
        )

    axes[1].set_title("Cluster Projections")
    axes[1].grid(True)
    handles, labels = axes[1].get_legend_handles_labels()
    axes[1].legend(handles, labels, loc="upper right", markerscale=5)
    plt.tight_layout()
    outpath = PLOTS_DIR / "embeddings/projection_and_clusters.png"
    print("Saving plot to", outpath)
    plt.savefig(outpath)
    plt.show()

    return None



def distribution_of_countries_per_cluster(clustered_label, index_2_chip):
    """
    clustered_label: clustered.labels_ gives the cluster number for chip
    index_2_chip: gives the country name for the index
    """

    cluster_count = {}

    for cluster_name in set(clustered_label):
        cluster_indices = [i for i, x in enumerate(clustered_label) if x == cluster_name]
        cluster_countries = [index_2_chip[i][0] for i in cluster_indices]
        country_counts = Counter(cluster_countries)
        cluster_count[cluster_name] = country_counts

    fig, ax = plt.subplots(figsize=(10, 5))

    legend_handled = set()  # Set to track which countries have been added to the legend
    for cluster, country_count_dict in cluster_count.items():
        bottom_height = 0  # Initialize the bottom at 0 for each cluster
        for country, count in sorted(country_count_dict.items(), key=lambda x: x[0]):  # Sort countries alphabetically
            if country not in legend_handled:
                ax.bar(cluster, count, bottom=bottom_height, label=country, color=COUNTRY_COLORS[country])
                legend_handled.add(country)  # Mark this country as added to the legend
            else:
                ax.bar(cluster, count, bottom=bottom_height, color=COUNTRY_COLORS[country])
            bottom_height += count  # Increase the bottom height by the count of the current country

    # Add legends, labels, and adjust ticks for clarity
    ax.set_xticks(list(cluster_count.keys()))
    ax.set_xticklabels(cluster_count.keys())
    plt.legend(title="Country", bbox_to_anchor=(0.5, -0.05), loc='upper center', ncol=4)  # Adjust the position of the legend

    plt.tight_layout()
    outpath = PLOTS_DIR / "embeddings/countries_per_cluster.png"
    print("Saving plot to", outpath)
    plt.savefig(outpath)
    plt.show()

    return None


def sankey_for_cluster_LCs(
    cluster_LCs: Dict[int, pd.DataFrame], titles: List[str] = None, tfidf_edition=False
) -> None:
    
    plt.style.use("default")

    if titles is None:
        titles = [f"Cluster {cluster_id}" for cluster_id in cluster_LCs.keys()]

    for idx, (cluster_id, cluster_LC) in enumerate(cluster_LCs.items()):
        sort_it = cluster_LC.sort_values(by="lulc_category_from", ascending=True)
        if tfidf_edition:
            outpath = PLOTS_DIR / f"embeddings/tfidf_cluster_{cluster_id}.png"
        else:
            outpath = PLOTS_DIR / f"embeddings/cluster_{cluster_id}.png"
        plot_sankey(
            sort_it,
            titles[idx],
            outpath=outpath,
        )


        # change outpath to csv
        outpath = outpath.with_suffix(".csv")
        cluster_LC.to_csv(outpath)
