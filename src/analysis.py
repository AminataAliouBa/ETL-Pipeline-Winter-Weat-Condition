# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from scipy.stats import gaussian_kde
import numpy as np
from pathlib import Path

# Setup paths
BASE_DIR = Path(__file__).parent.parent
ANALYTICS_DIR = BASE_DIR / "analytics"

# Stress threshold constant
STRESS_THRESHOLD = 300  # Index below which conditions are considered stressed


def plot_strong_analysis(ts_simple):
    """
    Generate deep analysis plots with risk indicators.
    
    Creates 6 analytical plots:
        1. Mean vs Annual Minimum scatter
        2. Historical stress probability
        3. Interannual variability evolution
        4. Distribution of worst annual conditions
        5. Seasonal risk calendar
        6. Cumulative stress index by crop year
        
    Args:
        ts_simple: Simple time series (date indexed, values are condition indices)
    """
    plt.style.use('_mpl-gallery')
    
    # Récupération des données existantes
    df = ts_simple.reset_index()
    df.columns = ['date', 'index']

    # Ajouter crop_year depuis ts_matrix
    df["crop_year"] = df["date"].dt.year
    df.loc[df["date"].dt.month >= 10, "crop_year"] += 1

    # Calculs préliminaires
    df["week"] = df["date"].dt.isocalendar().week

    annual_stats = df.groupby("crop_year")["index"].agg(['mean', 'min'])
    annual_min = annual_stats['min']
    risk_flag = annual_min < STRESS_THRESHOLD
    risk_summary = risk_flag.value_counts(normalize=True) * 100
    risk_summary = risk_summary.rename({True: "Années à risque", False: "Années normales"})
    annual_volatility = df.groupby("crop_year")["index"].std()
    stress_prob = (df["index"] < STRESS_THRESHOLD).groupby(df["week"]).mean()
    stress_intensity = df[df["index"] < STRESS_THRESHOLD].assign(
        stress_gap=STRESS_THRESHOLD - df["index"]
    ).groupby("crop_year")["stress_gap"].sum()

    # Création de la figure
    fig, axes = plt.subplots(6, 1, figsize=(20, 30))

    # Graph 1 : Moyenne vs Minimum annuel
    axes[0].scatter(annual_stats['mean'], annual_stats['min'])
    axes[0].axhline(STRESS_THRESHOLD, color='r', linestyle='--', label=f'Threshold ({STRESS_THRESHOLD})')
    axes[0].set_xlabel("Moyenne annuelle de l'indice")
    axes[0].set_ylabel("Minimum annuel de l'indice")
    axes[0].set_title("Moyenne vs Minimum annuel — Risque asymétrique")
    axes[0].legend()

    # Graph 2 : Probabilité historique
    risk_summary.plot(kind="bar", ax=axes[1])
    axes[1].set_ylabel("Pourcentage des campagnes (%)")
    axes[1].set_title("Probabilité historique d'années à stress sévère")
    axes[1].tick_params(axis='x', rotation=0)
    axes[1].legend()

    # Graph 3 : Évolution de la variabilité
    annual_volatility.plot(ax=axes[2])
    axes[2].set_ylabel("Écart-type annuel de l'indice")
    axes[2].set_xlabel("Année de campagne")
    axes[2].set_title("Évolution de la variabilité interannuelle")
    axes[2].legend()

    # Graph 4 : Distribution des minimums
    axes[3].hist(annual_min, bins=20, edgecolor='black')
    axes[3].axvline(STRESS_THRESHOLD, color='r', linestyle='--', label=f'Threshold ({STRESS_THRESHOLD})')
    axes[3].set_xlabel("Minimum hebdomadaire annuel")
    axes[3].set_ylabel("Nombre de campagnes")
    axes[3].set_title("Distribution des pires conditions annuelles")
    axes[3].legend()

    # Graph 5 : Calendrier saisonnier
    stress_prob.plot(ax=axes[4])
    axes[4].set_ylabel("Probabilité de stress (< seuil)")
    axes[4].set_xlabel("Semaine de l'année")
    axes[4].set_title("Calendrier saisonnier du risque climatique")
    axes[4].legend()

    # Graph 6 : Stress cumulé
    stress_intensity.plot(kind="bar", ax=axes[5])
    axes[5].set_ylabel("Stress cumulé")
    axes[5].set_title("Indice de stress cumulé par campagne")
    axes[5].tick_params(axis='x', rotation=45)
    axes[5].legend()

    plt.tight_layout()
    fig.suptitle("Monitoring Winter Wheat Conditions — Deep Analysis", fontsize=30, y=1.03)
    
    output_path = ANALYTICS_DIR / f"KPI_Winter-Wheat-Condition-South-Dakota_{datetime.today().strftime('%d%m%Y')}.png"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def plot_describ_sats(ts, mat):
    """
    Generate descriptive statistics dashboard.
    
    Creates 6 visualization plots:
        1. Distribution histogram with KDE
        2. Global boxplot
        3. Boxplot by year
        4. Annual evolution with rolling stats
        5. Monthly average evolution
        6. Heatmap of week × year matrix
        
    Args:
        ts: Simple time series (date indexed pandas Series)
        mat: Matrix format (week × year DataFrame)
    """
    plt.style.use('_mpl-gallery')
    fig, axes = plt.subplots(6, 1, figsize=(20, 30))
        
    # Distribution Histogramme
    ax = axes[0]
    ax.hist(ts, bins=20, edgecolor="black", density=True)
    kde = gaussian_kde(ts)
    x_range = np.linspace(ts.min(), ts.max(), 500)
    ax.plot(x_range, kde(x_range), color="red", linewidth=2, label="Smooth Density")
    ax.axvline(ts.mean(), color="black", linestyle="--", label="MEAN")
    ax.axvline(ts.median(), color="orange", linestyle=":", label="MEDIAN")
    ax.set_title("Condition Index Distribution")
    ax.set_xlabel("Index (100 = VERY POOR, 500 = EXCELLENT)")
    ax.set_ylabel("FREQUENCY")
    ax.grid(alpha=0.3)
    ax.legend()

    # boxplot globale
    ax = axes[1]
    ax.boxplot(ts.values, vert=True, showmeans=True, meanline=True, notch=True)
    ax.set_title("Distribution of Winter Wheat Condition Index")
    ax.set_ylabel("Index value")

    # Boxplot par année
    ax = axes[2]
    df_ts = ts.to_frame(name="Index")
    df_ts["Year"] = df_ts.index.year
    years = sorted(df_ts["Year"].unique())
    data = [df_ts[df_ts["Year"] == y]["Index"].values for y in years]
    ax.boxplot(data, tick_labels=years, showfliers=True)
    ax.axhline(ts.median(), color="red", linestyle="--", label="MEDIAN")
    ax.axhline(ts.mean(), color="green", linestyle="--", label="MEAN")
    ax.set_title("Index Distribution By Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Index")
    ax.legend()

    # Évolution annuelle
    ax = axes[3] 
    ts_year = ts.resample('YE').mean()  # moyenne annuelle
    rolling_min_10y = ts_year.rolling(10).min()
    rolling_mean = ts_year.rolling(10).mean()
    ax.plot(ts_year, 'o-', label="YEAR")
    ax.plot(rolling_mean, 'o--', label="Moyenne 10 ans", color='green')
    ax.plot(rolling_min_10y, 'x--', label="Min 10 ans", color='red')
    ax.set_title("Evolution By Year")
    ax.legend()

    # Évolution mensuelle
    ax = axes[4]
    ts_month = ts.groupby(ts.index.month).mean()  # moyenne par mois (1 à 12)
    ts_month.plot(ax=ax, marker='o', color='orange', linewidth=2)
    ax.set_title("Winter Wheat Condition Evolution Per Mean Monthly")
    ax.set_xlabel("Month")
    ax.set_ylabel("Index")
    ax.grid(alpha=0.3)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels([
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ])

    # Saison par semaine et année
    ax = axes[5]
    masked_mat = np.ma.masked_invalid(mat.values)
    cmap = plt.cm.RdYlGn
    cmap.set_bad(color="white")  # (dormance)
    im = ax.imshow(masked_mat, aspect="auto", cmap=cmap, vmin=100, vmax=500)
    ax.set_yticks(range(len(mat.index)))
    ax.set_yticklabels(mat.index)
    ax.set_xticks(range(len(mat.columns)))
    ax.set_xticklabels(mat.columns, rotation=45)
    ax.set_title("Winter Wheat Condition index — Temporal structure (weeks × years) [Crop calendar format]")
    ax.set_xlabel("Year")
    ax.set_ylabel("Week")
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Index (100 = VERY POOR, 500 = EXCELLENT)")

    plt.tight_layout()
    fig.suptitle("Monitoring Winter Wheat Conditions — Weekly & Annual Trends", fontsize=30, y=1.03)
    
    output_path = ANALYTICS_DIR / f"dashboard_Winter-Wheat-Condition-South-Dakota_{datetime.today().strftime('%d%m%Y')}.png"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
