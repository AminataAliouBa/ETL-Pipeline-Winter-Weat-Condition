# -*- coding: utf-8 -*-
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
from pathlib import Path
import argparse
import json
import csv
import My_logger
import analysis

# Setup paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
ANALYTICS_DIR = BASE_DIR / "analytics"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
ANALYTICS_DIR.mkdir(exist_ok=True)

# Initialisation des fichiers logs
logger = My_logger.log(
    "quick_stats_pipeline_logger",
    str(LOGS_DIR / "wwcsd_error.log"),
    str(LOGS_DIR / "wwcsd_metrics.log")
)

class Pipeline:
    """
    ETL Pipeline for Winter Wheat Condition data from USDA QuickStats API.
    
    This pipeline extracts wheat condition data, transforms it into time series,
    and generates analytical visualizations.
    """
    
    def __init__(self):
        """
        Initialize the Pipeline with empty dataframes.
        
        Attributes:
            df: Raw and cleaned data
            ts_simple: Simple time series (date => index value)
            ts_matrix: Matrix format (week x year => index value)
        """
        self.df = pd.DataFrame()  # Données brutes, nettoyées
        self.ts_simple = pd.DataFrame()  # Série temporelle simple date => valeur*index
        self.ts_matrix = pd.DataFrame()  # Matrice semaine, année => valeur*index


    def extract_data(self, url, params, end_year, start_year):
        """
        Extract data from USDA QuickStats API with pagination by year.
        
        Args:
            url: API endpoint URL
            params: Query parameters dictionary
            end_year: Most recent year to fetch
            start_year: Oldest year to fetch
            
        Raises:
            RuntimeError: If no data is returned from API
        """
        logger.info("Extracting Data...") 
        all_dfs = []
        last_year = end_year
        while (last_year >= start_year):
            params = {
                **params,
                "year": last_year
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                df_year = pd.read_csv(StringIO(response.text))
                if not df_year.empty:
                    all_dfs.append(df_year)
                else:
                    break
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error, for year {last_year} : {e.response.status_code}, {e.response.reason}")

            last_year -= 1
            
        if not all_dfs:
            logger.error("No Data Found From API")
            raise RuntimeError("No data found from API")
        else:
            self.df = pd.concat(all_dfs, ignore_index=True)
            raw_data_path = DATA_DIR / f"cww-sd_raw_data_{datetime.today().strftime('%d%m%Y')}.csv"
            self.df.to_csv(raw_data_path, sep=',', quoting=csv.QUOTE_ALL, quotechar='"', index=False)
        

    def transform_data(self):
        """
        Transform raw data into cleaned format and compute time series.
        
        Steps:
            1. Remove null columns and duplicate rows
            2. Filter valid weeks (sum = 100%)
            3. Compute condition index from 5 categories
            4. Create simple time series
            5. Create crop-year matrix format
        """
        logger.info("Transforming Data...")  
        logger.info("Preprocessing...") 
        logger.info("Avant Nettoyage :")
        logger.info(f"Nombre de lignes : {len(self.df)}")
        logger.info(f"Nombre de colonnes : {len(self.df.columns)}")
        
        # Colonnes entièrement nulles
        empty_cols = self.df.columns[self.df.isna().all()]
        logger.info(f"Colonnes complètement nulles : {list(empty_cols)}")
        
        # Nombre de doublons
        nb_doublons = self.df.duplicated().sum()
        logger.info(f"Nombre de doublons : {nb_doublons}")

        logger.info("Cleaning...")
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.dropna(axis=0, how='all')
        self.df = self.df.drop_duplicates()
        self.df = self.df[['load_time', 'week_ending', 'unit_desc', 'Value']]  # garder les lignes dont on a besoin
        
        # supprimer les semaines sans valeurs
        valid_weeks = (self.df.groupby("week_ending")["Value"].sum().round(1))
        self.df = self.df[self.df["week_ending"].isin(valid_weeks[valid_weeks == 100].index)]
        
        logger.info("Après Nettoyage :")
        logger.info(f"Nombre de lignes : {len(self.df)}")
        logger.info(f"Nombre de colonnes : {len(self.df.columns)}")

        logger.info("Computing series...")
        # Sorting the information by publication date
        self.df['load_time'] = pd.to_datetime(self.df['load_time'], errors='coerce')
        self.df = self.df.sort_values(by='load_time')
        										
        # Computing an Index, to avoid working with 5 categories 
        # Index = 1 x Very Poor + 2 x Poor + 3 x Fair + 4 x Good + 5 x Excellent
        mapping = {
            'PCT VERY POOR': 1,
            'PCT POOR': 2,
            'PCT FAIR': 3,
            'PCT GOOD': 4,
            'PCT EXCELLENT': 5
        }
        self.df["index"] = self.df["unit_desc"].map(mapping)
        self.df["index_value"] = self.df["index"] * self.df["Value"]
        									
        # Organizing this index as a simple time series (i.e. a T x 1 vector)	
        self.ts_simple = self.df.groupby('week_ending')['index_value'].sum() 
        self.ts_simple.index = pd.to_datetime(self.ts_simple.index)

        # and a by-crop-year time series (i.e. a t x n matrix)
        # Recalcule de l'année et de la semaine pour sécurisation
        weekly_df = self.ts_simple.reset_index(name="index")
        
        # Crop year (winter wheat)
        weekly_df["crop_year"] = weekly_df["week_ending"].dt.year
        weekly_df.loc[weekly_df["week_ending"].dt.month >= 10, "crop_year"] += 1
        
        # Sequential week number within crop year
        weekly_df["week_in_crop"] = (weekly_df.groupby("crop_year").cumcount() + 1)
        self.ts_matrix = weekly_df.pivot(index="week_in_crop", columns="crop_year", values="index")


    def load_data(self):
        """
        Save cleaned data to CSV file.
        """
        logger.info("Loading Cleaned Data...")
        cleaned_data_path = DATA_DIR / f"cww-sd_cleaned_data_{datetime.today().strftime('%d%m%Y')}.csv"
        self.df.to_csv(cleaned_data_path, sep=',', quoting=csv.QUOTE_ALL, quotechar='"', index=False)


    def visualize_data(self):
        """
        Generate analytical visualizations and log key metrics.
        
        Creates two sets of plots:
            1. Descriptive statistics dashboard
            2. Deep analysis with risk indicators
        """
        logger.info("Analizing Data...")

        analysis.plot_describ_sats(self.ts_simple, self.ts_matrix)
        analysis.plot_strong_analysis(self.ts_simple)

        # Statistiques clés
        logger.info("----- METRICS -----")
        mean_val = self.ts_simple.mean()
        last_val = self.ts_simple.iloc[-1]
        logger.info(f"MEAN : {mean_val:.1f}")
        logger.info(f"Standart-deviation : {self.ts_simple.std():.1f}")
        logger.info(f"Min : {self.ts_simple.min():.1f}")
        logger.info(f"Max : {self.ts_simple.max():.1f}")
        logger.info(f"Last Value : {last_val:.1f}")
        logger.info(f"Relative deviation : Last_value vs Mean : {(last_val/mean_val - 1)*100:.1f}%")
        logger.info(f"Last value percentile: {(self.ts_simple < last_val).mean() * 100:.0f}%")        

    
    def run(self, url, params, end_year, start_year):
        """
        Execute the complete ETL and analytics pipeline.
        
        Args:
            url: API endpoint URL
            params: Query parameters dictionary
            end_year: Most recent year to fetch
            start_year: Oldest year to fetch
        """
        logger.info("Running ETL & Analytics Pipeline")
        self.extract_data(url, params, end_year, start_year)
        self.transform_data()
        self.load_data()
        self.visualize_data()
        logger.info("Pipeline Successfully Done!!!")


if __name__ == '__main__':

    # Récupérer la clé API en argument pour le téléchargement des données
    logger.info("Loading config parameters...")
    parser = argparse.ArgumentParser()
    parser.add_argument("--api_key", required=True, help="Your API key")
    args = parser.parse_args()
    api_key = args.api_key
    
    # Charger le fichier de paramètres
    config_path = BASE_DIR / "params.json"
    with open(config_path) as f:
        configs = json.load(f)
    
    # Récupérer les paramètres
    url = configs["qs_url"]
    params = configs["qswwc_params"]
    end_year = configs["end_year_for_pagination"]
    start_year = configs["start_year_for_pagination"]
    params["key"] = api_key
    
    # Initialisation de lancement du pipeline
    my_pipiline = Pipeline()
    my_pipiline.run(url, params, end_year, start_year)
