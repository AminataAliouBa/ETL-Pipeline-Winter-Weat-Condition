## Description
Pipeline Python pour récupérer, traiter et visualiser les données historiques de (Winter Wheat pour South Dakota).


## Arborences
- src/ (Pipeline, parameters, requirements, logger)
- logs/ (info, metrics, errors)
- data/ (fichier csv)
- Analytics/ (dashboard) 

## Run
- pip install -r requirements.txt
- python3 Pipeline.py --api_key=my_key (key_api gratuit via [ici](https://quickstats.nass.usda.gov/api))

## Documentation

- Pipeline class
    - extract_data(url, params)
        - Télécharge les données depuis l'API QuickStats.
    - transform_data()
        - Calcule l’index synthétique du blé d’hiver.
    - load_data()
        - Sauvegarde des données propres dans data/
    - visualize_data()
        - Génère les visualisations (courbes, heatmap, histogramme, boxplot) et log les métriques : graphes dans (analytics/) et logs dans logs/
- My_logger class
    - log_quick_stats_ppl()
        - Log tous les infos, erreurs...
- params json
    - Fichier de paramètres (adaptable) de téléchargement des données le module requests de Python 