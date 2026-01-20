"""Contrôles de qualité des données"""

import pandas as pd
from typing import Dict, List, Any
from datetime import datetime


def validate_data_quality(
    df: pd.DataFrame,
    rules: Dict[str, Any],
    dataset_name: str = "dataset"
) -> Dict[str, Any]:
    """
    Valide la qualité des données selon des règles.
    
    Args:
        df: DataFrame à valider
        rules: Dict de règles de qualité
        dataset_name: Nom du dataset pour le rapport
    
    Returns:
        Dict avec résultats de validation
    """
    report = {
        "dataset": dataset_name,
        "total_rows": len(df),
        "checks": {}
    }
    
    # Complétude
    if "completeness" in rules:
        for col, threshold in rules["completeness"].items():
            if col in df.columns:
                completeness = (1 - df[col].isna().sum() / len(df)) * 100
                report["checks"][f"{col}_completeness"] = {
                    "value": completeness,
                    "threshold": threshold * 100,
                    "status": "✅" if completeness >= threshold * 100 else "❌"
                }
    
    # Unicité
    if "uniqueness" in rules:
        for col, threshold in rules["uniqueness"].items():
            if col in df.columns:
                uniqueness = (df[col].nunique() / len(df)) * 100
                report["checks"][f"{col}_uniqueness"] = {
                    "value": uniqueness,
                    "threshold": threshold * 100,
                    "status": "✅" if uniqueness >= threshold * 100 else "❌"
                }
    
    # Validité
    if "validity" in rules:
        for col, rule in rules["validity"].items():
            if col in df.columns:
                if rule == "not_future":
                    future_count = (pd.to_datetime(df[col], errors='coerce') > pd.Timestamp.now()).sum()
                    validity = (1 - future_count / len(df)) * 100
                    report["checks"][f"{col}_validity"] = {
                        "value": validity,
                        "status": "✅" if validity == 100 else "❌",
                        "details": f"{future_count} dates futures"
                    }
                elif isinstance(rule, dict):
                    if "min" in rule and "max" in rule:
                        invalid = ((df[col] < rule["min"]) | (df[col] > rule["max"])).sum()
                        validity = (1 - invalid / len(df)) * 100
                        report["checks"][f"{col}_validity"] = {
                            "value": validity,
                            "status": "✅" if validity == 100 else "❌",
                            "details": f"{invalid} valeurs hors plage [{rule['min']}, {rule['max']}]"
                        }
    
    return report


def detect_anomalies(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    """
    Détecte les valeurs aberrantes avec la méthode IQR.
    
    Args:
        df: DataFrame à analyser
        numeric_columns: Colonnes numériques à vérifier
    
    Returns:
        DataFrame avec colonnes d'anomalies ajoutées
    """
    df_anomalies = df.copy()
    
    for col in numeric_columns:
        if col in df_anomalies.columns:
            Q1 = df_anomalies[col].quantile(0.25)
            Q3 = df_anomalies[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            df_anomalies[f"{col}_is_anomaly"] = (
                (df_anomalies[col] < lower_bound) | 
                (df_anomalies[col] > upper_bound)
            )
    
    return df_anomalies


def generate_quality_report(reports: List[Dict[str, Any]]) -> None:
    """
    Génère un rapport de qualité formaté.
    
    Args:
        reports: Liste de rapports de validation
    """
    print("\n" + "="*60)
    print("📋 RAPPORT DE QUALITÉ DES DONNÉES")
    print("="*60)
    
    for report in reports:
        print(f"\n📊 Dataset: {report['dataset']}")
        print(f"   Total lignes: {report['total_rows']}")
        print("\n   Contrôles:")
        
        for check_name, check_result in report['checks'].items():
            status = check_result['status']
            value = check_result.get('value', 0)
            threshold = check_result.get('threshold', 0)
            details = check_result.get('details', '')
            
            print(f"   {status} {check_name}: {value:.2f}% (seuil: {threshold:.2f}%)")
            if details:
                print(f"      → {details}")
    
    print("\n" + "="*60 + "\n")

