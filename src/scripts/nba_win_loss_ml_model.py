import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.inspection import permutation_importance
from utility.reference.sql import convert_sql_to_df
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    query = """
        WITH tradlogs as (
        SELECT 
            "TEAM_ID" as "team_id",
            "TEAM" as "team",
            "OPPONENT" as "opp",
            "HOME/AWAY" as "home_away",
            "GAME_ID" as "game_id",
            "GAME_DATE" as "date",
            "WL",
            "MIN" as "min",
            "FGM" as "fgm",
            "FGA" as "fga",
            "FG_PCT" as "fg_pct",
            "FG3M" as "fg3m",
            "FG3A" as "fg3a",
            "FG3_PCT" as "fg3_pct",
            "FTM" as "ftm",
            "FTA" as "fta",
            "FT_PCT" as "ft_pct",
            "OREB" as "oreb",
            "DREB" as "dreb",
            "REB" as "reb",
            "AST" as "ast",
            "STL" as "stl",
            "BLK" as "blk",
            "TOV" as "tov",
            "PF" as "pf",
            "PTS" as "pts",
            "PLUS_MINUS" as "plus_minus",
            "SEASON_YEAR" as "season"
        FROM nba_gamelogs.team_gamelogs
        WHERE "SEASON_YEAR" >= 2014
        --AND "TEAM_ABBREVIATION" = 'MIA'
    ),
    misc_metrics_filtered as (
        SELECT
            "game_id",
            "date",
            "away_team_id" as "team_id",
            "away_points_off_turnovers" as "pts_off_turnovers",
            "away_points_second_chance" as "second_chance_pts",
            "away_points_fast_break" as "fast_break_pts",
            "away_points_paint" as "paint_pts",
            "away_opp_points_off_turnovers" as "opp_pts_off_turnovers",
            "away_opp_points_second_chance" as "opp_second_chance_pts",
            "away_opp_points_fast_break" as "opp_fast_break_pts",
            "away_opp_points_paint" as "opp_paint_pts"
        FROM nba_gamelogs.team_misc_metrics
        WHERE "season" >= 2014

        UNION

        SELECT
            "game_id",
            "date",
            "home_team_id" as "team_id",
            "home_points_off_turnovers" as "pts_off_turnovers",
            "home_points_second_chance" as "second_chance_pts",
            "home_points_fast_break" as "fast_break_pts",
            "home_points_paint" as "paint_pts",
            "home_opp_points_off_turnovers" as "opp_pts_off_turnovers",
            "home_opp_points_second_chance" as "opp_second_chance_pts",
            "home_opp_points_fast_break" as "opp_fast_break_pts",
            "home_opp_points_paint" as "opp_paint_pts"
        FROM nba_gamelogs.team_misc_metrics
        WHERE "season" >= 2014
    ),
    advanced_metrics_filtered as (
        SELECT
            "game_id",
            "date",
            "away_team_id" as "team_id",
            "away_assist_to_turnover" as "assist_to_turnover",
            "away_pace" as "pace"
        FROM nba_gamelogs.team_advanced_metrics
        WHERE "season" >= 2014

        UNION

        SELECT
            "game_id",
            "date",
            "home_team_id" as "team_id",
            "home_assist_to_turnover" as "assist_to_turnover",
            "home_pace" as "pace"
        FROM nba_gamelogs.team_advanced_metrics
        WHERE "season" >= 2014		
    )
    SELECT
        "season",
        mm."date",
        "team",
        "opp",
        CASE WHEN "WL" = 'W' THEN 1 ELSE 0 END as "win?",
        "min",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts_off_turnovers",
        "second_chance_pts",
        "fast_break_pts",
        "paint_pts",
        "opp_pts_off_turnovers",
        "opp_second_chance_pts",
        "opp_fast_break_pts",
        "opp_paint_pts",
        "assist_to_turnover",
        "pace"
    FROM tradlogs as tl
    JOIN misc_metrics_filtered as mm
        ON tl."game_id" = mm."game_id"
        AND tl."team_id" = mm."team_id"
    JOIN advanced_metrics_filtered as am
        ON mm."game_id" = am."game_id"
        AND mm."team_id" = am."team_id"
    ORDER BY "date";
    """
    df = convert_sql_to_df(query=query)
    
    df['win'] = df['win?'].astype(int)
    

    feature_columns = [
        'min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
        'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb', 'ast', 'stl',
        'blk', 'tov', 'pf', 'pts_off_turnovers',
        'second_chance_pts', 'fast_break_pts', 'paint_pts',
        'opp_pts_off_turnovers', 'opp_second_chance_pts',
        'opp_fast_break_pts', 'opp_paint_pts', 'assist_to_turnover', 'pace'
    ]
    
    X = df[feature_columns].copy()
    y = df['win'].copy()
    
    X = X.fillna(X.mean())
    
    print(f"Dataset shape: {X.shape}")
    print(f"Win rate: {y.mean():.3f}")
    print(f"Features: {list(X.columns)}")
    
    return X, y

def run_feature_importance_analysis(X, y):
    """Run comprehensive feature importance analysis using multiple methods"""
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    results = {}
    feature_importance_results = {}
    
    print("Running machine learning models...")
    print("=" * 50)
    
    # Random Forest
    print("1. Random Forest Classifier")
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)
    rf_pred = rf_model.predict(X_test)
    rf_accuracy = accuracy_score(y_test, rf_pred)
    
    results['Random Forest'] = {
        'accuracy': rf_accuracy,
        'cv_score': cross_val_score(rf_model, X_train, y_train, cv=5).mean()
    }
    
    feature_importance_results['Random Forest'] = dict(zip(X.columns, rf_model.feature_importances_))
    print(f"   Accuracy: {rf_accuracy:.4f}")
    print(f"   CV Score: {results['Random Forest']['cv_score']:.4f}")
    
    # Gradient Boosting
    print("\n2. Gradient Boosting Classifier")
    gb_model = GradientBoostingClassifier(n_estimators=100, random_state=42)
    gb_model.fit(X_train, y_train)
    gb_pred = gb_model.predict(X_test)
    gb_accuracy = accuracy_score(y_test, gb_pred)
    
    results['Gradient Boosting'] = {
        'accuracy': gb_accuracy,
        'cv_score': cross_val_score(gb_model, X_train, y_train, cv=5).mean()
    }
    
    feature_importance_results['Gradient Boosting'] = dict(zip(X.columns, gb_model.feature_importances_))
    print(f"   Accuracy: {gb_accuracy:.4f}")
    print(f"   CV Score: {results['Gradient Boosting']['cv_score']:.4f}")
    
    # Logistic Regression
    print("\n3. Logistic Regression")
    lr_model = LogisticRegression(random_state=42, max_iter=1000)
    lr_model.fit(X_train_scaled, y_train)
    lr_pred = lr_model.predict(X_test_scaled)
    lr_accuracy = accuracy_score(y_test, lr_pred)
    
    results['Logistic Regression'] = {
        'accuracy': lr_accuracy,
        'cv_score': cross_val_score(lr_model, X_train_scaled, y_train, cv=5).mean()
    }
    
    # logistic regression
    feature_importance_results['Logistic Regression'] = dict(zip(X.columns, np.abs(lr_model.coef_[0])))
    print(f"   Accuracy: {lr_accuracy:.4f}")
    print(f"   CV Score: {results['Logistic Regression']['cv_score']:.4f}")
    
    print("\n4. Statistical Feature Selection")
    
    f_selector = SelectKBest(f_classif, k='all')
    f_selector.fit(X_train, y_train)
    f_scores = f_selector.scores_
    feature_importance_results['F-statistic'] = dict(zip(X.columns, f_scores))
    
    mi_scores = mutual_info_classif(X_train, y_train, random_state=42)
    feature_importance_results['Mutual Information'] = dict(zip(X.columns, mi_scores))
    
    print("\n5. Permutation Importance")
    perm_importance = permutation_importance(rf_model, X_test, y_test, n_repeats=10, random_state=42)
    feature_importance_results['Permutation Importance'] = dict(zip(X.columns, perm_importance.importances_mean))
    
    return results, feature_importance_results

def create_feature_ranking_summary(feature_importance_results):
    
    importance_df = pd.DataFrame(feature_importance_results)
    
    for method in importance_df.columns:
        importance_df[method] = (importance_df[method] - importance_df[method].min()) / \
                               (importance_df[method].max() - importance_df[method].min())
    
    importance_df['Average'] = importance_df.mean(axis=1)
    
    ranking_df = importance_df.rank(ascending=False, method='min')
    
    importance_df = importance_df.sort_values('Average', ascending=False)
    ranking_df = ranking_df.loc[importance_df.index]
    
    return importance_df, ranking_df

def plot_feature_importance(importance_df, top_n=15):
    
    plt.figure(figsize=(12, 8))
    top_features = importance_df.head(top_n)
    
    plt.subplot(2, 2, 1)
    plt.barh(range(len(top_features)), top_features['Average'])
    plt.yticks(range(len(top_features)), top_features.index)
    plt.xlabel('Average Normalized Importance')
    plt.title(f'Top {top_n} Features by Average Importance')
    plt.gca().invert_yaxis()
    
    plt.subplot(2, 2, 2)
    methods_to_plot = ['Random Forest', 'Gradient Boosting', 'Logistic Regression', 'Permutation Importance']
    heatmap_data = top_features[methods_to_plot].T
    sns.heatmap(heatmap_data, annot=False, cmap='YlOrRd', cbar=True)
    plt.title('Feature Importance Heatmap')
    plt.xlabel('Features')
    plt.ylabel('Methods')
    
    plt.subplot(2, 2, 3)
    rf_importance = importance_df['Random Forest'].head(top_n)
    plt.barh(range(len(rf_importance)), rf_importance)
    plt.yticks(range(len(rf_importance)), rf_importance.index)
    plt.xlabel('Random Forest Importance')
    plt.title('Random Forest Feature Importance')
    plt.gca().invert_yaxis()
    
    plt.subplot(2, 2, 4)
    model_names = ['Random Forest', 'Gradient Boosting', 'Logistic Regression']
    plt.title('Model Performance Comparison')
    plt.xlabel('Models')
    plt.ylabel('Accuracy')
    
    plt.tight_layout()
    plt.show()

def print_detailed_results(results, importance_df, ranking_df):
    """Print detailed analysis results"""
    
    print("\n" + "="*60)
    print("DETAILED FEATURE IMPORTANCE ANALYSIS RESULTS")
    print("="*60)
    
    print("\n1. MODEL PERFORMANCE SUMMARY")
    print("-" * 40)
    performance_df = pd.DataFrame(results).T
    print(performance_df.round(4))
    
    print("\n2. TOP 10 MOST IMPORTANT FEATURES (by average ranking)")
    print("-" * 40)
    top_10 = importance_df.head(10)
    for i, (feature, row) in enumerate(top_10.iterrows(), 1):
        print(f"{i:2d}. {feature:20s} (Avg Score: {row['Average']:.3f})")
    
    print("\n3. FEATURE RANKINGS BY METHOD (Top 10)")
    print("-" * 40)
    top_10_ranking = ranking_df.head(10)
    methods = ['Random Forest', 'Gradient Boosting', 'Logistic Regression', 'Permutation Importance']
    
    print(f"{'Feature':<20} {'RF':<4} {'GB':<4} {'LR':<4} {'PI':<4} {'Avg':<4}")
    print("-" * 45)
    for feature, row in top_10_ranking.iterrows():
        rf_rank = int(row['Random Forest'])
        gb_rank = int(row['Gradient Boosting'])
        lr_rank = int(row['Logistic Regression'])
        pi_rank = int(row['Permutation Importance'])
        avg_rank = int(row['Average'])
        print(f"{feature:<20} {rf_rank:<4} {gb_rank:<4} {lr_rank:<4} {pi_rank:<4} {avg_rank:<4}")
    
    print("\n4. KEY INSIGHTS")
    print("-" * 40)
    
    ranking_std = ranking_df[methods].std(axis=1)
    most_consistent = ranking_std.head(5)
    
    print("Most consistent features across methods:")
    for i, (feature, std_val) in enumerate(most_consistent.items(), 1):
        avg_rank = int(ranking_df.loc[feature, 'Average'])
        print(f"  {i}. {feature} (Avg rank: {avg_rank}, Std: {std_val:.2f})")
    
    most_controversial = ranking_std.tail(5)
    print("\nMost controversial features (varied rankings):")
    for i, (feature, std_val) in enumerate(most_controversial.items(), 1):
        avg_rank = int(ranking_df.loc[feature, 'Average'])
        print(f"  {i}. {feature} (Avg rank: {avg_rank}, Std: {std_val:.2f})")

def main():
    print("NBA Win/Loss Prediction - Feature Importance Analysis")
    print("=" * 60)
    
    X, y = load_and_prepare_data('team_logs_agg_test.csv')
   
    results, feature_importance_results = run_feature_importance_analysis(X, y)
    
    importance_df, ranking_df = create_feature_ranking_summary(feature_importance_results)
    
    print_detailed_results(results, importance_df, ranking_df)
    
    print("\n5. GENERATING VISUALIZATIONS...")
    print("-" * 40)
    plot_feature_importance(importance_df)
    
    importance_df.to_csv('feature_importance_results.csv')
    ranking_df.to_csv('feature_ranking_results.csv')
    
    print("\nAnalysis complete!")
    print("Results saved to:")
    print("- feature_importance_results.csv")
    print("- feature_ranking_results.csv")

if __name__ == "__main__":
    main()