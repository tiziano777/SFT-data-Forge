import pandas as pd
import numpy as np
from typing import Dict, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Tipi di metriche per categorizzazione"""
    STRUCTURAL = "structural"
    CONTENT = "content"
    QUALITY = "quality"
    TECHNICAL = "technical"
    SIMPLE_DIFF = "simple_diff"

@dataclass
class ComparisonResult:
    """Risultato strutturato di una metrica comparativa"""
    metric_name: str
    metric_type: MetricType
    value: float
    interpretation: str
    components: Dict[str, float]
    significance: Optional[float] = None  # p-value se applicabile
    
class DistributionComparator:
    """
    Classe per confrontare due distribution basandosi sulle statistiche aggregate.
    Ogni metrica è implementata come funzione modulare per facilitare testing.
    """
    
    def __init__(self, 
                 stats_a: pd.DataFrame,
                 stats_b: pd.DataFrame,
                 feature_weights: Optional[Dict[str, float]] = None):
        """
        Inizializza il comparatore con due distribution di statistiche.
        
        Args:
            stats_a: DataFrame con statistiche del distribution A
            stats_b: DataFrame con statistiche del distribution B
            feature_weights: Pesi opzionali per le feature (default: tutti 1.0)
        """
        self.stats_a = stats_a.copy()
        self.stats_b = stats_b.copy()
        self.feature_weights = feature_weights or {}
        
        # Verifica consistenza colonne
        self._validate_distributions()
        
        # Pre-calcola aggregati per efficienza
        self.agg_a = self._compute_aggregates(self.stats_a)
        self.agg_b = self._compute_aggregates(self.stats_b)
        
        logger.info(f"Distribution A: {len(stats_a)} documenti, {len(stats_a.columns)} feature")
        logger.info(f"Distribution B: {len(stats_b)} documenti, {len(stats_b.columns)} feature")
    
    def _validate_distributions(self):
        """Verifica che i distribution abbiano le stesse colonne"""
        cols_a = set(self.stats_a.columns)
        cols_b = set(self.stats_b.columns)
        
        if cols_a != cols_b:
            missing_in_b = cols_a - cols_b
            missing_in_a = cols_b - cols_a
            
            if missing_in_b:
                logger.warning(f"Colonne mancanti in distribution B: {missing_in_b}")
            if missing_in_a:
                logger.warning(f"Colonne mancanti in distribution A: {missing_in_a}")
            
            # Manteniamo solo le colonne comuni
            common_cols = cols_a.intersection(cols_b)
            self.stats_a = self.stats_a[list(common_cols)]
            self.stats_b = self.stats_b[list(common_cols)]
            
            logger.info(f"Usando {len(common_cols)} colonne comuni")
    
    def _compute_aggregates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calcola aggregati base per un distribution"""
        return {
            'means': df.mean(numeric_only=True).to_dict(),
            'stds': df.std(numeric_only=True).to_dict(),
            'counts': df.count().to_dict(),
            'presence_rates': self._compute_presence_rates(df),
            'medians': df.median(numeric_only=True).to_dict()
        }
    
    def _compute_presence_rates(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calcola percentuali di presenza per feature binarie"""
        rates = {}
        for col in df.columns:
            if df[col].dtype in ['bool', 'int'] and df[col].max() <= 1:
                rates[col] = df[col].mean()
        return rates
    
    def _get_feature_weight(self, feature_name: str) -> float:
        """Ottiene il peso di una feature"""
        return self.feature_weights.get(feature_name, 1.0)
    
    # ==================== METRICHE PRINCIPALI ====================

    def calculate_technical_depth_ratio(self) -> ComparisonResult:
        """
        Technical Depth Ratio (TDR)
        Rapporto della profondità tecnica tra distribution.
        """
        try:
            # Code density
            code_count_a = self.agg_a['means'].get('_code_count', 0)
            word_count_a = self.agg_a['means'].get('_word_count', 1)
            code_count_b = self.agg_b['means'].get('_code_count', 0)
            word_count_b = self.agg_b['means'].get('_word_count', 1)
            
            code_density_a = code_count_a / word_count_a if word_count_a > 0 else 0
            code_density_b = code_count_b / word_count_b if word_count_b > 0 else 0
            
            # JSON presence
            json_rate_a = self.agg_a['presence_rates'].get('_assistant_has_json', 0)
            json_rate_b = self.agg_b['presence_rates'].get('_assistant_has_json', 0)
            
            # Curly brackets normalized
            curly_a = self.agg_a['means'].get('_curly_brackets_count', 0)
            chars_a = self.agg_a['means'].get('_char_count', 1)
            curly_b = self.agg_b['means'].get('_curly_brackets_count', 0)
            chars_b = self.agg_b['means'].get('_char_count', 1)
            
            curly_norm_a = curly_a / chars_a if chars_a > 0 else 0
            curly_norm_b = curly_b / chars_b if chars_b > 0 else 0
            
            # Calcola TDR
            numerator = code_density_b + json_rate_b + curly_norm_b
            denominator = code_density_a + json_rate_a + curly_norm_a
            
            tdr_value = numerator / denominator if denominator > 0 else 1.0
            
            components = {
                'code_density_a': code_density_a,
                'code_density_b': code_density_b,
                'json_rate_a': json_rate_a,
                'json_rate_b': json_rate_b,
                'curly_norm_a': curly_norm_a,
                'curly_norm_b': curly_norm_b
            }
            
            # Interpretazione
            if 0.9 <= tdr_value <= 1.1:
                interpretation = "Profondità tecnica equivalente"
            elif tdr_value > 1.2:
                interpretation = f"Distribution B {tdr_value:.1f}x più tecnico"
            elif tdr_value < 0.8:
                interpretation = f"Distribution A {1/tdr_value:.1f}x più tecnico"
            else:
                interpretation = "Leggera differenza nella profondità tecnica"
            
            return ComparisonResult(
                metric_name="technical_depth_ratio",
                metric_type=MetricType.TECHNICAL,
                value=tdr_value,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo TDR: {e}")
            return ComparisonResult(
                metric_name="technical_depth_ratio",
                metric_type=MetricType.TECHNICAL,
                value=1.0,
                interpretation="Calcolo fallito",
                components={}
            )
    
    def calculate_qa_pattern_similarity(self) -> ComparisonResult:
        """
        QA Pattern Similarity (QAPS)
        Similarità nei pattern di domanda-risposta.
        """
        try:
            # Question rate
            question_rate_a = self.agg_a['presence_rates'].get('_user_ends_with_question', 0)
            question_rate_b = self.agg_b['presence_rates'].get('_user_ends_with_question', 0)
            question_diff = abs(question_rate_b - question_rate_a)
            
            # Answer adequacy (assistant chars / user chars)
            user_chars_a = self.agg_a['means'].get('_user_char_count', 0)
            assistant_chars_a = self.agg_a['means'].get('_assistant_char_count', 0)
            user_chars_b = self.agg_b['means'].get('_user_char_count', 0)
            assistant_chars_b = self.agg_b['means'].get('_assistant_char_count', 0)
            
            if user_chars_a > 0 and user_chars_b > 0:
                adequacy_a = assistant_chars_a / user_chars_a
                adequacy_b = assistant_chars_b / user_chars_b
                adequacy_diff = abs(adequacy_b - adequacy_a)
            else:
                adequacy_diff = 0.0
            
            # Response completeness (inverso di assistant ends with question)
            assistant_question_a = self.agg_a['presence_rates'].get('_assistant_ends_with_question', 0)
            assistant_question_b = self.agg_b['presence_rates'].get('_assistant_ends_with_question', 0)
            completeness_diff = abs(assistant_question_b - assistant_question_a)
            
            # Calcola QAPS
            qaps_value = 1 - (question_diff * 0.4 + adequacy_diff * 0.4 + completeness_diff * 0.2)
            
            components = {
                'question_rate_diff': question_diff,
                'answer_adequacy_diff': adequacy_diff,
                'response_completeness_diff': completeness_diff
            }
            
            # Interpretazione
            if qaps_value > 0.8:
                interpretation = "Pattern QA molto simili"
            elif qaps_value > 0.6:
                interpretation = "Pattern QA moderatamente simili"
            else:
                interpretation = "Pattern QA significativamente diversi"
            
            return ComparisonResult(
                metric_name="qa_pattern_similarity",
                metric_type=MetricType.QUALITY,
                value=qaps_value,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo QAPS: {e}")
            return ComparisonResult(
                metric_name="qa_pattern_similarity",
                metric_type=MetricType.QUALITY,
                value=0.0,
                interpretation="Calcolo fallito",
                components={}
            )
    
    def calculate_conversation_dynamics_gap(self) -> ComparisonResult:
        """
        Conversation Dynamics Gap (CDG)
        Differenza nelle dinamiche di conversazione avanzate.
        """
        try:
            # Think intensity
            think_chars_a = self.agg_a['means'].get('_think_char_count', 0)
            assistant_chars_a = self.agg_a['means'].get('_assistant_char_count', 1)
            think_chars_b = self.agg_b['means'].get('_think_char_count', 0)
            assistant_chars_b = self.agg_b['means'].get('_assistant_char_count', 1)
            
            think_intensity_a = think_chars_a / assistant_chars_a if assistant_chars_a > 0 else 0
            think_intensity_b = think_chars_b / assistant_chars_b if assistant_chars_b > 0 else 0
            think_intensity_diff = abs(think_intensity_b - think_intensity_a)
            
            # Function call frequency
            fc_count_a = self.agg_a['means'].get('_functioncall_messages_count', 0)
            total_msgs_a = self.agg_a['means'].get('_total_messages', 1)
            fc_count_b = self.agg_b['means'].get('_functioncall_messages_count', 0)
            total_msgs_b = self.agg_b['means'].get('_total_messages', 1)
            
            fc_freq_a = fc_count_a / total_msgs_a if total_msgs_a > 0 else 0
            fc_freq_b = fc_count_b / total_msgs_b if total_msgs_b > 0 else 0
            fc_freq_diff = abs(fc_freq_b - fc_freq_a)
            
            # Context usage
            context_count_a = self.agg_a['means'].get('_context_messages_count', 0)
            context_count_b = self.agg_b['means'].get('_context_messages_count', 0)
            
            context_usage_a = context_count_a / total_msgs_a if total_msgs_a > 0 else 0
            context_usage_b = context_count_b / total_msgs_b if total_msgs_b > 0 else 0
            context_usage_diff = abs(context_usage_b - context_usage_a)
            
            # Calcola CDG
            cdg_value = np.sqrt(
                think_intensity_diff**2 + 
                fc_freq_diff**2 + 
                context_usage_diff**2
            )
            
            components = {
                'think_intensity_diff': think_intensity_diff,
                'functioncall_freq_diff': fc_freq_diff,
                'context_usage_diff': context_usage_diff
            }
            
            # Interpretazione
            if cdg_value < 0.1:
                interpretation = "Dinamiche conversazionali identiche"
            elif cdg_value < 0.3:
                interpretation = "Leggere differenze nelle dinamiche"
            elif cdg_value < 0.5:
                interpretation = "Differenze sostanziali nelle dinamiche"
            else:
                interpretation = "Approcci conversazionali molto diversi"
            
            return ComparisonResult(
                metric_name="conversation_dynamics_gap",
                metric_type=MetricType.QUALITY,
                value=cdg_value,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo CDG: {e}")
            return ComparisonResult(
                metric_name="conversation_dynamics_gap",
                metric_type=MetricType.QUALITY,
                value=0.0,
                interpretation="Calcolo fallito",
                components={}
            )
    
    def calculate_readability_profile_distance(self) -> ComparisonResult:
        """
        Readability Profile Distance (RPD)
        Distanza nel profilo di leggibilità e complessità testuale.
        """
        try:
            # Flesch-Kincaid (se disponibile)
            fk_a = self.agg_a['means'].get('_flesch_kincaid', 8)  # default grade 8
            fk_b = self.agg_b['means'].get('_flesch_kincaid', 8)
            fk_diff = abs(fk_b - fk_a) / 12  # Normalizzato su scala 0-12
            
            # Type-Token Ratio
            ttr_a = self.agg_a['means'].get('_unique_word_ratio', 0.5)
            ttr_b = self.agg_b['means'].get('_unique_word_ratio', 0.5)
            ttr_diff = abs(ttr_b - ttr_a)
            
            # Stopword ratio
            stop_a = self.agg_a['means'].get('_stop_word_ratio', 0.3)
            stop_b = self.agg_b['means'].get('_stop_word_ratio', 0.3)
            stop_diff = abs(stop_b - stop_a)
            
            # Calcola RPD
            rpd_value = (fk_diff + ttr_diff + stop_diff) / 3
            
            components = {
                'flesch_kincaid_diff': fk_diff,
                'ttr_diff': ttr_diff,
                'stopword_ratio_diff': stop_diff
            }
            
            # Interpretazione
            if rpd_value < 0.1:
                interpretation = "Profilo di leggibilità identico"
            elif rpd_value < 0.2:
                interpretation = "Leggere differenze nel profilo di leggibilità"
            elif rpd_value < 0.4:
                interpretation = "Differenze significative nel profilo di leggibilità"
            else:
                interpretation = "Complessità testuale molto diversa"
            
            return ComparisonResult(
                metric_name="readability_profile_distance",
                metric_type=MetricType.QUALITY,
                value=rpd_value,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo RPD: {e}")
            return ComparisonResult(
                metric_name="readability_profile_distance",
                metric_type=MetricType.QUALITY,
                value=0.0,
                interpretation="Calcolo fallito",
                components={}
            )
    
    def calculate_format_consistency_score(self) -> ComparisonResult:
        """
        Format Consistency Score (FCS)
        Coerenza nei formati e strutture testuali.
        """
        try:
            differences = []
            components = {}
            
            # Markdown rate
            md_rate_a = self.agg_a['presence_rates'].get('_assistant_has_markdown', 0)
            md_rate_b = self.agg_b['presence_rates'].get('_assistant_has_markdown', 0)
            md_diff = abs(md_rate_b - md_rate_a)
            differences.append(md_diff)
            components['markdown_diff'] = md_diff
            
            # Table rate
            table_rate_a = self.agg_a['presence_rates'].get('_assistant_has_table', 0)
            table_rate_b = self.agg_b['presence_rates'].get('_assistant_has_table', 0)
            table_diff = abs(table_rate_b - table_rate_a)
            differences.append(table_diff)
            components['table_diff'] = table_diff
            
            # Bulletpoint rate
            bullet_a = self.agg_a['means'].get('_lines_bulletpoint_start', 0)
            lines_a = self.agg_a['means'].get('_lines_count', 1)
            bullet_b = self.agg_b['means'].get('_lines_bulletpoint_start', 0)
            lines_b = self.agg_b['means'].get('_lines_count', 1)
            
            bullet_rate_a = bullet_a / lines_a if lines_a > 0 else 0
            bullet_rate_b = bullet_b / lines_b if lines_b > 0 else 0
            bullet_diff = abs(bullet_rate_b - bullet_rate_a)
            differences.append(bullet_diff)
            components['bulletpoint_diff'] = bullet_diff
            
            # Ellipsis rate
            ellipsis_a = self.agg_a['means'].get('_lines_end_ellipsis_count', 0)
            ellipsis_b = self.agg_b['means'].get('_lines_end_ellipsis_count', 0)
            
            ellipsis_rate_a = ellipsis_a / lines_a if lines_a > 0 else 0
            ellipsis_rate_b = ellipsis_b / lines_b if lines_b > 0 else 0
            ellipsis_diff = abs(ellipsis_rate_b - ellipsis_rate_a)
            differences.append(ellipsis_diff)
            components['ellipsis_diff'] = ellipsis_diff
            
            # Calcola FCS
            avg_diff = np.mean(differences) if differences else 0
            fcs_value = 1 - avg_diff
            
            # Interpretazione
            if fcs_value > 0.9:
                interpretation = "Formati molto consistenti tra distribution"
            elif fcs_value > 0.7:
                interpretation = "Formati moderatamente consistenti"
            else:
                interpretation = "Formati significativamente diversi"
            
            return ComparisonResult(
                metric_name="format_consistency_score",
                metric_type=MetricType.QUALITY,
                value=fcs_value,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo FCS: {e}")
            return ComparisonResult(
                metric_name="format_consistency_score",
                metric_type=MetricType.QUALITY,
                value=0.0,
                interpretation="Calcolo fallito",
                components={}
            )
    
    def calculate_interaction_complexity_index(self) -> ComparisonResult:
        """
        Interaction Complexity Index (ICI) Ratio
        Rapporto della complessità delle interazioni.
        """
        try:
            # Calcola ICI per ogni distribution
            def compute_ici(agg):
                total_msgs = agg['means'].get('_total_messages', 5)
                has_think = agg['presence_rates'].get('_has_think', 0)
                has_fc = agg['presence_rates'].get('_has_functioncall', 0)
                mean_word_len = agg['means'].get('_mean_word_length', 5)
                symbol_ratio = agg['means'].get('_symbol_to_word_ratio', 0.05)
                
                return total_msgs * (1 + has_think + has_fc) * mean_word_len * (1 - symbol_ratio)
            
            ici_a = compute_ici(self.agg_a)
            ici_b = compute_ici(self.agg_b)
            
            ici_ratio = ici_b / ici_a if ici_a > 0 else 1.0
            
            components = {
                'ici_a': ici_a,
                'ici_b': ici_b,
                'total_msgs_a': self.agg_a['means'].get('_total_messages', 5),
                'total_msgs_b': self.agg_b['means'].get('_total_messages', 5),
                'think_presence_a': self.agg_a['presence_rates'].get('_has_think', 0),
                'think_presence_b': self.agg_b['presence_rates'].get('_has_think', 0)
            }
            
            # Interpretazione
            if 0.9 <= ici_ratio <= 1.1:
                interpretation = "Complessità interazioni simile"
            elif ici_ratio > 1.2:
                interpretation = f"Distribution B {ici_ratio:.1f}x più complesso nelle interazioni"
            elif ici_ratio < 0.8:
                interpretation = f"Distribution A {1/ici_ratio:.1f}x più complesso nelle interazioni"
            else:
                interpretation = "Leggera differenza nella complessità interazioni"
            
            return ComparisonResult(
                metric_name="interaction_complexity_index",
                metric_type=MetricType.QUALITY,
                value=ici_ratio,
                interpretation=interpretation,
                components=components
            )
            
        except Exception as e:
            logger.error(f"Errore calcolo ICI: {e}")
            return ComparisonResult(
                metric_name="interaction_complexity_index",
                metric_type=MetricType.QUALITY,
                value=1.0,
                interpretation="Calcolo fallito",
                components={}
            )

    def calculate_overall_structure_distance(self) -> ComparisonResult:
        """
        Overall Structure Distance (OSD) - REVISED
        Corretto il bias della media integrando la differenza dei massimi (outliers)
        e gestendo la stabilità della varianza.
        """
        try:
            numeric_features = [col for col in self.stats_a.columns 
                               if pd.api.types.is_numeric_dtype(self.stats_a[col]) 
                               and col in self.agg_a['means']]
            
            distances = []
            components = {}
            
            for feature in numeric_features:
                # Recupero medie, std e massimi (per intercettare differenze di coda)
                mean_a, mean_b = self.agg_a['means'][feature], self.agg_b['means'][feature]
                std_a, std_b = self.agg_a['stds'].get(feature, 1.0), self.agg_b['stds'].get(feature, 1.0)
                max_a, max_b = self.stats_a[feature].max(), self.stats_b[feature].max()
                
                # Pooled standard deviation con epsilon per stabilità
                pooled_std = np.sqrt((std_a**2 + std_b**2) / 2) + 1e-9
                
                # Normalizzazione della differenza delle medie
                norm_diff_mean = (mean_b - mean_a) / pooled_std
                
                # Componente Outlier: se i massimi differiscono violentemente, l'OSD deve salire
                # Usiamo una log-distance per i massimi per non dominare l'intera metrica
                outlier_factor = np.abs(np.log1p(max_b) - np.log1p(max_a))
                
                weight = self._get_feature_weight(feature)
                
                # La distanza è data dalla differenza media + un contributo dalla divergenza degli estremi
                combined_dist = (norm_diff_mean ** 2) + (outlier_factor * 0.5)
                distances.append(weight * combined_dist)
                
                components[feature] = norm_diff_mean

            osd_value = np.sqrt(np.mean(distances)) if distances else 0.0
            
            # Soglie tarate sulla nuova sensibilità (outlier inclusi)
            if osd_value < 0.25:
                interpretation = "Distribution molto simili strutturalmente"
            elif osd_value < 0.5:
                interpretation = "Differenze strutturali moderate (code incluse)"
            elif osd_value < 0.75:
                interpretation = "Differenze strutturali significative"
            else:
                interpretation = "Strutturalmente molto diversi (possibile incompatibilità)"
            
            return ComparisonResult(
                metric_name="overall_structure_distance",
                metric_type=MetricType.STRUCTURAL,
                value=osd_value,
                interpretation=interpretation,
                components=components
            )
        except Exception as e:
            logger.error(f"Errore calcolo OSD: {e}")
            return ComparisonResult("overall_structure_distance", MetricType.STRUCTURAL, 0.0, "Fallito", {})

    def calculate_content_richness_advantage(self) -> ComparisonResult:
        """
        Content Richness Advantage (CRA) - REVISED
        Risolto il problema della cancellazione degli errori calcolando 
        sia il vantaggio netto che la divergenza assoluta.
        """
        try:
            content_features = [
                '_assistant_has_json', '_assistant_has_code', 
                '_assistant_has_markdown', '_assistant_has_table',
                '_code_count', '_math_count'
            ]
            
            feature_importance = {
                '_assistant_has_json': 1.2, '_assistant_has_code': 1.2,
                '_assistant_has_markdown': 0.8, '_assistant_has_table': 1.0,
                '_code_count': 1.1, '_math_count': 0.9
            }
            
            net_advantage = 0.0
            absolute_divergence = 0.0  # Nuova componente per evitare cancellazione errori
            total_weight = 0.0
            components = {}
            
            for feature in content_features:
                if feature in self.agg_a['presence_rates']:
                    rate_a, rate_b = self.agg_a['presence_rates'][feature], self.agg_b['presence_rates'][feature]
                elif feature in self.stats_a.columns:
                    rate_a, rate_b = (self.stats_a[feature] > 0).mean(), (self.stats_b[feature] > 0).mean()
                else: continue
                
                importance = feature_importance.get(feature, 1.0)
                diff = (rate_b - rate_a)
                
                net_advantage += diff * importance
                absolute_divergence += abs(diff) * importance
                total_weight += importance
                components[feature] = diff

            cra_value = (net_advantage / total_weight * 100) if total_weight > 0 else 0.0
            div_value = (absolute_divergence / total_weight * 100) if total_weight > 0 else 0.0
            
            # Interpretazione critica: se la divergenza assoluta è molto più alta del vantaggio netto, 
            # significa che i dataset sono ricchi in modi opposti.
            if abs(cra_value) < 5 and div_value < 10:
                interpretation = "Ricchezza contenuti simile"
            elif div_value > abs(cra_value) + 15:
                interpretation = f"Divergenza forte: i dataset hanno feature strutturate diverse (Div: {div_value:.1f}%)"
            elif cra_value > 0:
                interpretation = f"Distribution B ha {cra_value:.1f}% più contenuti ricchi"
            else:
                interpretation = f"Distribution A ha {abs(cra_value):.1f}% più contenuti ricchi"
            
            # Aggiungiamo la divergenza assoluta ai componenti per il report critico
            components['absolute_divergence_score'] = div_value
            
            return ComparisonResult("content_richness_advantage", MetricType.CONTENT, cra_value, interpretation, components)
        except Exception as e:
            logger.error(f"Errore calcolo CRA: {e}")
            return ComparisonResult("content_richness_advantage", MetricType.CONTENT, 0.0, "Fallito", {})

    def calculate_role_balance_divergence(self) -> ComparisonResult:
        """
        Role Balance Divergence (RBD) - REVISED
        Migliorata la stabilità numerica e normalizzazione delle componenti.
        """
        try:
            # Calcolo UA ratio con epsilon per evitare log(0)
            u_a, a_a = self.agg_a['means'].get('_user_char_count', 0), self.agg_a['means'].get('_assistant_char_count', 0)
            u_b, a_b = self.agg_b['means'].get('_user_char_count', 0), self.agg_b['means'].get('_assistant_char_count', 0)
            
            # Ratio Difference (usiamo log2 per interpretabilità: 1 = raddoppio/dimezzamento)
            eps = 1e-4
            ratio_a = (u_a + eps) / (a_a + eps)
            ratio_b = (u_b + eps) / (a_b + eps)
            ratio_diff = abs(np.log2(ratio_b) - np.log2(ratio_a))
            
            # Normalizzazione: una differenza di log2=1 è già significativa, la scaliamo a 0-1
            norm_ratio_diff = min(ratio_diff / 2, 1.0) 
            
            # System & Think presence
            sys_diff = abs(self.agg_b['presence_rates'].get('_has_system', 0) - self.agg_a['presence_rates'].get('_has_system', 0))
            think_diff = abs(self.agg_b['presence_rates'].get('_has_think', 0) - self.agg_a['presence_rates'].get('_has_think', 0))
            
            # RBD pesato (il ratio UA è più critico della presenza di system prompt)
            rbd_value = (norm_ratio_diff * 0.5 + sys_diff * 0.2 + think_diff * 0.3)
            
            components = {'ratio_log2_diff': ratio_diff, 'system_diff': sys_diff, 'think_diff': think_diff}
            
            if rbd_value < 0.15: interpretation = "Equilibrio ruoli coerente"
            elif rbd_value < 0.35: interpretation = "Differenze moderate: stili conversazionali diversi"
            else: interpretation = "Dinamiche di ruolo divergenti: rischio bias nel training"
            
            return ComparisonResult("role_balance_divergence", MetricType.STRUCTURAL, rbd_value, interpretation, components)
        except Exception as e:
            logger.error(f"Errore calcolo RBD: {e}")
            return ComparisonResult("role_balance_divergence", MetricType.STRUCTURAL, 0.0, "Fallito", {})

    def calculate_token_efficiency_ratio(self) -> ComparisonResult:
        """
        Token Efficiency Ratio (TER) - REVISED
        Corretto il bias dell'efficienza pura introducendo la "Naturalness Penalty".
        Un dataset troppo denso (poche stopword, troppe parole uniche) potrebbe non essere naturale.
        """
        try:
            def get_efficiency_and_naturalness(agg):
                u_ratio = agg['means'].get('_unique_word_ratio', 0.5)
                s_ratio = agg['means'].get('_stop_word_ratio', 0.3)
                tokens = agg['means'].get('_token_count', 1.0) + 1e-9
                
                # Densità informativa
                info_density = u_ratio * (1 - s_ratio)
                # Penalità se s_ratio è troppo basso (< 0.2), segno di linguaggio non naturale
                naturalness = 1.0 if s_ratio > 0.2 else (s_ratio / 0.2)
                
                return (info_density / tokens) * naturalness

            eff_a = get_efficiency_and_naturalness(self.agg_a)
            eff_b = get_efficiency_and_naturalness(self.agg_b)
            
            ter_value = eff_b / eff_a if eff_a > 0 else 1.0
            
            components = {'eff_a': eff_a, 'eff_b': eff_b, 'naturalness_b': self.agg_b['means'].get('_stop_word_ratio', 0)}
            
            if 0.85 <= ter_value <= 1.15: interpretation = "Efficienza e naturalezza comparabili"
            elif ter_value > 1.15: interpretation = f"B è più efficiente (+{(ter_value-1)*100:.1f}%)"
            else: interpretation = f"A è più efficiente (+{(1/ter_value-1)*100:.1f}%)"
            
            return ComparisonResult("token_efficiency_ratio", MetricType.TECHNICAL, ter_value, interpretation, components)
        except Exception as e:
            logger.error(f"Errore calcolo TER: {e}")
            return ComparisonResult("token_efficiency_ratio", MetricType.TECHNICAL, 1.0, "Fallito", {})
    
    # ==================== METRICHE SEMPLICI ====================
    
    def calculate_simple_differences(self) -> Dict[str, ComparisonResult]:
        """
        Calcola differenze percentuali semplici per statistiche base.
        """
        results = {}
        
        # Statistiche chiave per differenziali semplici
        key_stats = [
            '_word_count',
            '_char_count',
            '_total_messages',
            '_user_char_count',
            '_assistant_char_count',
            '_mean_word_length',
            '_unique_word_ratio'
        ]
        
        for stat in key_stats:
            if stat in self.agg_a['means'] and stat in self.agg_b['means']:
                mean_a = self.agg_a['means'][stat]
                mean_b = self.agg_b['means'][stat]
                
                if mean_a != 0:
                    diff_pct = 100 * (mean_b - mean_a) / mean_a
                else:
                    diff_pct = 0.0
                
                results[stat] = ComparisonResult(
                    metric_name=f"simple_diff_{stat}",
                    metric_type=MetricType.SIMPLE_DIFF,
                    value=diff_pct,
                    interpretation=f"Differenza: {diff_pct:+.1f}%",
                    components={'mean_a': mean_a, 'mean_b': mean_b}
                )
        
        return results
    
    def calculate_presence_ratios(self) -> Dict[str, ComparisonResult]:
        """
        Calcola ratio di presenza con protezione per divisione per zero 
        e gestione logica della rarità.
        """
        results = {}
        binary_features = [
            '_has_system', '_has_think', '_has_functioncall',
            '_has_context', '_assistant_has_json', '_assistant_has_code'
        ]
        
        for feature in binary_features:
            if feature in self.agg_a['presence_rates'] and feature in self.agg_b['presence_rates']:
                rate_a = self.agg_a['presence_rates'][feature]
                rate_b = self.agg_b['presence_rates'][feature]
                
                # Protezione: usiamo un epsilon o un valore massimo per evitare 'inf'
                # Un ratio di 10x è già un segnale massimo per il bilanciamento.
                if rate_a > 1e-6:
                    presence_ratio = rate_b / rate_a
                else:
                    # Se A non lo ha e B sì, assegnamo un ratio "cap" convenzionale (es. 10.0)
                    presence_ratio = 10.0 if rate_b > 0.01 else 1.0
                
                results[feature] = ComparisonResult(
                    metric_name=f"presence_ratio_{feature}",
                    metric_type=MetricType.SIMPLE_DIFF,
                    value=presence_ratio,
                    interpretation=f"Ratio B/A: {presence_ratio:.2f}x",
                    components={'rate_a': rate_a, 'rate_b': rate_b}
                )
        return results

    # ==================== METODO PRINCIPALE ====================
    
    def compare_all(self) -> Dict[str, ComparisonResult]:
        """
        Calcola tutte le metriche comparative.
        
        Returns:
            Dizionario con tutti i risultati delle metriche
        """
        all_results = {}
        
        # Metriche principali
        main_metrics = [
            ('overall_structure_distance', self.calculate_overall_structure_distance),
            ('content_richness_advantage', self.calculate_content_richness_advantage),
            ('role_balance_divergence', self.calculate_role_balance_divergence),
            ('technical_depth_ratio', self.calculate_technical_depth_ratio),
            ('qa_pattern_similarity', self.calculate_qa_pattern_similarity),
            ('conversation_dynamics_gap', self.calculate_conversation_dynamics_gap),
            ('readability_profile_distance', self.calculate_readability_profile_distance),
            ('format_consistency_score', self.calculate_format_consistency_score),
            ('token_efficiency_ratio', self.calculate_token_efficiency_ratio),
            ('interaction_complexity_index', self.calculate_interaction_complexity_index)
        ]
        
        for name, method in main_metrics:
            try:
                result = method()
                all_results[name] = result
                logger.info(f"Metrica {name}: {result.value:.3f} - {result.interpretation}")
            except Exception as e:
                logger.error(f"Errore calcolo metrica {name}: {e}")
                all_results[name] = ComparisonResult(
                    metric_name=name,
                    metric_type=MetricType.STRUCTURAL,
                    value=0.0,
                    interpretation=f"Errore: {str(e)[:50]}",
                    components={}
                )
        
        # Metriche semplici
        simple_diffs = self.calculate_simple_differences()
        presence_ratios = self.calculate_presence_ratios()
        
        all_results.update(simple_diffs)
        all_results.update(presence_ratios)
        
        return all_results

    # ====================  REPORT ====================

    def generate_summary_report(self) -> str:
        """
        Genera un report testuale di sintesi con logica critica avanzata.
        """
        results = self.compare_all()
        
        report = []
        report.append("=" * 80)
        report.append("DISTRIBUTION COMPARISON REPORT (RIGOROUS VERSION)")
        report.append("=" * 80)
        report.append(f"Distribution A: {len(self.stats_a)} documenti")
        report.append(f"Distribution B: {len(self.stats_b)} documenti")
        report.append("")
        
        # 1. Metriche Principali
        report.append("PRINCIPAL METRICS:")
        report.append("-" * 40)
        main_metric_names = [
            'overall_structure_distance', 'content_richness_advantage', 
            'role_balance_divergence', 'technical_depth_ratio', 'qa_pattern_similarity'
        ]
        
        for name in main_metric_names:
            if name in results:
                res = results[name]
                report.append(f"{name:30} {res.value:7.3f}  |  {res.interpretation}")
        
        # 2. Focus Critico sulla Divergenza (Novità introdotta nella 1.1)
        if 'content_richness_advantage' in results:
            cra = results['content_richness_advantage']
            div = cra.components.get('absolute_divergence_score', 0)
            if div > abs(cra.value) + 15:
                report.append(f"\n[!] DIVERGENZA CRITICA: I dataset hanno specializzazioni opposte.")
                report.append(f"    Vantaggio Netto: {cra.value:+.1f}% | Divergenza Totale: {div:.1f}%")

        # 3. Differenze significative (Protezione contro outlier percentuali)
        report.append("\nSIGNIFICANT DIFFERENCES (Threshold > 15%):")
        report.append("-" * 40)
        for name, res in results.items():
            if name.startswith('simple_diff_'):
                # Evitiamo di segnalare variazioni enormi su base piccola che sono rumore
                if abs(res.value) > 15 and abs(res.components['mean_a']) > 0.1:
                    stat_name = name.replace('simple_diff_', '')
                    report.append(f"{stat_name:30} {res.value:+.1f}%")
        
        # 4. Raccomandazioni Attoriali (Conclusioni rigorose)
        report.append("\nCRITICAL RECOMMENDATIONS:")
        report.append("-" * 40)
        
        osd = results.get('overall_structure_distance')
        if osd and osd.value > 0.6:
            report.append("❌ STRUTTURA: Incompatibili. Il mix causerà instabilità nella lunghezza delle risposte.")
        
        rbd = results.get('role_balance_divergence')
        if rbd and rbd.value > 0.35:
            report.append("⚠️ RUOLI: Bilanciare i dataset. Uno dei due forza risposte Assistant troppo asimmetriche.")
            
        tdr = results.get('technical_depth_ratio')
        if tdr and (tdr.value > 2.0 or tdr.value < 0.5):
            report.append("⚡ TECNICO: Sbilanciamento estremo nella densità di codice/JSON.")

        return "\n".join(report)
