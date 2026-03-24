# ui/data_studio/strategy/stats_retriver/retriver.py
import duckdb
import logging
import os

# Configurazione Logger
logger = logging.getLogger(__name__)

class DistributionStatsRetriever:
    def __init__(self):
        self.con = duckdb.connect(database=':memory:')

    def fetch_all_stats(self, dist_bag):
        """
        Itera sul bag e recupera le stats reali dai file parquet.
        """
        logger.info(f"🚀 Avvio recupero statistiche per {len(dist_bag)} dataset.")
        stats_map = {}
        
        for ds_id, group in dist_bag.items():
            for dist in group['dist']:
                dist_id_str = str(dist.id)
                logger.info(f"🔍 Analisi distribuzione: {dist.name} (ID: {dist_id_str})")
                
                try:
                    stats_uri = self._get_stats_uri(dist.uri)
                    
                    if not stats_uri or not os.path.exists(stats_uri):
                        logger.warning(f"⚠️ Stats file non trovato per {dist.name}. Path: {stats_uri}")
                        stats_map[dist_id_str] = {'samples': 0, 'tokens': 0, 'words': 0}
                        continue

                    query = f"""
                        SELECT 
                            COUNT(*) as samples,
                            SUM(_token_count) as tokens,
                            SUM(_word_count) as words
                        FROM read_parquet('{stats_uri}')
                    """
                    res = self.con.execute(query).fetchone()
                    
                    stats_map[dist_id_str] = {
                        'samples': int(res[0]) if res[0] else 0,
                        'tokens': int(res[1]) if res[1] else 0,
                        'words': int(res[2]) if res[2] else 0
                    }
                    logger.info(f"✅ Stats recuperate per {dist.name}: {stats_map[dist_id_str]}")

                except Exception as e:
                    logger.error(f"❌ Errore critico nel recupero stats per {dist.name}: {str(e)}")
                    stats_map[dist_id_str] = {'samples': 0, 'tokens': 0, 'words': 0}
        
        return stats_map

    def _get_stats_uri(self, dist_uri):
        """Logica di mapping interna con logging del path risultante."""
        from utils.path_utils import to_internal_path
        
        try:
            MAPPED_DATA_DIR = os.getenv('MAPPED_DATA_DIR')
            STATS_DATA_DIR = os.getenv('STATS_DATA_DIR')   
            LOW_LEVEL_STATS_EXTENSION = os.getenv('LOW_LEVEL_STATS_EXTENSION')
            BASE_PREFIX = os.getenv('BASE_PREFIX', '')
            logger.info(f"Mapping URI: {dist_uri} con MAPPED_DATA_DIR={MAPPED_DATA_DIR}, STATS_DATA_DIR={STATS_DATA_DIR}, LOW_LEVEL_STATS_EXTENSION={LOW_LEVEL_STATS_EXTENSION}")
            internal_path = to_internal_path(dist_uri).replace(BASE_PREFIX, '')
            logger.info(f"Internal path dopo mapping: {internal_path}")
            stats_uri= internal_path.replace(MAPPED_DATA_DIR, STATS_DATA_DIR) + LOW_LEVEL_STATS_EXTENSION
            logger.info(f"Stats URI risultante: {stats_uri}")
            return stats_uri
        except Exception as e:
            logger.error(f"Errore nel mapping dell'URI: {e}")
            return None