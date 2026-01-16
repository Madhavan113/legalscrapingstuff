"""Local storage abstraction with DuckDB for metadata and normalized data."""
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False


def compute_hash(content: bytes) -> str:
    """Compute SHA-256 hash of content."""
    return hashlib.sha256(content).hexdigest()


@dataclass
class DatasetVersion:
    """A specific version of a dataset."""
    dataset_id: str
    run_id: str
    raw_path: str
    normalized_path: Optional[str]
    content_hash: str
    row_count: Optional[int]
    retrieved_at: str
    schema: Optional[Dict]


class LocalStore:
    """Local storage for raw files, normalized data, and metadata."""
    
    def __init__(self, base_dir: str, store_type: str = "duckdb"):
        self.base_dir = os.path.abspath(base_dir)
        self.store_type = store_type
        self.raw_dir = os.path.join(base_dir, "raw")
        self.normalized_dir = os.path.join(base_dir, "normalized")
        
        # Create directories
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.normalized_dir, exist_ok=True)
        
        # Initialize metadata store
        if store_type == "duckdb" and HAS_DUCKDB:
            self.db_path = os.path.join(base_dir, "metadata.duckdb")
            self._init_duckdb()
        else:
            # Fall back to JSON-based metadata
            self.db_path = os.path.join(base_dir, "metadata.json")
            self._init_json_store()
    
    def _init_duckdb(self) -> None:
        """Initialize DuckDB schema."""
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                id VARCHAR PRIMARY KEY,
                source VARCHAR NOT NULL,
                title VARCHAR,
                description VARCHAR,
                license VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id VARCHAR PRIMARY KEY,
                manifest_name VARCHAR,
                manifest_hash VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                datasets_processed INTEGER DEFAULT 0,
                datasets_failed INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dataset_versions (
                id INTEGER PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                run_id VARCHAR NOT NULL,
                raw_path VARCHAR,
                normalized_path VARCHAR,
                content_hash VARCHAR,
                row_count INTEGER,
                retrieved_at TIMESTAMP,
                schema_json VARCHAR,
                FOREIGN KEY (dataset_id) REFERENCES datasets(id)
            )
        """)
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS version_seq START 1
        """)
        conn.close()
    
    def _init_json_store(self) -> None:
        """Initialize JSON-based metadata store."""
        if not os.path.exists(self.db_path):
            with open(self.db_path, "w") as f:
                json.dump({
                    "datasets": {},
                    "runs": {},
                    "versions": []
                }, f, indent=2)
    
    def _get_conn(self):
        """Get DuckDB connection."""
        return duckdb.connect(self.db_path)
    
    def save_raw(
        self,
        dataset_id: str,
        source: str,
        content: bytes,
        format: str,
        run_id: str,
    ) -> str:
        """Save raw file and return path."""
        # Create source subdirectory
        source_dir = os.path.join(self.raw_dir, source.replace(".", "_"))
        os.makedirs(source_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        ext = format.lower()
        filename = f"{dataset_id}_{timestamp}.{ext}"
        filepath = os.path.join(source_dir, filename)
        
        with open(filepath, "wb") as f:
            f.write(content)
        
        return filepath
    
    def save_normalized(
        self,
        dataset_id: str,
        source: str,
        data: List[Dict],
        schema: Dict,
    ) -> str:
        """Save normalized data and return path."""
        source_dir = os.path.join(self.normalized_dir, source.replace(".", "_"))
        os.makedirs(source_dir, exist_ok=True)
        
        # Save as JSON (could be parquet with pyarrow)
        filename = f"{dataset_id}.json"
        filepath = os.path.join(source_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({
                "schema": schema,
                "data": data
            }, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def register_dataset(
        self,
        dataset_id: str,
        source: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        license: Optional[str] = None,
    ) -> None:
        """Register a dataset in metadata."""
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            conn.execute("""
                INSERT INTO datasets (id, source, title, description, license)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    license = EXCLUDED.license
            """, [dataset_id, source, title, description, license])
            conn.close()
        else:
            self._update_json_metadata(
                "datasets",
                dataset_id,
                {"source": source, "title": title, "description": description, "license": license}
            )
    
    def start_run(self, run_id: str, manifest_name: str, manifest_hash: str) -> None:
        """Record start of an ingestion run."""
        now = datetime.now(timezone.utc).isoformat()
        
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            conn.execute("""
                INSERT INTO ingestion_runs (run_id, manifest_name, manifest_hash, started_at, status)
                VALUES (?, ?, ?, ?, 'running')
            """, [run_id, manifest_name, manifest_hash, now])
            conn.close()
    
    def complete_run(
        self,
        run_id: str,
        status: str,
        datasets_processed: int,
        datasets_failed: int,
    ) -> None:
        """Record completion of an ingestion run."""
        now = datetime.now(timezone.utc).isoformat()
        
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            conn.execute("""
                UPDATE ingestion_runs
                SET completed_at = ?, status = ?, datasets_processed = ?, datasets_failed = ?
                WHERE run_id = ?
            """, [now, status, datasets_processed, datasets_failed, run_id])
            conn.close()
    
    def record_version(
        self,
        dataset_id: str,
        run_id: str,
        raw_path: str,
        normalized_path: Optional[str],
        content_hash: str,
        row_count: Optional[int],
        schema: Optional[Dict],
    ) -> None:
        """Record a new version of a dataset."""
        now = datetime.now(timezone.utc).isoformat()
        
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            conn.execute("""
                INSERT INTO dataset_versions 
                (id, dataset_id, run_id, raw_path, normalized_path, content_hash, row_count, retrieved_at, schema_json)
                VALUES (nextval('version_seq'), ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                dataset_id, run_id, raw_path, normalized_path,
                content_hash, row_count, now,
                json.dumps(schema) if schema else None
            ])
            conn.close()
    
    def get_latest_hash(self, dataset_id: str) -> Optional[str]:
        """Get the content hash of the latest version."""
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            result = conn.execute("""
                SELECT content_hash FROM dataset_versions
                WHERE dataset_id = ?
                ORDER BY retrieved_at DESC
                LIMIT 1
            """, [dataset_id]).fetchone()
            conn.close()
            return result[0] if result else None
        return None
    
    def get_runs(self, limit: int = 10) -> List[Dict]:
        """Get recent ingestion runs."""
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            result = conn.execute("""
                SELECT run_id, manifest_name, started_at, completed_at, status, 
                       datasets_processed, datasets_failed
                FROM ingestion_runs
                ORDER BY started_at DESC
                LIMIT ?
            """, [limit]).fetchall()
            conn.close()
            return [
                {
                    "run_id": r[0],
                    "manifest_name": r[1],
                    "started_at": r[2],
                    "completed_at": r[3],
                    "status": r[4],
                    "datasets_processed": r[5],
                    "datasets_failed": r[6],
                }
                for r in result
            ]
        return []
    
    def get_datasets(self) -> List[Dict]:
        """Get all registered datasets."""
        if self.store_type == "duckdb" and HAS_DUCKDB:
            conn = self._get_conn()
            result = conn.execute("""
                SELECT d.id, d.source, d.title, d.license,
                       (SELECT content_hash FROM dataset_versions 
                        WHERE dataset_id = d.id ORDER BY retrieved_at DESC LIMIT 1) as latest_hash,
                       (SELECT retrieved_at FROM dataset_versions 
                        WHERE dataset_id = d.id ORDER BY retrieved_at DESC LIMIT 1) as last_updated
                FROM datasets d
            """).fetchall()
            conn.close()
            return [
                {
                    "id": r[0],
                    "source": r[1],
                    "title": r[2],
                    "license": r[3],
                    "latest_hash": r[4],
                    "last_updated": r[5],
                }
                for r in result
            ]
        return []
    
    def _update_json_metadata(self, collection: str, key: str, data: Dict) -> None:
        """Update JSON metadata store."""
        with open(self.db_path, "r") as f:
            metadata = json.load(f)
        
        metadata[collection][key] = data
        
        with open(self.db_path, "w") as f:
            json.dump(metadata, f, indent=2)
