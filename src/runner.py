"""Ingestion runner - orchestrates fetching, normalizing, and storing datasets."""
import hashlib
import json
import logging
import os
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.manifest import DatasetSpec, Manifest
from src.store import LocalStore, compute_hash
from src import normalizer as norm
from src.sources import data_gov, fred

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


@dataclass
class DatasetResult:
    """Result of processing a single dataset."""
    dataset_id: str
    status: str  # success, skipped, failed
    raw_path: Optional[str] = None
    normalized_path: Optional[str] = None
    row_count: Optional[int] = None
    error: Optional[str] = None
    changed: bool = False


@dataclass
class IngestionReport:
    """Report of an ingestion run."""
    run_id: str
    manifest_name: str
    started_at: str
    completed_at: Optional[str] = None
    status: str = "running"
    results: List[DatasetResult] = field(default_factory=list)
    
    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.status == "success")
    
    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "skipped")
    
    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")


class IngestionRunner:
    """Orchestrates ingestion runs from manifests."""
    
    def __init__(
        self,
        manifest: Manifest,
        store: Optional[LocalStore] = None,
        api_keys: Optional[Dict[str, str]] = None,
    ):
        self.manifest = manifest
        self.store = store or LocalStore(
            manifest.settings.output_dir,
            manifest.settings.store_type,
        )
        # Merge API keys from manifest settings and explicit args
        self.api_keys = {**manifest.settings.api_keys, **(api_keys or {})}
    
    def run(self, dry_run: bool = False, force: bool = False) -> IngestionReport:
        """Execute the full manifest ingestion.
        
        Args:
            dry_run: If True, don't actually fetch or store anything
            force: If True, re-fetch even if content hash unchanged
            
        Returns:
            IngestionReport with results for each dataset
        """
        run_id = str(uuid4())[:8]
        started_at = datetime.now(timezone.utc).isoformat()
        manifest_hash = hashlib.sha256(
            json.dumps(self.manifest.__dict__, default=str).encode()
        ).hexdigest()[:16]
        
        report = IngestionReport(
            run_id=run_id,
            manifest_name=self.manifest.name,
            started_at=started_at,
        )
        
        log.info(f"Starting ingestion run {run_id} for manifest '{self.manifest.name}'")
        log.info(f"Processing {len(self.manifest.sources)} datasets")
        
        if not dry_run:
            self.store.start_run(run_id, self.manifest.name, manifest_hash)
        
        for spec in self.manifest.sources:
            result = self._process_dataset(spec, run_id, dry_run, force)
            report.results.append(result)
            
            log.info(f"  [{result.status.upper()}] {spec.id}" + 
                    (f" - {result.row_count} rows" if result.row_count else "") +
                    (f" - {result.error}" if result.error else ""))
        
        report.completed_at = datetime.now(timezone.utc).isoformat()
        report.status = "completed" if report.failed == 0 else "completed_with_errors"
        
        if not dry_run:
            self.store.complete_run(
                run_id,
                report.status,
                report.succeeded + report.skipped,
                report.failed,
            )
        
        log.info(f"Run {run_id} complete: {report.succeeded} succeeded, "
                f"{report.skipped} skipped, {report.failed} failed")
        
        return report
    
    def _process_dataset(
        self,
        spec: DatasetSpec,
        run_id: str,
        dry_run: bool,
        force: bool,
    ) -> DatasetResult:
        """Process a single dataset spec."""
        try:
            # Fetch raw content
            content, format_hint = self._fetch(spec)
            
            if dry_run:
                return DatasetResult(
                    dataset_id=spec.id,
                    status="success",
                    row_count=None,
                )
            
            # Check if content changed
            content_hash = compute_hash(content)
            last_hash = self.store.get_latest_hash(spec.id)
            
            if not force and last_hash == content_hash:
                return DatasetResult(
                    dataset_id=spec.id,
                    status="skipped",
                    changed=False,
                )
            
            # Register dataset
            self.store.register_dataset(
                spec.id,
                spec.source,
                title=spec.description or spec.id,
                license=spec.license,
            )
            
            # Save raw
            raw_path = self.store.save_raw(
                spec.id,
                spec.source,
                content,
                format_hint,
                run_id,
            )
            
            # Normalize
            result = norm.normalize_content(content, format_hint)
            
            # Save normalized
            normalized_path = self.store.save_normalized(
                spec.id,
                spec.source,
                result.data,
                result.schema,
            )
            
            # Record version
            self.store.record_version(
                spec.id,
                run_id,
                raw_path,
                normalized_path,
                content_hash,
                result.row_count,
                result.schema,
            )
            
            return DatasetResult(
                dataset_id=spec.id,
                status="success",
                raw_path=raw_path,
                normalized_path=normalized_path,
                row_count=result.row_count,
                changed=True,
            )
            
        except Exception as e:
            log.exception(f"Error processing {spec.id}")
            return DatasetResult(
                dataset_id=spec.id,
                status="failed",
                error=str(e),
            )
    
    def _fetch(self, spec: DatasetSpec) -> tuple[bytes, str]:
        """Fetch raw content for a dataset spec.
        
        Returns:
            Tuple of (content bytes, format hint)
        """
        if spec.source == "fred":
            return self._fetch_fred(spec)
        elif spec.source == "data.gov":
            return self._fetch_datagov(spec)
        else:
            raise ValueError(f"Unsupported source: {spec.source}")
    
    def _fetch_fred(self, spec: DatasetSpec) -> tuple[bytes, str]:
        """Fetch FRED series observations."""
        api_key = self.api_keys.get("fred") or os.environ.get("FRED_API_KEY")
        if not api_key:
            raise ValueError("FRED API key required")
        
        if not spec.series_id:
            raise ValueError(f"FRED dataset {spec.id} requires series_id")
        
        # Fetch observations
        observations = fred.get_observations(
            spec.series_id,
            api_key=api_key,
        )
        
        # Also get series metadata
        series_info = None
        for series in fred.iter_series(spec.series_id, limit=1, api_key=api_key):
            series_info = series
            break
        
        result = {
            "series_id": spec.series_id,
            "series_info": series_info,
            "observations": observations,
        }
        
        return json.dumps(result, indent=2).encode("utf-8"), "json"
    
    def _fetch_datagov(self, spec: DatasetSpec) -> tuple[bytes, str]:
        """Fetch data.gov dataset."""
        if spec.dataset_id:
            # Fetch specific dataset by ID
            url = f"https://catalog.data.gov/api/3/action/package_show?id={spec.dataset_id}"
            with urllib.request.urlopen(url) as response:
                content = response.read()
            return content, "json"
        
        elif spec.query:
            # Search and fetch first result
            datasets = list(data_gov.iter_datasets(query=spec.query, limit=1))
            if not datasets:
                raise ValueError(f"No datasets found for query: {spec.query}")
            
            # Get resources from first dataset
            dataset = datasets[0]
            resources = dataset.get("resources", [])
            
            # Find best resource (prefer CSV, JSON)
            best_resource = None
            for fmt in ["CSV", "JSON", "XML"]:
                for res in resources:
                    if res.get("format", "").upper() == fmt:
                        best_resource = res
                        break
                if best_resource:
                    break
            
            if not best_resource:
                # Just return the metadata
                return json.dumps(dataset, indent=2).encode("utf-8"), "json"
            
            # Download the resource
            url = best_resource.get("url")
            if url:
                req = urllib.request.Request(url, headers={"User-Agent": "AmethystDataIngestion/1.0"})
                with urllib.request.urlopen(req, timeout=120) as response:
                    content = response.read()
                return content, best_resource.get("format", "json").lower()
            
            return json.dumps(dataset, indent=2).encode("utf-8"), "json"
        
        else:
            raise ValueError(f"data.gov dataset {spec.id} requires dataset_id or query")


def run_manifest(
    manifest_path: str,
    data_dir: Optional[str] = None,
    api_keys: Optional[Dict[str, str]] = None,
    dry_run: bool = False,
    force: bool = False,
) -> IngestionReport:
    """Convenience function to run a manifest file.
    
    Args:
        manifest_path: Path to YAML/JSON manifest
        data_dir: Override output directory
        api_keys: API keys dict
        dry_run: Don't actually fetch
        force: Re-fetch even if unchanged
        
    Returns:
        IngestionReport
    """
    manifest = Manifest.load(manifest_path)
    
    if data_dir:
        manifest.settings.output_dir = data_dir
    
    errors = manifest.validate()
    if errors:
        raise ValueError(f"Invalid manifest: {errors}")
    
    runner = IngestionRunner(manifest, api_keys=api_keys)
    return runner.run(dry_run=dry_run, force=force)
