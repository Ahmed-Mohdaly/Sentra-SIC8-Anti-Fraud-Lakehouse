"""FastAPI REST API for transactions, operational control, and cloud health probes."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
from fastapi import Depends, FastAPI, HTTPException, Header, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import desc, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.database import (
    CardModel,
    CityReferenceModel,
    MerchantCategoryCodeModel,
    TransactionModel,
    UserModel,
    check_db_health,
    create_db_engine,
    init_db,
    sanitize_db_error,
    seed_reference_data,
)
from synthetic_streaming.generator_service import GeneratorService

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Request / Response Models
# ------------------------------------------------------------------------------

class UpdateConfigRequest(BaseModel):
    target_tps: Optional[float] = Field(None, gt=0, description="New target TPS / rate")
    generator_rate: Optional[float] = Field(None, gt=0, description="Alias for target_tps")
    new_user_transaction_ratio: Optional[float] = Field(None, ge=0.0, le=1.0, description="New user transaction ratio")


class GenerateBatchRequest(BaseModel):
    count: int = Field(default=50, ge=1, le=10000, description="Number of transactions to generate")
    run_id: Optional[str] = Field(None, description="Optional custom run ID identifier")


class GenerateBatchResponse(BaseModel):
    generated_count: int
    first_id: Optional[int] = None
    last_id: Optional[int] = None
    run_id: str
    message: str


class TransactionResponse(BaseModel):
    id: int = Field(..., description="Monotonic sequence ID / Watermark")
    transaction_id: str = Field(..., description="Stable unique transaction identifier")
    event_id: str = Field(..., description="Event UUIDv4")
    event_type: str = Field(..., description="Lifecycle event type e.g. transaction.created")
    user_id: int = Field(..., description="User ID")
    card_index: int = Field(..., description="Card index referencing user's cards")
    amount: str = Field(..., description="Standardized decimal string amount (e.g. 45.20)")
    amount_raw: str = Field(..., description="Raw formatted currency string (e.g. $45.20)")
    use_chip: str = Field(..., description="Payment interaction method")
    transaction_entry_method: Optional[str] = Field("unknown", description="Detailed payment-entry classification")
    merchant_name: str = Field(..., description="Merchant identifier or name")
    merchant_city: Optional[str] = Field(None, description="Merchant city")
    merchant_state: Optional[str] = Field(None, description="Merchant US state")
    merchant_latitude: Optional[float] = Field(None, description="City-level reference latitude of merchant")
    merchant_longitude: Optional[float] = Field(None, description="City-level reference longitude of merchant")
    zip: Optional[str] = Field(None, description="Merchant zipcode")
    mcc: int = Field(..., description="Merchant Category Code")
    errors: Optional[str] = Field(None, description="Processing error or decline code")
    is_fraud: str = Field(..., description="Fraud indicator ('Yes' or 'No')")
    scenario: str = Field(..., description="Simulated scenario name")
    source_system: str = Field(..., description="Originating source system")
    run_id: str = Field(..., description="Generator execution run ID")
    created_at: datetime = Field(..., description="UTC creation timestamp")
    payload: Optional[Dict[str, Any]] = Field(None, description="Full raw event payload")


class PaginationMeta(BaseModel):
    limit: int
    count: int
    has_more: bool
    next_cursor: Optional[str] = None
    max_id: Optional[int] = None


class PaginatedTransactionsResponse(BaseModel):
    items: List[TransactionResponse]
    limit: int
    count: int
    has_more: bool
    next_cursor: Optional[str] = None
    max_id: Optional[int] = None
    pagination: Optional[PaginationMeta] = None


# ------------------------------------------------------------------------------
# Application Factory
# ------------------------------------------------------------------------------

def create_app(
    service: Optional[GeneratorService] = None,
    config: Optional[Any] = None,
    db_engine: Optional[Engine] = None,
    **kwargs: Any,
) -> FastAPI:
    """FastAPI application factory for the Transaction REST API and Generator Management Plane."""
    if config is not None and isinstance(config, GeneratorConfig):
        cfg = config
    elif service is not None:
        cfg = service.config
    else:
        cfg = GeneratorConfig()

    # Determine database engine
    if db_engine is not None:
        engine = db_engine
    elif service is not None:
        engine = service.db_engine
    else:
        engine = create_db_engine(cfg.database_url)
        init_db(engine)
        if cfg.auto_seed:
            try:
                seed_reference_data(engine, data_dir=cfg.baseline_data_dir)
            except Exception as e:
                logger.warning(f"Auto-seeding reference data skipped or failed: {e}")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Manage application startup checks and graceful shutdown on SIGTERM/SIGINT."""
        logger.info("Initializing Synthetic Transaction Data Provider API...")
        yield
        logger.info("Shutting down Synthetic Transaction Data Provider API...")
        if service is not None:
            try:
                service.stop(timeout=5.0)
            except Exception as e:
                logger.warning(f"Error stopping GeneratorService during shutdown: {e}")
        if engine is not None:
            try:
                engine.dispose()
            except Exception as e:
                logger.warning(f"Error disposing database engine during shutdown: {e}")

    # Ensure an active embedded GeneratorService is bound to the application
    if service is None:
        try:
            service = GeneratorService(config=cfg, db_engine=engine)
        except Exception as e:
            logger.warning(f"GeneratorService could not be auto-instantiated: {e}")
            service = None

    app = FastAPI(
        title="Synthetic Transaction Data Provider API",
        description="REST API for serving synthetic transactions, pagination, incremental watermark extraction, and reference data.",
        version="0.2.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def get_db() -> Session:
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def verify_auth(
        authorization: Optional[str] = Header(None),
        x_api_key: Optional[str] = Header(None),
    ) -> None:
        """Validate optional API token if configured."""
        if not cfg.api_key:
            return

        expected_key = cfg.api_key.get_secret_value()
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization[7:]
        elif x_api_key:
            token = x_api_key

        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication token required.",
            )
        if token != expected_key:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid authentication credentials.",
            )

    # --------------------------------------------------------------------------
    # Health and Readiness Probes
    # --------------------------------------------------------------------------

    @app.get("/health", tags=["Operational"])
    def get_health() -> Dict[str, Any]:
        """Liveness check verifying database connectivity and application status."""
        db_health = check_db_health(engine)
        if db_health.get("status") == "unhealthy":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Database connectivity failure: {db_health.get('error')}",
            )

        gen_state = service.state.value if service else "unbound"
        return {
            "status": "ok",
            "service": "synthetic-transaction-data-provider",
            "mode": "standalone",
            "app_env": cfg.app_env,
            "database": db_health,
            "generator_state": gen_state,
        }

    @app.get("/ready", tags=["Operational"])
    def get_readiness() -> Dict[str, Any]:
        """Readiness probe checking database connectivity and publisher availability."""
        db_health = check_db_health(engine)
        if db_health.get("status") == "unhealthy":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Database not ready: {db_health.get('error')}",
            )

        if service is not None:
            pub_health = service.publisher.health()
            if pub_health.get("status") == "unhealthy":
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Publisher not ready: {pub_health}",
                )
            return {
                "status": "ready",
                "database": db_health,
                "publisher": pub_health,
                "generator_state": service.state.value,
            }

        return {"status": "ready", "database": db_health}

    # --------------------------------------------------------------------------
    # Source A: Transaction REST API Endpoints
    # --------------------------------------------------------------------------

    @app.get(
        "/api/v1/transactions",
        response_model=PaginatedTransactionsResponse,
        tags=["Transactions (Historical DB)"],
        summary="Query historical database-backed transactions (Legacy Store)",
    )
    def list_transactions(
        limit: int = Query(default=50, ge=1, le=1000, description="Page size limit (1 to 1000)"),
        cursor: Optional[str] = Query(None, description="Opaque cursor (sequential ID) for keyset pagination"),
        since_id: Optional[int] = Query(None, ge=0, description="Watermark: return records with id > since_id"),
        since_timestamp: Optional[datetime] = Query(None, description="Incremental extraction: created_at >= since_timestamp"),
        until_timestamp: Optional[datetime] = Query(None, description="Incremental extraction: created_at <= until_timestamp"),
        user_id: Optional[int] = Query(None, description="Filter by user ID"),
        scenario: Optional[str] = Query(None, description="Filter by transaction scenario"),
        is_fraud: Optional[str] = Query(None, description="Filter by fraud indicator ('Yes' or 'No')"),
        transaction_entry_method: Optional[str] = Query(None, description="Filter by transaction entry method"),
        order: str = Query(default="asc", pattern="^(asc|desc)$", description="Sort order by sequential ID"),
        db: Session = Depends(get_db),
    ) -> PaginatedTransactionsResponse:
        """Serve historical/baseline transaction records from persistent PostgreSQL storage.
        
        ARCHITECTURE NOTE:
        Newly generated transactions stream directly to Amazon MSK and Amazon S3 Bronze lakehouse
        and are strictly NOT inserted into PostgreSQL. This endpoint queries the historical baseline
        relational store only. For live transactions, consume the Amazon MSK `transactions` topic.
        
        Supports:
        - Keyset cursor pagination without offset degradation.
        - Monotonic sequence watermarking (`since_id`).
        - Time range filtering (`since_timestamp`, `until_timestamp`).
        - Faceted filtering by user, scenario, fraud status, and entry method.
        """
        # Keyset pagination and monotonic watermarking require ascending sequential order
        if (since_id is not None or cursor is not None) and order == "desc":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Keyset pagination with since_id/cursor requires order='asc' to maintain monotonic sequential extraction.",
            )

        query = db.query(TransactionModel)

        # Apply cursor
        if cursor is not None:
            try:
                cursor_id = int(cursor)
                query = query.filter(TransactionModel.id > cursor_id)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid cursor format: cursor must be an integer sequence ID string.",
                )

        # Apply sequence watermark
        if since_id is not None:
            query = query.filter(TransactionModel.id > since_id)

        # Apply timestamp filters
        if since_timestamp is not None:
            query = query.filter(TransactionModel.created_at >= since_timestamp)
        if until_timestamp is not None:
            query = query.filter(TransactionModel.created_at <= until_timestamp)

        # Apply attribute filters
        if user_id is not None:
            query = query.filter(TransactionModel.user_id == user_id)
        if scenario is not None:
            query = query.filter(TransactionModel.scenario == scenario)
        if transaction_entry_method is not None:
            query = query.filter(TransactionModel.transaction_entry_method == transaction_entry_method)
        if is_fraud is not None:
            normalized_fraud = is_fraud.strip().capitalize()
            if normalized_fraud not in ("Yes", "No"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid is_fraud parameter: must be 'Yes' or 'No'.",
                )
            query = query.filter(TransactionModel.is_fraud == normalized_fraud)

        # Sort order
        if order == "desc":
            query = query.order_by(desc(TransactionModel.id))
        else:
            query = query.order_by(TransactionModel.id.asc())

        # Fetch limit + 1 to detect has_more
        records = query.limit(limit + 1).all()

        has_more = len(records) > limit
        page_records = records[:limit]

        next_cursor = str(page_records[-1].id) if (has_more and page_records and order == "asc") else None
        max_id = max([r.id for r in page_records]) if page_records else since_id

        items = [TransactionResponse(**r.to_dict()) for r in page_records]
        count = len(items)

        pagination_meta = PaginationMeta(
            limit=limit,
            count=count,
            has_more=has_more,
            next_cursor=next_cursor,
            max_id=max_id,
        )

        return PaginatedTransactionsResponse(
            items=items,
            limit=limit,
            count=count,
            has_more=has_more,
            next_cursor=next_cursor,
            max_id=max_id,
            pagination=pagination_meta,
        )

    @app.get(
        "/api/v1/transactions/{transaction_id}",
        response_model=TransactionResponse,
        tags=["Transactions (Historical DB)"],
        summary="Retrieve a historical transaction from database by transaction ID or event UUID",
    )
    def get_transaction(
        transaction_id: str,
        db: Session = Depends(get_db),
    ) -> TransactionResponse:
        """Retrieve a single historical transaction record from PostgreSQL.
        
        NOTE: Real-time streamed transactions are not stored in PostgreSQL.
        """
        rec = (
            db.query(TransactionModel)
            .filter((TransactionModel.transaction_id == transaction_id) | (TransactionModel.event_id == transaction_id))
            .first()
        )
        if not rec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transaction with ID '{transaction_id}' was not found.",
            )
        return TransactionResponse(**rec.to_dict())

    # --------------------------------------------------------------------------
    # Generator Operational & Batch Execution Endpoints
    # --------------------------------------------------------------------------

    @app.post(
        "/api/v1/generator/generate",
        response_model=GenerateBatchResponse,
        tags=["Generator Control"],
        summary="Synchronously trigger batch generation of transactions into persistent storage",
        dependencies=[Depends(verify_auth)],
    )
    def generate_batch_transactions(
        req: GenerateBatchRequest = GenerateBatchRequest(),
        db: Session = Depends(get_db),
    ) -> GenerateBatchResponse:
        """Trigger synchronous generation and persistence of N transaction records."""
        if service is not None:
            try:
                records = service.generate_batch(count=req.count, run_id=req.run_id)
            except Exception as e:
                logger.error(f"Batch generation failed: {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Batch generation failed: {sanitize_db_error(e)}",
                )
            first_id = records[0].id if records else None
            last_id = records[-1].id if records else None
            effective_run_id = req.run_id or (records[0].run_id if records else "unknown")
            return GenerateBatchResponse(
                generated_count=len(records),
                first_id=first_id,
                last_id=last_id,
                run_id=effective_run_id,
                message=f"Successfully generated and persisted {len(records)} transaction records.",
            )

        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Active generator service not bound to API instance.",
        )

    @app.get("/api/v1/generator/status", tags=["Generator Control"])
    def get_generator_status(db: Session = Depends(get_db)) -> Dict[str, Any]:
        """Current operational status, throughput counters, and database entity counts."""
        if service is not None:
            return service.get_status()

        db_counts = {
            "transactions": db.query(TransactionModel).count(),
            "users": db.query(UserModel).count(),
            "cards": db.query(CardModel).count(),
            "merchant_category_codes": db.query(MerchantCategoryCodeModel).count(),
            "city_reference": db.query(CityReferenceModel).count(),
        }
        return {
            "state": "STANDALONE_API",
            "app_env": cfg.app_env,
            "database_counts": db_counts,
            "note": "API plane active without bound generator service",
        }

    @app.post("/api/v1/generator/start", tags=["Generator Control"], dependencies=[Depends(verify_auth)])
    def start_generator() -> Dict[str, Any]:
        """Start continuous background transaction generation."""
        if service is not None:
            success = service.start()
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Generator is already running or failed to initialize.",
                )
            return {"message": "Generator started successfully", "status": service.get_status()}
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Active generator service not bound to API instance.",
        )

    @app.post("/api/v1/generator/stop", tags=["Generator Control"], dependencies=[Depends(verify_auth)])
    def stop_generator() -> Dict[str, Any]:
        """Gracefully stop background transaction generation."""
        if service is not None:
            service.stop()
            return {"message": "Generator stopped successfully", "status": service.get_status()}
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Active generator service not bound to API instance.",
        )

    @app.get("/api/v1/generator/config", tags=["Configuration"])
    def get_config() -> Dict[str, Any]:
        """View safe configuration settings with masked credentials."""
        return cfg.to_safe_dict()

    @app.patch("/api/v1/generator/config", tags=["Configuration"], dependencies=[Depends(verify_auth)])
    def update_config(req: UpdateConfigRequest) -> Dict[str, Any]:
        """Update runtime configurable generator parameters."""
        effective_rate = req.generator_rate or req.target_tps
        if service is not None:
            updated = service.update_runtime_config(
                target_tps=effective_rate,
                new_user_ratio=req.new_user_transaction_ratio,
            )
            return {"message": "Configuration updated successfully", "updated": updated}
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Active generator service not bound to API instance.",
        )

    @app.get(
        "/api/v1/reference/cities",
        tags=["Reference Data"],
        summary="List or search city geographic reference records",
    )
    def list_reference_cities(
        city: Optional[str] = Query(None, description="Filter by city name"),
        state: Optional[str] = Query(None, description="Filter by state code"),
        limit: int = Query(default=100, ge=1, le=1000),
        db: Session = Depends(get_db),
    ) -> List[Dict[str, Any]]:
        """Retrieve city geographic reference data with coordinates and provenance."""
        q = db.query(CityReferenceModel)
        if city:
            q = q.filter(func.lower(CityReferenceModel.city_name) == city.strip().lower())
        if state:
            q = q.filter(func.lower(CityReferenceModel.state_code) == state.strip().lower())
        return [c.to_dict() for c in q.limit(limit).all()]

    return app
