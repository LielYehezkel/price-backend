import logging
import time
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.config import settings
from backend.db import engine, init_db
from backend.routers import admin, ai_ops, auth, integrations, plugin, price, shops
from backend.services.monitor_checks import run_scheduled_checks
from backend.services.sales_notifications import send_scheduled_sales_reports
from backend.services.scan_engine_journal import record_tick_failure, record_tick_success
from sqlmodel import Session

scheduler = BackgroundScheduler()
log = logging.getLogger(__name__)


def _scheduled_job() -> None:
    t0 = time.perf_counter()
    try:
        with Session(engine) as session:
            scans, shops_n = run_scheduled_checks(session)
        ms = int((time.perf_counter() - t0) * 1000)
        with Session(engine) as hb_session:
            record_tick_success(hb_session, ms, scans, shops_n)
    except Exception as ex:
        log.exception("scheduled price_checks job failed")
        ms = int((time.perf_counter() - t0) * 1000)
        try:
            with Session(engine) as hb_session:
                record_tick_failure(hb_session, ms, ex)
        except Exception:
            log.exception("failed to record scheduler heartbeat failure")


def _sales_reports_job() -> None:
    try:
        with Session(engine) as session:
            send_scheduled_sales_reports(session)
    except Exception:
        log.exception("scheduled sales reports job failed")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    if not scheduler.running:
        scheduler.add_job(
            _scheduled_job,
            "interval",
            seconds=5,
            id="price_checks",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            _sales_reports_job,
            "interval",
            minutes=1,
            id="sales_reports",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Price Resolver API", lifespan=lifespan)

origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _apply_error_cors_headers(request: Request, response: JSONResponse) -> JSONResponse:
    origin = request.headers.get("origin")
    if not origin:
        return response
    if origins and "*" not in origins and origin not in origins:
        return response
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Access-Control-Allow-Credentials"] = "true"
    vary = response.headers.get("Vary")
    if not vary:
        response.headers["Vary"] = "Origin"
    elif "origin" not in vary.lower():
        response.headers["Vary"] = f"{vary}, Origin"
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    payload = {"detail": exc.detail}
    response = JSONResponse(status_code=exc.status_code, content=payload, headers=exc.headers)
    return _apply_error_cors_headers(request, response)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    response = JSONResponse(status_code=422, content={"detail": exc.errors()})
    return _apply_error_cors_headers(request, response)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled request error: %s %s", request.method, request.url.path)
    response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
    return _apply_error_cors_headers(request, response)

app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(price.router)
app.include_router(shops.router)
app.include_router(ai_ops.router)
app.include_router(integrations.router)
app.include_router(plugin.router)


@app.get("/health")
def health():
    return {"ok": True}
