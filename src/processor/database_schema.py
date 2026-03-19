"""
src/processor/database_schema.py
---------------------------------
SQLAlchemy ORM models for a person-wise, multi-company, multi-year
shareholder data store.

Design goals
------------
1. One person can hold shares in multiple companies across multiple years
   without false deduplication.
2. Identity key = (name_normalized, pan) if PAN known;
                  (name_normalized, folio_no, company_id) otherwise.
3. Each financial year's snapshot is a separate row in `holding_snapshots`
   → no pivoted FY columns, clean time-series queries.

Tables
------
persons             Core identity record (one row per unique human)
companies           Company master (BSE code, NSE symbol, ISIN)
holdings            Link table: one row per (person × company × folio)
holding_snapshots   Time-series: shares & market_value per holding per FY
dividends           Unclaimed dividend amounts per holding per FY
contacts            Phone/email enrichment data (multiple per person)
pipeline_jobs       Job audit trail (mirrors RQ metadata)
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DB location
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = _ROOT / "data" / "shareholders.db"


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# persons
# ---------------------------------------------------------------------------
class Person(Base):
    """Normalised identity for a unique human shareholder."""

    __tablename__ = "persons"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name_raw = Column(String(300), nullable=False)
    name_normalized = Column(String(300), nullable=False)
    pan = Column(String(10), nullable=True)          # Permanent Account Number (India)
    address = Column(Text, nullable=True)
    identity_hash = Column(String(64), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    holdings = relationship("Holding", back_populates="person", cascade="all, delete-orphan")
    contacts = relationship("Contact", back_populates="person", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_person_name_norm", "name_normalized"),
        Index("ix_person_pan", "pan"),
    )

    def __repr__(self) -> str:
        return f"<Person id={self.id} name={self.name_normalized!r} pan={self.pan!r}>"


# ---------------------------------------------------------------------------
# companies
# ---------------------------------------------------------------------------
class Company(Base):
    """Company master — one row per listed entity."""

    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(300), nullable=False)
    name_normalized = Column(String(300), nullable=False)
    bse_code = Column(String(20), nullable=True)
    nse_symbol = Column(String(20), nullable=True)
    isin = Column(String(12), nullable=True)
    sector = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    holdings = relationship("Holding", back_populates="company")

    __table_args__ = (
        UniqueConstraint("name_normalized", name="uq_company_name"),
        Index("ix_company_isin", "isin"),
    )

    def __repr__(self) -> str:
        return f"<Company id={self.id} name={self.name!r}>"


# ---------------------------------------------------------------------------
# holdings  (person × company × folio)
# ---------------------------------------------------------------------------
class Holding(Base):
    """Link table: one row per (person, company, folio_no) combination.

    A person may have multiple folios in the same company (e.g. joint holders),
    so (person_id, company_id, folio_no) is the natural PK.
    """

    __tablename__ = "holdings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("persons.id", ondelete="CASCADE"), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    folio_no = Column(String(50), nullable=True)
    source_pdf = Column(String(500), nullable=True)   # path or filename of source PDF
    is_joint = Column(Boolean, default=False)         # True if joint holding
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    person = relationship("Person", back_populates="holdings")
    company = relationship("Company", back_populates="holdings")
    snapshots = relationship("HoldingSnapshot", back_populates="holding", cascade="all, delete-orphan")
    dividends = relationship("Dividend", back_populates="holding", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("person_id", "company_id", "folio_no", name="uq_holding"),
        Index("ix_holding_person", "person_id"),
        Index("ix_holding_company", "company_id"),
    )


# ---------------------------------------------------------------------------
# holding_snapshots  (time-series per FY)
# ---------------------------------------------------------------------------
class HoldingSnapshot(Base):
    """Shares held and market value for one financial year.

    Instead of pivoting FYs into columns (which breaks with new years),
    each FY is a separate row.
    """

    __tablename__ = "holding_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    holding_id = Column(Integer, ForeignKey("holdings.id", ondelete="CASCADE"), nullable=False)
    financial_year = Column(String(10), nullable=False)  # e.g. "2023-24"
    shares = Column(Float, nullable=True)
    market_value = Column(Float, nullable=True)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    holding = relationship("Holding", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("holding_id", "financial_year", name="uq_snapshot_fy"),
        Index("ix_snapshot_fy", "financial_year"),
    )


# ---------------------------------------------------------------------------
# dividends
# ---------------------------------------------------------------------------
class Dividend(Base):
    """Unclaimed dividend amount per holding per financial year."""

    __tablename__ = "dividends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    holding_id = Column(Integer, ForeignKey("holdings.id", ondelete="CASCADE"), nullable=False)
    financial_year = Column(String(10), nullable=False)
    amount_rs = Column(Float, nullable=True)
    status = Column(String(30), nullable=True)       # "unclaimed", "transferred_to_iepf", etc.
    recorded_at = Column(DateTime, default=datetime.utcnow)

    holding = relationship("Holding", back_populates="dividends")

    __table_args__ = (
        UniqueConstraint("holding_id", "financial_year", name="uq_dividend_fy"),
    )


# ---------------------------------------------------------------------------
# contacts
# ---------------------------------------------------------------------------
class Contact(Base):
    """Phone/email enrichment results linked to a person."""

    __tablename__ = "contacts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("persons.id", ondelete="CASCADE"), nullable=False)
    mobile = Column(String(15), nullable=True)
    email = Column(String(200), nullable=True)
    source = Column(String(50), nullable=True)           # "inhouse_db", "mca", "apollo"
    enrichment_layer = Column(Integer, nullable=True)    # 1, 2, or 3
    verified = Column(Boolean, default=False)
    verified_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    person = relationship("Person", back_populates="contacts")

    __table_args__ = (
        Index("ix_contact_mobile", "mobile"),
        Index("ix_contact_person", "person_id"),
    )


# ---------------------------------------------------------------------------
# pipeline_jobs  (audit trail)
# ---------------------------------------------------------------------------
class PipelineJob(Base):
    """Audit trail for every submitted background job."""

    __tablename__ = "pipeline_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(100), unique=True, nullable=False)
    job_type = Column(String(50), nullable=True)          # "download", "parse", "full"
    status = Column(String(30), default="queued")
    companies = Column(Text, nullable=True)               # JSON-encoded list
    source = Column(String(10), nullable=True)
    records_extracted = Column(Integer, nullable=True)
    records_after_dedup = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# ---------------------------------------------------------------------------
# Engine + Session factory
# ---------------------------------------------------------------------------
_engine = None
_SessionLocal = None


def get_engine(db_path: str | Path | None = None):
    """Return (creating if needed) the SQLAlchemy engine.

    Args:
        db_path: Path to the SQLite file. Defaults to DEFAULT_DB_PATH.
    """
    global _engine
    if _engine is None:
        path = Path(db_path) if db_path else DEFAULT_DB_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        _engine = create_engine(
            f"sqlite:///{path}",
            connect_args={"check_same_thread": False},
            echo=False,
        )
        _logger.info("SQLAlchemy engine created: %s", path)
    return _engine


def get_session(db_path: str | Path | None = None) -> Session:
    """Return a new SQLAlchemy Session."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine(db_path), autoflush=False, autocommit=False)
    return _SessionLocal()


def create_all_tables(db_path: str | Path | None = None) -> None:
    """Create all tables in the database (safe to call multiple times)."""
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    _logger.info("All tables created/verified")


# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse spaces."""
    n = name.lower().strip()
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _identity_hash(name_normalized: str, pan: str | None, folio: str | None, company_id: int | None) -> str:
    """Return a stable 64-char SHA-256 hex digest for a person's identity."""
    if pan:
        key = f"pan:{pan.upper().strip()}"
    elif folio and company_id:
        key = f"folio:{folio.strip()}:co:{company_id}"
    else:
        key = f"name:{name_normalized}"
    return hashlib.sha256(key.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def get_or_create_company(session: Session, name: str, **kwargs) -> Company:
    """Return an existing Company or create a new one."""
    name_norm = _normalize_name(name)
    company = session.query(Company).filter_by(name_normalized=name_norm).first()
    if company is None:
        company = Company(name=name, name_normalized=name_norm, **kwargs)
        session.add(company)
        session.flush()
    return company


def get_or_create_person(
    session: Session,
    name_raw: str,
    pan: str | None = None,
    folio: str | None = None,
    company_id: int | None = None,
    address: str | None = None,
) -> Person:
    """Return an existing Person matching the identity key, or create a new one.

    Identity resolution:
    1. If PAN is known → identity_hash("pan:{PAN}")
    2. Else if folio+company → identity_hash("folio:{folio}:co:{company_id}")
    3. Else → identity_hash("name:{name_normalized}")
    """
    name_norm = _normalize_name(name_raw)
    ihash = _identity_hash(name_norm, pan, folio, company_id)

    person = session.query(Person).filter_by(identity_hash=ihash).first()
    if person is None:
        person = Person(
            name_raw=name_raw,
            name_normalized=name_norm,
            pan=pan,
            address=address,
            identity_hash=ihash,
        )
        session.add(person)
        session.flush()
    return person


def upsert_holding_snapshot(
    session: Session,
    holding: Holding,
    financial_year: str,
    shares: float | None,
    market_value: float | None,
) -> HoldingSnapshot:
    """Insert or update a holding snapshot for a given FY."""
    snap = (
        session.query(HoldingSnapshot)
        .filter_by(holding_id=holding.id, financial_year=financial_year)
        .first()
    )
    if snap is None:
        snap = HoldingSnapshot(
            holding_id=holding.id,
            financial_year=financial_year,
            shares=shares,
            market_value=market_value,
        )
        session.add(snap)
    else:
        snap.shares = shares
        snap.market_value = market_value
    return snap


def upsert_dataframe_to_db(df, db_path: str | Path | None = None) -> int:
    """Bulk-load a master DataFrame into the person-wise schema.

    Expects columns: full_name, company_name, folio_no, financial_year,
                     current_holding, market_value, total_dividend, [pan], [address].

    Returns:
        Number of person records created or updated.
    """
    import pandas as pd

    create_all_tables(db_path)
    session = get_session(db_path)
    count = 0

    try:
        for _, row in df.iterrows():
            name = str(row.get("full_name", "")).strip()
            company_name = str(row.get("company_name", "")).strip()
            folio = str(row.get("folio_no", "")).strip() or None
            fy = str(row.get("financial_year", "")).strip()
            shares = _safe_float(row.get("current_holding"))
            mv = _safe_float(row.get("market_value"))
            dividend = _safe_float(row.get("total_dividend"))
            pan = str(row.get("pan", "")).strip() or None
            address = str(row.get("address", "")).strip() or None

            if not name or not company_name:
                continue

            company = get_or_create_company(session, company_name)
            person = get_or_create_person(
                session, name, pan=pan, folio=folio,
                company_id=company.id, address=address
            )

            # Holding
            holding = (
                session.query(Holding)
                .filter_by(person_id=person.id, company_id=company.id, folio_no=folio)
                .first()
            )
            if holding is None:
                holding = Holding(person_id=person.id, company_id=company.id, folio_no=folio)
                session.add(holding)
                session.flush()

            # Snapshot
            if fy:
                upsert_holding_snapshot(session, holding, fy, shares, mv)

            # Dividend
            if fy and dividend:
                div = (
                    session.query(Dividend)
                    .filter_by(holding_id=holding.id, financial_year=fy)
                    .first()
                )
                if div is None:
                    div = Dividend(holding_id=holding.id, financial_year=fy, amount_rs=dividend)
                    session.add(div)
                else:
                    div.amount_rs = dividend

            count += 1

        session.commit()
        _logger.info("upsert_dataframe_to_db: %d rows committed", count)
    except Exception:
        session.rollback()
        _logger.exception("upsert_dataframe_to_db failed; rolled back")
        raise
    finally:
        session.close()

    return count


def _safe_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
