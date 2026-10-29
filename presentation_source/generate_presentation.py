#!/usr/bin/env python3
"""
SIC Platform & Anti-Fraud Lakehouse — Architecture Presentation Generator
Generates an executive-quality, widescreen (16:9), editorial PowerPoint presentation
based strictly on the project's technical documentation and empirical audit evidence.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

# ==============================================================================
# DESIGN SYSTEM & PALETTE
# ==============================================================================
FONT_HEADING = "Helvetica"
FONT_BODY = "Arial"
FONT_MONO = "Consolas"

# Color Palette (Warm Editorial Architectural Theme)
COLOR_BG = RGBColor(248, 250, 252)          # Slate 50 (Warm Off-White)
COLOR_CARD_BG = RGBColor(255, 255, 255)     # Pure White
COLOR_CARD_BORDER = RGBColor(226, 232, 240) # Slate 200 (Clean Hairline)
COLOR_TEXT_PRIMARY = RGBColor(15, 23, 42)   # Slate 900 (Deep Charcoal)
COLOR_TEXT_MUTED = RGBColor(71, 85, 105)    # Slate 600 (Secondary Body)
COLOR_TEXT_LIGHT = RGBColor(148, 163, 184)  # Slate 400 (Subdued Labels)

# Distinct Architectural Accent Colors
COLOR_NAVY = RGBColor(30, 58, 138)          # Blue 900 (Core Backbone / Serving)
COLOR_AMBER = RGBColor(217, 119, 6)         # Amber 600 (Real-Time Streams / Path 1)
COLOR_TEAL = RGBColor(13, 148, 136)         # Teal 600 (Lakehouse Storage / S3)
COLOR_ROSE = RGBColor(225, 29, 72)          # Rose 600 (Fraud Engine / Alerts)
COLOR_INDIGO = RGBColor(79, 70, 229)        # Indigo 600 (CDC / Relational Entities)
COLOR_SLATE_TAG = RGBColor(241, 245, 249)   # Slate 100 (Badge Background)


def create_deck() -> Presentation:
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    return prs


def add_base_slide(prs: Presentation, slide_num: int | None = None) -> any:
    """Creates a blank slide with warm background and optional editorial footer."""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    
    # Background fill
    bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, prs.slide_height)
    bg.fill.solid()
    bg.fill.fore_color.rgb = COLOR_BG
    bg.line.fill.background()
    
    # Footer (if not cover)
    if slide_num is not None:
        footer_box = slide.shapes.add_textbox(Inches(0.8), Inches(7.05), Inches(11.733), Inches(0.35))
        tf = footer_box.text_frame
        tf.word_wrap = True
        tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0
        p = tf.paragraphs[0]
        p.text = f"SIC8 Platform Architecture · Real-Time Payment Ingestion & Anti-Fraud Lakehouse    |    Slide {slide_num:02d}"
        p.font.name = FONT_BODY
        p.font.size = Pt(10)
        p.font.color.rgb = COLOR_TEXT_LIGHT
        
    return slide


def add_header(slide, category: str, title: str, subtitle: str | None = None, cat_color: RGBColor = COLOR_NAVY):
    """Adds a standard editorial header with kicker tag, strong headline, and optional context."""
    header_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.55), Inches(11.733), Inches(1.15))
    tf = header_box.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0
    
    # Category Kicker
    p_cat = tf.paragraphs[0]
    p_cat.text = category.upper()
    p_cat.font.name = FONT_BODY
    p_cat.font.size = Pt(10)
    p_cat.font.bold = True
    p_cat.font.color.rgb = cat_color
    p_cat.space_after = Pt(4)
    
    # Title
    p_title = tf.add_paragraph()
    p_title.text = title
    p_title.font.name = FONT_HEADING
    p_title.font.size = Pt(22)
    p_title.font.bold = True
    p_title.font.color.rgb = COLOR_TEXT_PRIMARY
    
    # Subtitle
    if subtitle:
        p_sub = tf.add_paragraph()
        p_sub.text = subtitle
        p_sub.font.name = FONT_BODY
        p_sub.font.size = Pt(13)
        p_sub.font.color.rgb = COLOR_TEXT_MUTED
        p_sub.space_before = Pt(3)


def add_card(slide, left: Inches, top: Inches, width: Inches, height: Inches, 
             bg_color: RGBColor = COLOR_CARD_BG, border_color: RGBColor = COLOR_CARD_BORDER):
    """Creates a content card container with crisp styling."""
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = bg_color
    shape.line.color.rgb = border_color
    shape.line.width = Pt(1)
    return shape


def add_pill_tag(slide, text: str, left: Inches, top: Inches, width: Inches, height: Inches,
                 bg_color: RGBColor, text_color: RGBColor):
    """Creates a small status or identifier pill badge."""
    pill = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    pill.fill.solid()
    pill.fill.fore_color.rgb = bg_color
    pill.line.fill.background()
    tf = pill.text_frame
    tf.word_wrap = False
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    p = tf.paragraphs[0]
    p.text = text
    p.alignment = PP_ALIGN.CENTER
    p.font.name = FONT_MONO
    p.font.size = Pt(9)
    p.font.bold = True
    p.font.color.rgb = text_color
    return pill


def add_metric_box(slide, left: Inches, top: Inches, width: Inches, height: Inches,
                   value: str, label: str, sublabel: str | None = None, accent_color: RGBColor = COLOR_NAVY):
    """Creates a high-impact metric KPI card."""
    card = add_card(slide, left, top, width, height)
    
    # Accent top indicator
    indicator = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, left, top, width, Inches(0.06))
    indicator.fill.solid()
    indicator.fill.fore_color.rgb = accent_color
    indicator.line.fill.background()
    
    box = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.18), width - Inches(0.4), height - Inches(0.25))
    tf = box.text_frame
    tf.word_wrap = True
    tf.margin_left = tf.margin_top = tf.margin_right = tf.margin_bottom = 0
    
    p_val = tf.paragraphs[0]
    p_val.text = value
    p_val.font.name = FONT_HEADING
    p_val.font.size = Pt(28)
    p_val.font.bold = True
    p_val.font.color.rgb = accent_color
    
    p_lbl = tf.add_paragraph()
    p_lbl.text = label
    p_lbl.font.name = FONT_BODY
    p_lbl.font.size = Pt(12)
    p_lbl.font.bold = True
    p_lbl.font.color.rgb = COLOR_TEXT_PRIMARY
    p_lbl.space_before = Pt(4)
    
    if sublabel:
        p_sub = tf.add_paragraph()
        p_sub.text = sublabel
        p_sub.font.name = FONT_BODY
        p_sub.font.size = Pt(10)
        p_sub.font.color.rgb = COLOR_TEXT_MUTED
        p_sub.space_before = Pt(2)


# ==============================================================================
# SLIDE BUILDERS (22 SLIDES)
# ==============================================================================

def build_slide_01_cover(prs: Presentation):
    """Slide 1: Title & Executive Cover."""
    slide = add_base_slide(prs, None)
    
    # Left Hero Accent Bar
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(1.8), Inches(0.12), Inches(3.6))
    bar.fill.solid()
    bar.fill.fore_color.rgb = COLOR_AMBER
    bar.line.fill.background()
    
    # Main Title Box
    title_box = slide.shapes.add_textbox(Inches(1.2), Inches(1.75), Inches(11.3), Inches(3.8))
    tf = title_box.text_frame
    tf.word_wrap = True
    
    p_kicker = tf.paragraphs[0]
    p_kicker.text = "SAMSUNG INNOVATION CAMPUS  ·  BIG DATA ENGINEERING CAPSTONE"
    p_kicker.font.name = FONT_BODY
    p_kicker.font.size = Pt(12)
    p_kicker.font.bold = True
    p_kicker.font.color.rgb = COLOR_AMBER
    p_kicker.space_after = Pt(14)
    
    p_title = tf.add_paragraph()
    p_title.text = "SIC Financial Ingestion Pipeline\n& Anti-Fraud Medallion Lakehouse"
    p_title.font.name = FONT_HEADING
    p_title.font.size = Pt(36)
    p_title.font.bold = True
    p_title.font.color.rgb = COLOR_TEXT_PRIMARY
    p_title.space_after = Pt(14)
    
    p_sub = tf.add_paragraph()
    p_sub.text = "Enterprise Zero-DB Streaming Ingestion, PostgreSQL CDC via Debezium,\nSub-10ms Online LightGBM Fraud Inference, and Multi-Tier Databricks Lakehouse Architecture."
    p_sub.font.name = FONT_BODY
    p_sub.font.size = Pt(16)
    p_sub.font.color.rgb = COLOR_TEXT_MUTED
    
    # Bottom Metadata Cards
    coords = [
        ("ARCHITECTURE DOMAIN", "Real-Time FinTech Data Engineering"),
        ("THROUGHPUT SLA", "10–100+ TPS (Zero-DB Stream)"),
        ("FRAUD SCORING SLA", "Sub-10ms Inference (34 Features)"),
        ("VERIFIED QUALITY", "168 Automated Tests Passing (100%)"),
    ]
    card_w = Inches(2.75)
    for i, (k, v) in enumerate(coords):
        cx = Inches(0.8) + i * Inches(2.95)
        add_card(slide, cx, Inches(5.7), card_w, Inches(1.0))
        tb = slide.shapes.add_textbox(cx + Inches(0.15), Inches(5.8), card_w - Inches(0.3), Inches(0.8))
        tf_c = tb.text_frame
        tf_c.word_wrap = True
        p1 = tf_c.paragraphs[0]
        p1.text = k
        p1.font.name = FONT_BODY
        p1.font.size = Pt(9)
        p1.font.bold = True
        p1.font.color.rgb = COLOR_NAVY
        p2 = tf_c.add_paragraph()
        p2.text = v
        p2.font.name = FONT_HEADING
        p2.font.size = Pt(11)
        p2.font.bold = True
        p2.font.color.rgb = COLOR_TEXT_PRIMARY
        p2.space_before = Pt(3)


def build_slide_02_problem_and_invariants(prs: Presentation):
    """Slide 2: Banking Problem & Architectural Invariants."""
    slide = add_base_slide(prs, 2)
    add_header(slide, "1. Business Context & Engineering Invariants", 
               "Solving Financial Streaming Ingestion & Fraud Detection at Scale",
               "Why traditional monolithic database ingestion fails under high-velocity financial streams.")
    
    # Left Column: The 3 Core Banking Dilemmas
    add_card(slide, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_left = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_l = tb_left.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "THE THREE BANKING DILEMMAS"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_ROSE
    p.space_after = Pt(12)
    
    dilemmas = [
        ("1. Regulatory Privacy vs Extreme Imbalance",
         "PCI-DSS & GDPR prohibit using real credit card data in testing. Meanwhile, real-world fraud is extremely rare (0.1%–0.2%), requiring controlled synthetic anomaly generation."),
        ("2. The Database Write Bottleneck (Lock Contention)",
         "Writing 100+ transactions/sec into relational databases causes lock contention, transaction log spikes, and I/O saturation. High-velocity streams must bypass the database."),
        ("3. Strict Real-Time Decision SLA (<10ms)",
         "Fraud decisions must occur before POS/E-commerce authorization. Batch processing cannot prevent active card skimming, synthetic bursts, or impossible velocity anomalies.")
    ]
    for title, desc in dilemmas:
        p_t = tf_l.add_paragraph()
        p_t.text = title
        p_t.font.name = FONT_HEADING
        p_t.font.size = Pt(13)
        p_t.font.bold = True
        p_t.font.color.rgb = COLOR_TEXT_PRIMARY
        p_t.space_before = Pt(8)
        
        p_d = tf_l.add_paragraph()
        p_d.text = desc
        p_d.font.name = FONT_BODY
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = COLOR_TEXT_MUTED
        p_d.space_before = Pt(2)
        
    # Right Column: The 5 Core Invariants
    add_card(slide, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_right = slide.shapes.add_textbox(Inches(7.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_r = tb_right.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "SIC PLATFORM CORE ARCHITECTURAL INVARIANTS"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_TEAL
    p_r.space_after = Pt(10)
    
    invariants = [
        ("1. Zero-DB-Inserts for Stream", "Live payments stream directly to Kafka (MSK TCP 9092). Zero writes to PostgreSQL."),
        ("2. Sub-10ms Fraud Decision", "Online feature hydration via Redis (<2ms) and LightGBM evaluation (<0.5ms)."),
        ("3. Automated CDC Demographics", "User & Card account changes captured strictly via PostgreSQL WAL logical decoding."),
        ("4. Bronze Lakehouse Landing", "All events land in S3 Bronze as gzip-compressed JSON Lines or Snappy Parquet."),
        ("5. Resilient Offset Guard", "No auto-commit in Kafka; offset committed only after downstream alert emission.")
    ]
    for title, desc in invariants:
        p_it = tf_r.add_paragraph()
        p_it.text = title
        p_it.font.name = FONT_HEADING
        p_it.font.size = Pt(12)
        p_it.font.bold = True
        p_it.font.color.rgb = COLOR_TEXT_PRIMARY
        p_it.space_before = Pt(6)
        
        p_id = tf_r.add_paragraph()
        p_id.text = desc
        p_id.font.name = FONT_BODY
        p_id.font.size = Pt(11)
        p_id.font.color.rgb = COLOR_TEXT_MUTED
        p_id.space_before = Pt(1)


def build_slide_03_architecture_overview(prs: Presentation):
    """Slide 3: High-Level End-to-End System Architecture."""
    slide = add_base_slide(prs, 3)
    add_header(slide, "2. System Overview", 
               "End-to-End System Architecture & Component Topology",
               "Five decoupled functional tiers delivering ingestion, change data capture, real-time ML, and lakehouse storage.")
    
    # 5 Architectural Blocks horizontally
    tiers = [
        ("1. PRODUCERS", "FastAPI Service\nRateController (10-100 TPS)\nDynamic Demographics (10%)\nScenario Injection Engine", COLOR_AMBER, Inches(0.8)),
        ("2. OLTP STORAGE", "Amazon RDS PostgreSQL 18\nwal_level = logical\nTables: users, cards, mcc, cities\nACID Consistency Boundary", COLOR_INDIGO, Inches(3.2)),
        ("3. EVENT STREAMING", "Amazon MSK (Apache Kafka)\nTopics: transactions, users, cards\nKafka Connect on ECS Fargate\nDebezium + S3 Sink Connectors", COLOR_NAVY, Inches(5.6)),
        ("4. REAL-TIME ML", "AWS ElastiCache Redis\n8,346 User Profiles + Windows\nFraud Consumer (LightGBM v2)\nSub-10ms Alerting & DLQ", COLOR_ROSE, Inches(8.0)),
        ("5. LAKEHOUSE S3", "Amazon S3: sic-pipeline-storage\nBronze: json.gz & parquet\nSilver Delta Deduplication\nGold Facts & Power BI Marts", COLOR_TEAL, Inches(10.4))
    ]
    
    for title, desc, col, x_pos in tiers:
        card = add_card(slide, x_pos, Inches(1.9), Inches(2.15), Inches(4.0))
        
        # Color bar top
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x_pos, Inches(1.9), Inches(2.15), Inches(0.08))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x_pos + Inches(0.12), Inches(2.05), Inches(1.9), Inches(3.7))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_BODY
        p.font.size = Pt(11)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for line in desc.split("\n"):
            p_l = tf.add_paragraph()
            p_l.text = "• " + line
            p_l.font.name = FONT_BODY
            p_l.font.size = Pt(10)
            p_l.font.color.rgb = COLOR_TEXT_PRIMARY
            p_l.space_before = Pt(4)

    # Bottom summary callout card
    add_card(slide, Inches(0.8), Inches(6.05), Inches(11.733), Inches(0.8), bg_color=COLOR_SLATE_TAG)
    tb_b = slide.shapes.add_textbox(Inches(1.0), Inches(6.12), Inches(11.3), Inches(0.65))
    tf_b = tb_b.text_frame
    tf_b.word_wrap = True
    p_b = tf_b.paragraphs[0]
    p_b.text = "CORE DATAFLOW PRINCIPLE: Transactions bypass relational storage entirely (Producers → MSK → S3 Bronze & Real-Time ML), while User/Card demographics use RDS PostgreSQL for relational integrity, mirrored downstream via Debezium CDC without impacting streaming throughput."
    p_b.font.name = FONT_BODY
    p_b.font.size = Pt(10.5)
    p_b.font.color.rgb = COLOR_TEXT_PRIMARY


def build_slide_04_four_data_paths(prs: Presentation):
    """Slide 4: The Four Distinct Ingestion Paths."""
    slide = add_base_slide(prs, 4)
    add_header(slide, "3. Multi-Path Ingestion Design", 
               "The Four Specialized Data Ingestion & Serving Paths",
               "Tailoring protocol, latency, compression, and execution model to the nature of each financial data entity.")
    
    paths = [
        ("PATH 1: High-Speed Streaming", "Financial Transactions", COLOR_AMBER, 
         "Generator → TCP Port 9092 → Amazon MSK",
         "Pushes 10–100+ TPS directly into Kafka 'transactions' topic. Zero DB queries. Micro-batched via Aiven S3 Sink to S3 Bronze json.gz.", Inches(0.8), Inches(1.85)),
        ("PATH 2: Relational CDC", "User & Card Demographics", COLOR_INDIGO,
         "Generator → RDS PostgreSQL → Debezium CDC → MSK",
         "Captures account lifecycle events via PostgreSQL WAL logical decoding. Non-blocking CDC stream lands in bronze/users and bronze/cards.", Inches(6.8), Inches(1.85)),
        ("PATH 3: Serverless Batch", "Merchant Codes & Cities Reference", COLOR_TEAL,
         "EventBridge / Airflow → Fargate Extractor → S3 Parquet",
         "Daily automated snapshot of static reference tables directly from RDS. Bypasses Kafka to avoid broker costs; stores Snappy Parquet.", Inches(0.8), Inches(4.45)),
        ("PATH 4: Real-Time Fraud ML", "Inference & Instant Alerting", COLOR_ROSE,
         "MSK 'transactions' → Fraud Consumer → Redis & 'fraud_alerts'",
         "Sub-10ms consumer hydrates 34 features from Redis, evaluates LightGBM v2 model, dispatches alerts, and routes unparseable events to DLQ.", Inches(6.8), Inches(4.45)),
    ]
    
    for title, subtitle, col, route, desc, x, y in paths:
        card = add_card(slide, x, y, Inches(5.7), Inches(2.45))
        
        # Color bar left
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.08), Inches(2.45))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.2), y + Inches(0.15), Inches(5.3), Inches(2.2))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p1 = tf.paragraphs[0]
        p1.text = title.upper()
        p1.font.name = FONT_BODY
        p1.font.size = Pt(11)
        p1.font.bold = True
        p1.font.color.rgb = col
        
        p2 = tf.add_paragraph()
        p2.text = subtitle
        p2.font.name = FONT_HEADING
        p2.font.size = Pt(15)
        p2.font.bold = True
        p2.font.color.rgb = COLOR_TEXT_PRIMARY
        p2.space_before = Pt(2)
        
        # Route Pill
        p3 = tf.add_paragraph()
        p3.text = f"ROUTE: {route}"
        p3.font.name = FONT_MONO
        p3.font.size = Pt(9.5)
        p3.font.bold = True
        p3.font.color.rgb = COLOR_NAVY
        p3.space_before = Pt(6)
        
        p4 = tf.add_paragraph()
        p4.text = desc
        p4.font.name = FONT_BODY
        p4.font.size = Pt(11)
        p4.font.color.rgb = COLOR_TEXT_MUTED
        p4.space_before = Pt(4)


def build_slide_05_path_1_streaming(prs: Presentation):
    """Slide 5: Path 1 Deep Dive — Zero-DB Streaming."""
    slide = add_base_slide(prs, 5)
    add_header(slide, "4. Path 1 Deep Dive", 
               "Direct Stream Ingestion: Producer Pacing, Envelope & Keying",
               "Achieving high-throughput payment streaming without relational database locks or serialization bottlenecks.")
    
    # 3 Cards Layout
    col_w = Inches(3.65)
    cards_data = [
        ("RateController & Pacing", COLOR_AMBER, Inches(0.8), [
            ("Configurable Throughput", "Maintains calibrated 10.0 to 100+ TPS using floating point sleep intervals."),
            ("Burst Injection", "Simulates e-commerce flash sales, POS peak traffic, and ATM cash-out anomalies."),
            ("Zero Database Dependency", "Operates entirely in-memory using local data profiles; never queries PostgreSQL."),
            ("Worker Architecture", "Modular background thread pool with graceful shutdown and health inspection APIs.")
        ]),
        ("Kafka Keying & Partitioning", COLOR_NAVY, Inches(4.85), [
            ("User ID as Message Key", "Events keyed by account/user_id ensuring strict per-user FIFO order in Kafka."),
            ("Partition Locality", "Single-account events always route to the exact same Kafka partition."),
            ("Parallel Consumption", "Consumer groups scale horizontally across partitions without state races."),
            ("Cluster Topology", "Amazon MSK Kafka 3.8.1 with multi-broker replication and TLS encryption in-flight.")
        ]),
        ("Event Envelope Schema", COLOR_TEAL, Inches(8.9), [
            ("Standard Envelope v1", "Encapsulates payload inside metadata: event_id, event_type, timestamp, schema_version."),
            ("Ground Truth Separation", "Fraud simulation truth kept in sidecars; strictly isolated from payload features."),
            ("Payload Serialization", "High-performance UTF-8 JSON serialization matching production banking schemas."),
            ("Client Resilience", "Confluent-Kafka C-binding with buffer queues and non-blocking retry callbacks.")
        ]),
    ]
    
    for title, col, x, points in cards_data:
        add_card(slide, x, Inches(1.85), col_w, Inches(4.9))
        
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(1.85), col_w, Inches(0.06))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.18), Inches(2.05), col_w - Inches(0.36), Inches(4.5))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for k, v in points:
            p_k = tf.add_paragraph()
            p_k.text = k
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(11)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(8)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(10)
            p_v.font.color.rgb = COLOR_TEXT_MUTED
            p_v.space_before = Pt(2)


def build_slide_06_path_2_cdc(prs: Presentation):
    """Slide 6: Path 2 Deep Dive — Change Data Capture (CDC)."""
    slide = add_base_slide(prs, 6)
    add_header(slide, "5. Path 2 Deep Dive", 
               "Entity Change Data Capture via Debezium & PostgreSQL WAL",
               "Capturing demographic updates with strict ACID boundaries without polling overhead.")
    
    # Left Card: The Technical CDC Flow
    add_card(slide, Inches(0.8), Inches(1.85), Inches(6.0), Inches(4.9))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.6), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "HOW DEBEZIUM CAPTURES WAL CHANGES"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_INDIGO
    p.space_after = Pt(8)
    
    cdc_steps = [
        ("1. Dynamic User/Card Ingestion (10% Ratio)", "Generator writes newly simulated users and cards into Amazon RDS PostgreSQL with relational integrity and foreign keys."),
        ("2. Write-Ahead Log (WAL) Decoding", "PostgreSQL configured with `wal_level = logical`. Engine logs every INSERT/UPDATE/DELETE to disk before flushing data pages."),
        ("3. Debezium PostgreSQL Source Connector", "Runs as an ECS Fargate task inside Kafka Connect. Reads WAL changes directly using the `pgoutput` plugin without table locking."),
        ("4. Streaming Events to Kafka Topics", "Transforms WAL records into Debezium CDC envelopes and publishes to topics: `dbserver1.public.users` and `dbserver1.public.cards`."),
        ("5. S3 Bronze Mirroring", "Aiven S3 Sink connector flushes CDC events to S3 under `bronze/users/` and `bronze/cards/` for downstream SCD Type 2 dimension tracking.")
    ]
    for s_title, s_desc in cdc_steps:
        p_t = tf_l.add_paragraph()
        p_t.text = s_title
        p_t.font.name = FONT_HEADING
        p_t.font.size = Pt(11.5)
        p_t.font.bold = True
        p_t.font.color.rgb = COLOR_TEXT_PRIMARY
        p_t.space_before = Pt(6)
        
        p_d = tf_l.add_paragraph()
        p_d.text = s_desc
        p_d.font.name = FONT_BODY
        p_d.font.size = Pt(10)
        p_d.font.color.rgb = COLOR_TEXT_MUTED
        p_d.space_before = Pt(1)

    # Right Card: Debezium Envelope & Downstream SCD2
    add_card(slide, Inches(7.1), Inches(1.85), Inches(5.433), Inches(4.9))
    tb_r = slide.shapes.add_textbox(Inches(7.3), Inches(2.0), Inches(5.0), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "CDC ENVELOPE & LAKEHOUSE DOWNSTREAM"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_NAVY
    p_r.space_after = Pt(12)
    
    # Debezium sample snippet box
    add_card(slide, Inches(7.3), Inches(2.45), Inches(5.0), Inches(1.8), bg_color=COLOR_SLATE_TAG)
    tb_code = slide.shapes.add_textbox(Inches(7.4), Inches(2.5), Inches(4.8), Inches(1.7))
    tf_c = tb_code.text_frame
    tf_c.word_wrap = True
    p_c = tf_c.paragraphs[0]
    p_c.text = '{\n  "before": null,\n  "after": {"id": 2001, "name": "John Doe", "credit_score": 715},\n  "source": {"table": "users", "lsn": 10827341},\n  "op": "c", "ts_ms": 1729904123000\n}'
    p_c.font.name = FONT_MONO
    p_c.font.size = Pt(9.5)
    p_c.font.color.rgb = COLOR_NAVY
    
    # Additional text below snippet
    tb_info = slide.shapes.add_textbox(Inches(7.3), Inches(4.35), Inches(5.0), Inches(2.2))
    tf_i = tb_info.text_frame
    tf_i.word_wrap = True
    
    benefits = [
        ("Zero Table Locks", "Reads transaction logs directly; OLTP writes remain uninhibited."),
        ("Historical State Auditing", "Every change captures before/after states, enabling Point-in-Time historical feature hydration."),
        ("Lakehouse SCD Type 2", "Databricks Spark jobs read CDC streams to maintain valid_from and valid_to intervals in Delta tables.")
    ]
    for b_title, b_desc in benefits:
        p_bt = tf_i.add_paragraph() if tf_i.paragraphs[0].text else tf_i.paragraphs[0]
        p_bt.text = "• " + b_title + ": " + b_desc
        p_bt.font.name = FONT_BODY
        p_bt.font.size = Pt(10.5)
        p_bt.font.color.rgb = COLOR_TEXT_PRIMARY
        p_bt.space_before = Pt(4)


def build_slide_07_path_3_batch(prs: Presentation):
    """Slide 7: Path 3 Deep Dive — Serverless Batch Reference Ingestion."""
    slide = add_base_slide(prs, 7)
    add_header(slide, "6. Path 3 Deep Dive", 
               "Serverless Batch Ingestion: Reference Data & Parquet Optimization",
               "Why static merchant categories and city tables bypass Kafka to minimize cloud operating costs.")
    
    # 4 Cards Layout
    col_w = Inches(5.7)
    row_h = Inches(2.35)
    
    sections = [
        ("The Problem with Streaming Static Tables", COLOR_ROSE, Inches(0.8), Inches(1.85), [
            ("Wasteful Broker Overhead", "MCC (915 rows) and City coordinates (1,434 rows) are semi-static reference data. Streaming them 24/7 on Kafka generates continuous broker I/O and S3 write costs with zero business value."),
            ("Network Egress & Disk Waste", "Publishing unchanged tables repeatedly congests Kafka broker storage and inflates S3 PUT request bills.")
        ]),
        ("The Serverless Extractor Solution", COLOR_TEAL, Inches(6.8), Inches(1.85), [
            ("Automated EventBridge Scheduling", "Amazon EventBridge (or Airflow DAG 02) triggers an ephemeral AWS ECS Fargate task daily at midnight UTC."),
            ("Direct-to-S3 Lakehouse Push", "The container extracts MCC and Cities directly from RDS PostgreSQL and writes straight to S3, bypassing Kafka entirely.")
        ]),
        ("In-Memory Parquet Conversion", COLOR_AMBER, Inches(0.8), Inches(4.4), [
            ("Snappy-Compressed Parquet", "Data streams into memory, formats with PyArrow, and compresses via Snappy before uploading."),
            ("85%+ Footprint Reduction", "Converts large CSV tables into dense, columnar Parquet files optimized for Databricks and Amazon Athena queries.")
        ]),
        ("Partition Layout & Lakehouse Schema", COLOR_NAVY, Inches(6.8), Inches(4.4), [
            ("Hive-Compatible Partitioning", "Stored under `s3://sic-pipeline-storage/bronze/mcc/snapshot_date=YYYY-MM-DD/mcc_snapshot.parquet`."),
            ("Point-in-Time Schema Versioning", "Preserves immutable snapshots of merchant risk categories and city geolocations across historical pipeline runs.")
        ]),
    ]
    
    for title, col, x, y, bullets in sections:
        card = add_card(slide, x, y, col_w, row_h)
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.06), row_h)
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.2), y + Inches(0.12), col_w - Inches(0.35), row_h - Inches(0.25))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title.upper()
        p.font.name = FONT_HEADING
        p.font.size = Pt(12)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(4)
        
        for k, v in bullets:
            p_k = tf.add_paragraph()
            p_k.text = k + ": "
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(10.5)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(3)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(10)
            p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_08_bronze_storage(prs: Presentation):
    """Slide 8: Data Storage & S3 Bronze Layer."""
    slide = add_base_slide(prs, 8)
    add_header(slide, "7. S3 Storage & Lakehouse Landing", 
               "Immutable Bronze Lakehouse & The Small-Files Problem",
               "Configuring Kafka Connect Aiven S3 Sink to balance latency, object count, and compression efficiency.")
    
    # 3 Metric boxes on top
    add_metric_box(slide, Inches(0.8), Inches(1.85), Inches(3.65), Inches(1.3), "1,000 / 10s", "Flush Thresholds", "Records or max seconds", COLOR_NAVY)
    add_metric_box(slide, Inches(4.85), Inches(1.85), Inches(3.65), Inches(1.3), "85% Space Saved", "Gzip / Snappy Compression", "Optimized cloud storage", COLOR_TEAL)
    add_metric_box(slide, Inches(8.9), Inches(1.85), Inches(3.65), Inches(1.3), "Zero Small-Files", "Compaction Strategy", "Protects query performance", COLOR_AMBER)
    
    # Bottom Comparison: The Small-Files Solution & Partition Structure
    add_card(slide, Inches(0.8), Inches(3.35), Inches(5.7), Inches(3.4))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(3.5), Inches(5.3), Inches(3.1))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "SOLVING THE SMALL-FILES DISASTER"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_ROSE
    p.space_after = Pt(6)
    
    points_l = [
        ("The Disaster:", "Streaming individual events (100 msgs/sec) directly to S3 creates 8.6M micro-files daily. S3 API PUT costs explode, and downstream Spark metadata reading grinds to a halt."),
        ("Micro-Batch Flushing:", "Aiven S3 Sink connector buffers records in memory and triggers writes only when `flush.size=1000` records are accumulated OR `rotate.interval.ms=10000` (10s) expires."),
        ("Gzip Stream Compression:", "Events are compressed on the fly as `json.gz`, reducing S3 storage footprint and network transfer by 85% compared to raw JSON Lines.")
    ]
    for k, v in points_l:
        p_k = tf_l.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(11)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(4)
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        
    add_card(slide, Inches(6.8), Inches(3.35), Inches(5.7), Inches(3.4))
    tb_r = slide.shapes.add_textbox(Inches(7.0), Inches(3.5), Inches(5.3), Inches(3.1))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "HIVE-COMPATIBLE PARTITION LAYOUT"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_NAVY
    p_r.space_after = Pt(6)
    
    points_r = [
        ("s3://sic-pipeline-storage/bronze/transactions/year=YYYY/month=MM/day=DD/", "Contains batched transactions (`.json.gz`). Hive partitions allow Databricks AutoLoader to prune scans efficiently."),
        ("s3://sic-pipeline-storage/bronze/users/ & bronze/cards/", "Contains CDC demographic change events from Debezium with transaction timestamps."),
        ("s3://sic-pipeline-storage/bronze/mcc/ & bronze/cities/", "Daily snapshots formatted as columnar `.snappy.parquet` partitioned by snapshot_date.")
    ]
    for k, v in points_r:
        p_k = tf_r.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_MONO
        p_k.font.size = Pt(9.5)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(4)
        p_v = tf_r.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10)
        p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_09_fraud_detection(prs: Presentation):
    """Slide 9: Real-Time Fraud Detection Pipeline."""
    slide = add_base_slide(prs, 9)
    add_header(slide, "8. Real-Time ML Scoring", 
               "Sub-10ms Fraud Detection Engine & Latency Budget",
               "Hydrating profile context, extracting 34 features, and evaluating LightGBM v2 before payment authorization.")
    
    # Latency Budget Cards on Top
    col_w = Inches(2.75)
    budgets = [
        ("< 1.2 ms", "Redis State Hydration", "User profile & 7d window", COLOR_INDIGO, Inches(0.8)),
        ("< 0.5 ms", "LightGBM v2 Inference", "Fast GBDT decision tree", COLOR_ROSE, Inches(3.75)),
        ("< 3.0 ms", "Feature Transformation", "34 zero-leakage features", COLOR_AMBER, Inches(6.7)),
        ("< 8.0 ms", "Total End-to-End SLA", "Measured on AWS Fargate", COLOR_TEAL, Inches(9.65))
    ]
    for val, lbl, sub, col, x in budgets:
        add_metric_box(slide, x, Inches(1.85), col_w, Inches(1.25), val, lbl, sub, col)
        
    # Flow Diagram / Architecture Steps
    add_card(slide, Inches(0.8), Inches(3.3), Inches(11.6), Inches(3.5))
    tb = slide.shapes.add_textbox(Inches(1.0), Inches(3.45), Inches(11.2), Inches(3.2))
    tf = tb.text_frame
    tf.word_wrap = True
    
    p = tf.paragraphs[0]
    p.text = "REAL-TIME SCORING WORKFLOW & GROUND TRUTH ISOLATION"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_ROSE
    p.space_after = Pt(6)
    
    steps = [
        ("1. Stream Consumption", "Kafka consumer polls 'transactions' topic with manual offset control (enable.auto.commit=false)."),
        ("2. Online State Hydration", "Queries ElastiCache Redis for user profile (age, credit score) and rolling card velocity aggregates (1h/24h/7d)."),
        ("3. 34-Feature Vector Extraction", "FeatureExtractor computes geographic distance, entry method flags, temporal cyclical signals, and risk ratios."),
        ("4. LightGBM v2 Scoring", "Preloaded binary model evaluates tree ensemble and outputs probability score in 0.48ms."),
        ("5. Cost-Calibrated Decision", "Evaluates against optimal threshold 0.0453. If probability >= 0.0453, marks transaction as FRAUD."),
        ("6. Alert Dispatching", "Emits enriched alert JSON to 'fraud_alerts' Kafka topic and commits consumer offset atomically.")
    ]
    for s_title, s_desc in steps:
        p_s = tf.add_paragraph()
        p_s.text = f"{s_title}: "
        p_s.font.name = FONT_HEADING
        p_s.font.size = Pt(11)
        p_s.font.bold = True
        p_s.font.color.rgb = COLOR_TEXT_PRIMARY
        p_s.space_before = Pt(3)
        
        p_d = tf.add_paragraph()
        p_d.text = s_desc
        p_d.font.name = FONT_BODY
        p_d.font.size = Pt(10)
        p_d.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_10_feature_engineering(prs: Presentation):
    """Slide 10: 34-Feature Engineering & Zero-Leakage Contract."""
    slide = add_base_slide(prs, 10)
    add_header(slide, "9. Feature Engineering Contract", 
               "The 34-Feature Registry & Zero-Leakage Equivalence",
               "Strict mathematical feature parity between offline Databricks model training and online streaming inference.")
    
    # 4 Category Cards
    col_w = Inches(5.7)
    row_h = Inches(2.35)
    categories = [
        ("Tier 1: Transaction Features (9 Features)", COLOR_AMBER, Inches(0.8), Inches(1.85), [
            ("Amount & Normalized Log Amount", "Raw signed amount and log-transformed spend magnitude."),
            ("Cyclical Temporal Encodings", "Sin/Cos transformations of transaction hour (0-23) and day of week (0-6)."),
            ("Channel & Entry Method One-Hot", "Chips, contactless POS, online e-commerce, and PIN ATM flags.")
        ]),
        ("Tier 2: Demographic Profiles (7 Features)", COLOR_INDIGO, Inches(6.8), Inches(1.85), [
            ("User Attributes from Redis", "User current age, credit score, total credit cards owned, household size."),
            ("Financial Capacity Ratios", "Total debt, annual per-capita income, and debt-to-income leverage ratio.")
        ]),
        ("Tier 3: Sliding Window Velocity (11 Features)", COLOR_ROSE, Inches(0.8), Inches(4.4), [
            ("Rolling Spend Aggregates", "Card total spending in last 1 hour, 24 hours, and 7 days (Redis ZSET)."),
            ("Velocity Spike Multipliers", "Current amount divided by 24h average spend and 7d average spend."),
            ("Rolling Standard Deviation", "Online variance tracking detecting sudden abnormal spend deviations.")
        ]),
        ("Tier 4: Merchant & Spatial Risk (7 Features)", COLOR_TEAL, Inches(6.8), Inches(4.4), [
            ("MCC Historical Risk Target Encoding", "Empirical fraud probability per Merchant Category Code with Laplace smoothing."),
            ("Haversine Geographic Distance", "Distance (km) between merchant coordinates and user registered home address."),
            ("Impossible Speed Indicator", "Speed (km/h) between successive transactions; flags >800 km/h impossible travel.")
        ]),
    ]
    
    for title, col, x, y, feats in categories:
        card = add_card(slide, x, y, col_w, row_h)
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.06), row_h)
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.2), y + Inches(0.12), col_w - Inches(0.35), row_h - Inches(0.25))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title.upper()
        p.font.name = FONT_HEADING
        p.font.size = Pt(12)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(4)
        
        for k, v in feats:
            p_k = tf.add_paragraph()
            p_k.text = k + ": "
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(10)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(2)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(9.5)
            p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_11_redis_state_store(prs: Presentation):
    """Slide 11: Redis Online State Store & In-Memory Fallback."""
    slide = add_base_slide(prs, 11)
    add_header(slide, "10. State Store Architecture", 
               "Low-Latency Feature Store: ElastiCache Redis & Resilient Fallback",
               "Managing 8,346 preloaded user baselines and sliding window spend aggregates with zero single-point-of-failure.")
    
    # Left Card: Redis Topology & Sliding Window Math
    add_card(slide, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "AWS ELASTICACHE REDIS TOPOLOGY"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_ROSE
    p.space_after = Pt(8)
    
    redis_details = [
        ("Microsecond Latency", "AWS ElastiCache for Redis (cache.t4g.micro) located in the same private subnet as Fargate consumers; achieves ~1.1ms round-trip via TLS."),
        ("Preloaded Profile Cache", "8,346 user demographics and card profiles preloaded during container warm-up from RDS reference snapshots."),
        ("Sliding Window Sorted Sets (ZSET)", "Every transaction recorded as `ZADD user:{id}:tx {timestamp} {amount}`. Rolling 1h, 24h, and 7d sums calculated via `ZREVRANGEBYSCORE`."),
        ("Automated Memory TTL Pruning", "A 7-day TTL ensures expired transaction timestamps are automatically purged, preventing memory bloat.")
    ]
    for k, v in redis_details:
        p_k = tf_l.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        p_v.space_before = Pt(1)

    # Right Card: Dual-Mode Fallback & Idempotency
    add_card(slide, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_r = slide.shapes.add_textbox(Inches(7.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "DUAL-MODE RESILIENCE & IDEMPOTENCY"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_NAVY
    p_r.space_after = Pt(8)
    
    fallback_details = [
        ("Thread-Safe In-Memory Fallback", "If Redis cluster is unavailable or network drops, StateStore switches seamlessly to a thread-safe local dictionary (`_using_fallback = True`)."),
        ("Zero Downtime Guarantee", "In-memory mode mirrors exact Redis mathematical sliding window logic, ensuring zero dropped payments during infrastructure failovers."),
        ("Offline Development & Testing", "Enables running 168+ automated unit and integration tests locally without spinning up Docker or AWS Redis clusters."),
        ("Duplicate Event Deduplication", "Employs event ID hashing (`SET NX`) to ignore duplicate Kafka redeliveries and prevent double-counting transaction spend.")
    ]
    for k, v in fallback_details:
        p_k = tf_r.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        
        p_v = tf_r.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        p_v.space_before = Pt(1)


def build_slide_12_model_evolution(prs: Presentation):
    """Slide 12: LightGBM Model Evolution (V1 Leakage to V2 Remediated)."""
    slide = add_base_slide(prs, 12)
    add_header(slide, "11. Machine Learning Remediation", 
               "From Flawed V1 Baseline to Leak-Free LightGBM V2",
               "Discovering post-event target leakage, fixing temporal splits, and calibrating business cost thresholds.")
    
    # 2 Comparison Columns (V1 vs V2)
    col_w = Inches(5.7)
    
    # Left: The V1 Flaw
    add_card(slide, Inches(0.8), Inches(1.85), col_w, Inches(4.9))
    bar_l = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.8), Inches(1.85), Inches(0.06), Inches(4.9))
    bar_l.fill.solid()
    bar_l.fill.fore_color.rgb = COLOR_ROSE
    bar_l.line.fill.background()
    
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    p_l = tf_l.paragraphs[0]
    p_l.text = "THE FLAWED V1 BASELINE (HISTORICAL NOTEBOOK)"
    p_l.font.name = FONT_BODY
    p_l.font.size = Pt(11)
    p_l.font.bold = True
    p_l.font.color.rgb = COLOR_ROSE
    p_l.space_after = Pt(8)
    
    v1_points = [
        ("The Illusion of 0.99 AUC", "Original exploratory notebook showed near-perfect 0.99 AUC, creating false confidence in production readiness."),
        ("Post-Event Target Leakage", "Feature engineering used future data in rolling aggregates (aggregating across the entire user history including post-fraud transactions)."),
        ("Random Split Contamination", "Splitting data randomly across time leaked customer fraud patterns from future dates into training partitions."),
        ("Arbitrary 0.50 Threshold", "Default 0.50 cutoff ignored severe class imbalance (0.15% fraud), missing 65% of actual fraud attacks.")
    ]
    for k, v in v1_points:
        p_k = tf_l.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        
    # Right: The V2 Remediated Production Model
    add_card(slide, Inches(6.8), Inches(1.85), col_w, Inches(4.9))
    bar_r = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(6.8), Inches(1.85), Inches(0.06), Inches(4.9))
    bar_r.fill.solid()
    bar_r.fill.fore_color.rgb = COLOR_TEAL
    bar_r.line.fill.background()
    
    tb_r = slide.shapes.add_textbox(Inches(7.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    p_r = tf_r.paragraphs[0]
    p_r.text = "THE VERIFIED V2 PRODUCTION REMEDIATION"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_TEAL
    p_r.space_after = Pt(8)
    
    v2_points = [
        ("Strict Temporal Split (Zero Leakage)", "Trained strictly on historical timestamps < cutoff; evaluated on holdout future timestamps."),
        ("Verified Metrics on Real Test Set", "ROC-AUC 0.8746, Recall 81.18%, PR-AUC 0.6214 on genuine unbalanced payment data."),
        ("Sub-Millisecond Execution (0.48ms)", "LightGBM GBDT evaluated in 0.48ms on single CPU core without GPU acceleration."),
        ("Cost-Optimal Threshold (0.0453)", "Calibrated decision threshold to 0.0453 via financial loss matrix ($10 false positive friction vs $250 fraud loss).")
    ]
    for k, v in v2_points:
        p_k = tf_r.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_r.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_13_dlq_remediation(prs: Presentation):
    """Slide 13: Dead-Letter Queue (DLQ) & Remediation."""
    slide = add_base_slide(prs, 13)
    add_header(slide, "12. Fault Tolerance & Recovery", 
               "Production Dead-Letter Queue (DLQ) Specification",
               "Isolating malformed payloads and preventing poisoning of consumer group partitions.")
    
    # 3 Column Layout
    col_w = Inches(3.65)
    cards = [
        ("1. Malformed Payload Trap", COLOR_ROSE, Inches(0.8), [
            ("Unparseable JSON Trap", "Malformed payloads or unparseable timestamps caught in consumer try/except blocks."),
            ("Schema Contract Violations", "Transactions missing required account_id or card_id quarantined immediately."),
            ("Non-Blocking Execution", "Invalid events never crash the consumer loop or hold up partition offsets for valid payments.")
        ]),
        ("2. Standard DLQ Envelope", COLOR_AMBER, Inches(4.85), [
            ("Topic: transactions_dlq", "Emits structured DLQ envelope with full forensic metadata:"),
            ("error_code & error_reason", "Records exact exception trace (e.g. INVALID_TIMESTAMP_FORMAT)."),
            ("original_payload", "Preserves untouched raw payload bytes to enable root-cause debugging."),
            ("retry_count & timestamp", "Tracks failure epoch to coordinate automated retry backoffs.")
        ]),
        ("3. Databricks Redrive Runbook", COLOR_TEAL, Inches(8.9), [
            ("Notebook 08C Integration", "Databricks notebook `08C_Streaming_DLQ_Remediation_and_Redrive.ipynb`."),
            ("Quarantine S3 Inspection", "AutoLoader reads `bronze/quarantine/` allowing engineers to query poison messages."),
            ("Schema Repair & Replay", "Engineers apply patch transforms and redrive repaired events back into Kafka.")
        ]),
    ]
    
    for title, col, x, points in cards:
        add_card(slide, x, Inches(1.85), col_w, Inches(4.9))
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(1.85), col_w, Inches(0.06))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.18), Inches(2.05), col_w - Inches(0.36), Inches(4.5))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for k, v in points:
            p_k = tf.add_paragraph()
            p_k.text = k
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(11)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(8)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(10)
            p_v.font.color.rgb = COLOR_TEXT_MUTED
            p_v.space_before = Pt(2)


def build_slide_14_offset_management(prs: Presentation):
    """Slide 14: Offset Management & At-Least-Once Guarantees."""
    slide = add_base_slide(prs, 14)
    add_header(slide, "13. Delivery Guarantees", 
               "Offset Management & At-Least-Once Delivery Guarantees",
               "Preventing data loss and handling container crashes without corrupting state aggregates.")
    
    add_card(slide, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "MANUAL OFFSET COMMIT (ZERO MESSAGE LOSS)"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_NAVY
    p.space_after = Pt(8)
    
    offset_points = [
        ("Disabling Auto-Commit", "Default `enable.auto.commit = true` commits offsets asynchronously on a timer, causing message loss if the container crashes during scoring. Set strictly to `false`."),
        ("Two-Phase Processing", "The consumer only invokes `consumer.commit()` AFTER successfully emitting the fraud alert to Kafka or S3."),
        ("Crash & Recovery Semantics", "If the consumer crashes midway, uncommitted transactions are re-polled by the rebalanced consumer."),
        ("Strict At-Least-Once Guarantee", "Ensures zero un-evaluated transactions slip through during cloud network hiccups or node updates.")
    ]
    for k, v in offset_points:
        p_k = tf_l.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        
    add_card(slide, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_r = slide.shapes.add_textbox(Inches(7.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "IDEMPOTENT DEDUPLICATION UNDER RETRIES"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_TEAL
    p_r.space_after = Pt(8)
    
    idempotent_points = [
        ("The Duplicate State Hazard", "Under at-least-once delivery, re-delivered transactions could inflate 24h rolling spend totals in Redis."),
        ("Atomic SET NX Verification", "StateStore records `event_id` in Redis via atomic `SETNX event:{id} 1 EX 86400`. If key exists, aggregate update is skipped."),
        ("Idempotent Delta MERGE", "Downstream Databricks jobs use `MERGE INTO silver_transactions USING ... ON target.event_id = source.event_id`."),
        ("Verified by Regression Tests", "Tested by `test_duplicate_event_handling_idempotency`: duplicate UUID re-deliveries leave rolling sums unchanged.")
    ]
    for k, v in idempotent_points:
        p_k = tf_r.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_r.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_15_databricks_medallion(prs: Presentation):
    """Slide 15: Medallion Lakehouse on Databricks."""
    slide = add_base_slide(prs, 15)
    add_header(slide, "14. Downstream Lakehouse", 
               "Medallion Lakehouse on Databricks: Bronze → Silver → Gold",
               "Continuous streaming AutoLoader, Delta Lake deduplication, refund reconciliation, and Gold analytics.")
    
    col_w = Inches(3.65)
    layers = [
        ("BRONZE: Raw Lake", COLOR_AMBER, Inches(0.8), [
            ("Databricks AutoLoader", "AutoLoader stream reads `bronze/transactions/` and `bronze/quarantine/` with cloudFiles format."),
            ("Schema Evolution v3.1", "Auto-detects JSON string vs native object payloads without schema freeze crashes."),
            ("Immutable Audit Storage", "Preserves exact unedited raw messages for compliance and historical replay.")
        ]),
        ("SILVER: Validated Delta", COLOR_NAVY, Inches(4.85), [
            ("Deduplication via MERGE", "Eliminates duplicate deliveries using `event_id` unique keys."),
            ("Signed Amount & Refund Fix", "Preserves negative amounts for refunds (excludes refunds from positive velocity rules)."),
            ("SCD Type 2 Dimension Merge", "Merges CDC user and card updates into slowly changing dimensions with valid timestamps.")
        ]),
        ("GOLD: Business Marts", COLOR_TEAL, Inches(8.9), [
            ("Aggregated Fact Tables", "Daily spend facts, merchant fraud rate rollups, and channel volume distributions."),
            ("Dimensional Star Schema", "Clean foreign-key relationships to dim_users and dim_cards."),
            ("Analytical Safe Views", "Read-only SQL views powering Power BI dashboards and the Analyst Portal API.")
        ]),
    ]
    
    for title, col, x, points in layers:
        add_card(slide, x, Inches(1.85), col_w, Inches(4.9))
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(1.85), col_w, Inches(0.06))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.18), Inches(2.05), col_w - Inches(0.36), Inches(4.5))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for k, v in points:
            p_k = tf.add_paragraph()
            p_k.text = k
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(11)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(8)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(10)
            p_v.font.color.rgb = COLOR_TEXT_MUTED
            p_v.space_before = Pt(2)


def build_slide_16_orchestration_airflow(prs: Presentation):
    """Slide 16: Orchestration via Apache Airflow."""
    slide = add_base_slide(prs, 16)
    add_header(slide, "15. Workflow Orchestration", 
               "Orchestration Topology: Apache Airflow & Automated S3 Sync",
               "Coordinating batch reference extractors, ECS Fargate containers, and continuous Databricks jobs.")
    
    # Left Card: Airflow DAGs
    add_card(slide, Inches(0.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "AIRFLOW PRODUCTION DAG ARCHITECTURE"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEAL
    p.space_after = Pt(8)
    
    dag_points = [
        ("dag_01_incremental_ingestion", "Runs on scheduled intervals: checks generator API readiness, validates ECS container health, triggers extraction, and verifies S3 landing."),
        ("dag_02_batch_reference", "Daily midnight DAG: runs serverless Fargate task to snapshot MCC and Cities reference tables into Snappy Parquet on S3."),
        ("AST & Syntax Verification", "Validated by `tests/r1/test_airflow_dags.py`: zero compilation errors, valid schedules, and cycle-free topological graphs."),
        ("ECS Task Definitions", "Standardized container task definition `task_definition_airflow.json` running inside secure private subnets.")
    ]
    for k, v in dag_points:
        p_k = tf_l.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        
    # Right Card: Continuous Runner & S3 Sync
    add_card(slide, Inches(6.8), Inches(1.85), Inches(5.7), Inches(4.9))
    tb_r = slide.shapes.add_textbox(Inches(7.0), Inches(2.0), Inches(5.3), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "AUTOMATED DEPLOYMENT & STREAM RUNNERS"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_NAVY
    p_r.space_after = Pt(8)
    
    runner_points = [
        ("sync_airflow_dags.py Utility", "Automates syncing local DAG scripts to AWS MWAA / S3 bucket `s3://sic-pipeline-storage/airflow/dags/` with MD5 hash validation."),
        ("sic8_continuous_streaming_job.json", "Databricks cluster job definition maintaining 24/7 continuous stream processing with automated cluster restarts."),
        ("EventBridge Schedule Management", "Script `manage_eventbridge_schedules.py` enables/disables cron triggers for ephemeral ingestion tasks dynamically."),
        ("Least-Privilege IAM Execution", "Airflow execution roles restricted to triggering named ECS tasks without broad AWS administrator rights.")
    ]
    for k, v in runner_points:
        p_k = tf_r.add_paragraph()
        p_k.text = k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(6)
        p_v = tf_r.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_17_security_network(prs: Presentation):
    """Slide 17: Platform Security & Network Isolation."""
    slide = add_base_slide(prs, 17)
    add_header(slide, "16. Platform Security", 
               "VPC Network Isolation, Least-Privilege IAM & Zero Secrets",
               "Enterprise security posture ensuring financial stream isolation and strict defense-in-depth.")
    
    col_w = Inches(3.65)
    sec_cards = [
        ("VPC Subnet Isolation", COLOR_INDIGO, Inches(0.8), [
            ("Strict Subnet Segregation", "Public subnet reserved solely for ALBs and NAT gateways."),
            ("Private Compute & Data", "Amazon MSK (9092), RDS PostgreSQL (5432), and Redis (6379) bound strictly to private subnets."),
            ("Security Group Firewalls", "Inbound traffic to brokers and databases allowed only from authorized consumer security groups.")
        ]),
        ("Encryption Everywhere", COLOR_NAVY, Inches(4.85), [
            ("TLS In-Transit", "MSK, PostgreSQL, and Redis enforce TLS 1.3 encryption across all network boundaries."),
            ("KMS Encryption at Rest", "All S3 buckets and EBS volumes encrypted with AWS Key Management Service (KMS)."),
            ("PCI-DSS Data Hygiene", "Zero real credit card CVVs or plain cardholder names ever transit or persist.")
        ]),
        ("Zero Hardcoded Secrets", COLOR_TEAL, Inches(8.9), [
            ("Environment Templates Only", "Repository contains only sanitized `.env.example` with zero plaintext passwords."),
            ("AWS Secrets Manager", "Production credentials injected dynamically via AWS Secrets Manager at container runtime."),
            ("Git Secret Protection", "Validated by GitHub Push Protection & automated regex audits for access tokens.")
        ]),
    ]
    
    for title, col, x, points in sec_cards:
        add_card(slide, x, Inches(1.85), col_w, Inches(4.9))
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(1.85), col_w, Inches(0.06))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.18), Inches(2.05), col_w - Inches(0.36), Inches(4.5))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for k, v in points:
            p_k = tf.add_paragraph()
            p_k.text = k
            p_k.font.name = FONT_BODY
            p_k.font.size = Pt(11)
            p_k.font.bold = True
            p_k.font.color.rgb = COLOR_TEXT_PRIMARY
            p_k.space_before = Pt(8)
            
            p_v = tf.add_paragraph()
            p_v.text = v
            p_v.font.name = FONT_BODY
            p_v.font.size = Pt(10)
            p_v.font.color.rgb = COLOR_TEXT_MUTED
            p_v.space_before = Pt(2)


def build_slide_18_architecture_tradeoffs(prs: Presentation):
    """Slide 18: Architectural Trade-Offs & Decisions."""
    slide = add_base_slide(prs, 18)
    add_header(slide, "17. Architecture Decisions", 
               "Core Engineering Trade-Offs & Rationale (ADRs)",
               "Evaluating competing architectural options and justifying production implementation decisions.")
    
    col_w = Inches(5.7)
    row_h = Inches(2.35)
    tradeoffs = [
        ("Trade-off 1: Zero-DB Streaming vs Unified RDBMS", COLOR_AMBER, Inches(0.8), Inches(1.85),
         "Chosen: Direct MSK Stream  |  Rejected: Storing Transactions in PostgreSQL",
         "Writing 100+ TPS to PostgreSQL caused connection pool exhaustion and lock contention. Direct Kafka streaming delivers unbounded horizontal scalability while preserving sub-10ms latencies."),
        ("Trade-off 2: Redis State Store vs Flink / Kafka Streams", COLOR_ROSE, Inches(6.8), Inches(1.85),
         "Chosen: AWS ElastiCache Redis  |  Rejected: Embedded RocksDB / Flink",
         "Redis enables microsecond profile lookups and external API queryability from the Analyst Portal. Flink/KStreams would add significant operational complexity and cluster costs for our sliding window requirements."),
        ("Trade-off 3: LightGBM vs Deep Learning (Transformers/NN)", COLOR_NAVY, Inches(0.8), Inches(4.4),
         "Chosen: LightGBM Classifier  |  Rejected: PyTorch / Deep Learning Models",
         "LightGBM executes inference in 0.48ms on a low-cost CPU container without GPU dependencies. It handles tabular banking features and missing values natively with high explainability (SHAP values)."),
        ("Trade-off 4: Financial Cost Matrix vs Standard F1 Optimization", COLOR_TEAL, Inches(6.8), Inches(4.4),
         "Chosen: Optimal Cost Cutoff (0.0453)  |  Rejected: Default 0.50 Probability Cutoff",
         "Under 0.15% fraud prevalence, standard 0.50 cutoff missed 65% of fraud. Calibrating threshold to 0.0453 directly minimizes financial loss ($10 customer review friction vs $250 fraud loss)."),
    ]
    
    for title, col, x, y, chosen_txt, desc in tradeoffs:
        card = add_card(slide, x, y, col_w, row_h)
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.06), row_h)
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.2), y + Inches(0.12), col_w - Inches(0.35), row_h - Inches(0.25))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(12)
        p.font.bold = True
        p.font.color.rgb = col
        
        p_c = tf.add_paragraph()
        p_c.text = chosen_txt
        p_c.font.name = FONT_MONO
        p_c.font.size = Pt(9.5)
        p_c.font.bold = True
        p_c.font.color.rgb = COLOR_TEXT_PRIMARY
        p_c.space_before = Pt(3)
        
        p_d = tf.add_paragraph()
        p_d.text = desc
        p_d.font.name = FONT_BODY
        p_d.font.size = Pt(10.5)
        p_d.font.color.rgb = COLOR_TEXT_MUTED
        p_d.space_before = Pt(4)


def build_slide_19_tech_stack_matrix(prs: Presentation):
    """Slide 19: Technology Stack & Responsibility Matrix."""
    slide = add_base_slide(prs, 19)
    add_header(slide, "18. Technology Ecosystem", 
               "Technology Stack & Engineering Responsibilities",
               "Mapping enterprise technologies to their concrete architectural roles within the monorepo.")
    
    col_w = Inches(3.65)
    cols_data = [
        ("Ingestion & Streaming", COLOR_AMBER, Inches(0.8), [
            ("FastAPI & Uvicorn", "Async HTTP control API and generator worker orchestrator."),
            ("Apache Kafka (MSK 3.8.1)", "Distributed multi-broker event streaming backbone."),
            ("Debezium 2.5", "PostgreSQL WAL logical decoding connector on ECS Fargate."),
            ("Aiven S3 Sink Connector", "High-throughput Kafka-to-S3 micro-batch flusher.")
        ]),
        ("Storage & Machine Learning", COLOR_ROSE, Inches(4.85), [
            ("Amazon RDS PostgreSQL 18", "ACID transactional relational demographic store."),
            ("AWS ElastiCache Redis", "Low-latency sub-millisecond online feature store."),
            ("LightGBM v2 GBDT", "High-speed tabular fraud inference engine (<0.5ms)."),
            ("Amazon S3 Bronze", "Immutable raw data lakehouse landing in gzip / Snappy.")
        ]),
        ("Lakehouse & Orchestration", COLOR_TEAL, Inches(8.9), [
            ("Databricks & PySpark", "Distributed Medallion Delta Lake transformation jobs."),
            ("Apache Airflow", "Workflow DAG scheduler coordinating batch and cloud tasks."),
            ("Docker & AWS ECS", "Containerized serverless Fargate execution environments."),
            ("Pytest & Ruff", "Automated regression testing and strict CI code linting.")
        ]),
    ]
    
    for title, col, x, tools in cols_data:
        add_card(slide, x, Inches(1.85), col_w, Inches(4.9))
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, Inches(1.85), col_w, Inches(0.06))
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.18), Inches(2.05), col_w - Inches(0.36), Inches(4.5))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(14)
        p.font.bold = True
        p.font.color.rgb = col
        p.space_after = Pt(10)
        
        for name, role in tools:
            p_n = tf.add_paragraph()
            p_n.text = name
            p_n.font.name = FONT_BODY
            p_n.font.size = Pt(11)
            p_n.font.bold = True
            p_n.font.color.rgb = COLOR_TEXT_PRIMARY
            p_n.space_before = Pt(8)
            
            p_r = tf.add_paragraph()
            p_r.text = role
            p_r.font.name = FONT_BODY
            p_r.font.size = Pt(10)
            p_r.font.color.rgb = COLOR_TEXT_MUTED
            p_r.space_before = Pt(2)


def build_slide_20_validation_tests(prs: Presentation):
    """Slide 20: Empirical Validation & Test Evidence."""
    slide = add_base_slide(prs, 20)
    add_header(slide, "19. Quality Gate & Empirical Evidence", 
               "Automated Test Evidence & Quality Assurance Gate",
               "168 automated tests validating schema contracts, mathematical feature parity, and pipeline integrity.")
    
    # 4 Metric Cards
    add_metric_box(slide, Inches(0.8), Inches(1.85), Inches(2.75), Inches(1.3), "168 Passed", "Automated Tests", "0 Failures / 0 Errors", COLOR_TEAL)
    add_metric_box(slide, Inches(3.75), Inches(1.85), Inches(2.75), Inches(1.3), "100%", "Scenario Detection", "3 / 3 Anomaly Families", COLOR_NAVY)
    add_metric_box(slide, Inches(6.7), Inches(1.85), Inches(2.75), Inches(1.3), "0 Syntax Errs", "Ruff & Compileall", "Clean Python 3.10–3.14", COLOR_AMBER)
    add_metric_box(slide, Inches(9.65), Inches(1.85), Inches(2.75), Inches(1.3), "2.1 Seconds", "Full Suite Runtime", "Ultra-fast developer feedback", COLOR_ROSE)
    
    # Bottom Table: Test Suite Breakdown
    add_card(slide, Inches(0.8), Inches(3.35), Inches(11.6), Inches(3.4))
    tb = slide.shapes.add_textbox(Inches(1.0), Inches(3.5), Inches(11.2), Inches(3.1))
    tf = tb.text_frame
    tf.word_wrap = True
    
    p = tf.paragraphs[0]
    p.text = "TEST SUITE COVERAGE & VERIFIED FUNCTIONALITY"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_TEAL
    p.space_after = Pt(6)
    
    test_rows = [
        ("R1 Generator & APIs (tests/r1/)", "23 Test Files", "Validates rate controller pacing, schema contracts, event envelopes, and FastAPI worker lifecycle."),
        ("ML Feature Parity & Zero Leakage", "test_ml_fraud.py", "Verifies exact 34-feature extraction equivalence between offline batch training and real-time inference."),
        ("StateStore Idempotency & Variance", "test_state_store_parity.py", "Tests Redis sorted set sliding windows, online variance math, and duplicate redelivery handling."),
        ("Airflow DAGs & AST Structure", "test_airflow_dags.py", "Parses DAG ASTs, validating schedules, task dependencies, S3 sync utilities, and zero cycles."),
        ("Offline Demo & Gold SQLite", "make demo-local", "Generates deterministic POS/ATM/E-commerce streams; verifies 100% detection of CNP, Geo, and Micro anomalies.")
    ]
    for cat, suite, desc in test_rows:
        p_row = tf.add_paragraph()
        p_row.text = f"• {cat}  [{suite}]: "
        p_row.font.name = FONT_HEADING
        p_row.font.size = Pt(10.5)
        p_row.font.bold = True
        p_row.font.color.rgb = COLOR_TEXT_PRIMARY
        p_row.space_before = Pt(4)
        
        p_d = tf.add_paragraph()
        p_d.text = "   " + desc
        p_d.font.name = FONT_BODY
        p_d.font.size = Pt(9.5)
        p_d.font.color.rgb = COLOR_TEXT_MUTED


def build_slide_21_production_gotchas(prs: Presentation):
    """Slide 21: Real-World Engineering Gotchas."""
    slide = add_base_slide(prs, 21)
    add_header(slide, "20. Engineering Post-Mortem", 
               "Real-World Production Gotchas & Practical Solutions",
               "Hard-won cloud infrastructure and streaming lessons documented during platform implementation.")
    
    col_w = Inches(5.7)
    row_h = Inches(2.35)
    gotchas = [
        ("Gotcha 1: Aiven S3 Sink Regional STS Failure", COLOR_ROSE, Inches(0.8), Inches(1.85),
         "Symptom: Kafka Connect threw IAM Role Assumption timeouts uploading to S3.",
         "Root Cause: Global AWS STS endpoint timed out from European region (eu-west-3).\nFix: Forced regional STS endpoint in connector config: `aws.sts.regional.endpoints=regional`."),
        ("Gotcha 2: PostgreSQL WAL Storage Swell", COLOR_AMBER, Inches(6.8), Inches(1.85),
         "Symptom: Amazon RDS disk storage surged rapidly when Debezium container paused.",
         "Root Cause: Logical replication slots retain WAL logs on disk until consumer confirms LSN.\nFix: Implemented automated replication slot lag alarms and heartbeat checkpoints."),
        ("Gotcha 3: AWS Account vCPU Quota Limits", COLOR_INDIGO, Inches(0.8), Inches(4.4),
         "Symptom: ECS Fargate service failed to launch required parallel extractor tasks.",
         "Root Cause: Account limit capped at 2.0 vCPUs; default task definition requested 1.0 vCPU.\nFix: Downsized task definition to 0.25 vCPU and 0.5 GB RAM without performance degradation."),
        ("Gotcha 4: Confluent-Kafka Import & CI Scope", COLOR_TEAL, Inches(6.8), Inches(4.4),
         "Symptom: GitHub Actions CI runner failed test collection on machines lacking Kafka C-libs.",
         "Root Cause: Direct top-level `import confluent_kafka` in integration test.\nFix: Wrapped in `pytest.importorskip('confluent_kafka')` allowing clean optional skipping on CI."),
    ]
    
    for title, col, x, y, cause, fix in gotchas:
        card = add_card(slide, x, y, col_w, row_h)
        bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, Inches(0.06), row_h)
        bar.fill.solid()
        bar.fill.fore_color.rgb = col
        bar.line.fill.background()
        
        tb = slide.shapes.add_textbox(x + Inches(0.2), y + Inches(0.12), col_w - Inches(0.35), row_h - Inches(0.25))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p = tf.paragraphs[0]
        p.text = title
        p.font.name = FONT_HEADING
        p.font.size = Pt(12)
        p.font.bold = True
        p.font.color.rgb = col
        
        p_c = tf.add_paragraph()
        p_c.text = cause
        p_c.font.name = FONT_BODY
        p_c.font.size = Pt(10)
        p_c.font.bold = True
        p_c.font.color.rgb = COLOR_TEXT_PRIMARY
        p_c.space_before = Pt(3)
        
        p_f = tf.add_paragraph()
        p_f.text = fix
        p_f.font.name = FONT_BODY
        p_f.font.size = Pt(9.5)
        p_f.font.color.rgb = COLOR_TEXT_MUTED
        p_f.space_before = Pt(2)


def build_slide_22_conclusion(prs: Presentation):
    """Slide 22: Conclusion & Technical Discussion."""
    slide = add_base_slide(prs, 22)
    add_header(slide, "21. Conclusion & Defense", 
               "Architectural Summary & Technical Discussion",
               "A production-ready, empirically verified real-time financial data platform.")
    
    # Left Card: Summary Takeaways
    add_card(slide, Inches(0.8), Inches(1.85), Inches(6.8), Inches(4.9))
    tb_l = slide.shapes.add_textbox(Inches(1.0), Inches(2.0), Inches(6.4), Inches(4.5))
    tf_l = tb_l.text_frame
    tf_l.word_wrap = True
    
    p = tf_l.paragraphs[0]
    p.text = "CORE ENGINEERING ACHIEVEMENTS"
    p.font.name = FONT_BODY
    p.font.size = Pt(11)
    p.font.bold = True
    p.font.color.rgb = COLOR_NAVY
    p.space_after = Pt(10)
    
    achievements = [
        ("Zero-DB High Throughput", "Payment streams completely decoupled from transactional OLTP databases, achieving sustainable 100+ TPS throughput on Amazon MSK."),
        ("Sub-10ms Fraud Decisioning", "Engineered 34-feature extraction contract with ElastiCache Redis (<1.2ms) and LightGBM v2 GBDT (<0.5ms) without target leakage."),
        ("Comprehensive Lakehouse Integration", "Delta Lake Bronze, Silver, and Gold Medallion architecture with automated DLQ quarantine and Airflow DAG scheduling."),
        ("Empirically Verified Monorepo", "168 automated tests passing (100%), verified zero-secret Git safety, and reproducible offline demo harness.")
    ]
    for k, v in achievements:
        p_k = tf_l.add_paragraph()
        p_k.text = "✔ " + k
        p_k.font.name = FONT_HEADING
        p_k.font.size = Pt(12)
        p_k.font.bold = True
        p_k.font.color.rgb = COLOR_TEXT_PRIMARY
        p_k.space_before = Pt(8)
        
        p_v = tf_l.add_paragraph()
        p_v.text = v
        p_v.font.name = FONT_BODY
        p_v.font.size = Pt(10.5)
        p_v.font.color.rgb = COLOR_TEXT_MUTED
        p_v.space_before = Pt(2)
        
    # Right Card: Repository Links & Defense Ready
    add_card(slide, Inches(7.9), Inches(1.85), Inches(4.6), Inches(4.9))
    tb_r = slide.shapes.add_textbox(Inches(8.1), Inches(2.0), Inches(4.2), Inches(4.5))
    tf_r = tb_r.text_frame
    tf_r.word_wrap = True
    
    p_r = tf_r.paragraphs[0]
    p_r.text = "OPEN SOURCE REPOSITORY & DEFENSE"
    p_r.font.name = FONT_BODY
    p_r.font.size = Pt(11)
    p_r.font.bold = True
    p_r.font.color.rgb = COLOR_AMBER
    p_r.space_after = Pt(12)
    
    p_gh = tf_r.add_paragraph()
    p_gh.text = "GITHUB REPOSITORY:"
    p_gh.font.name = FONT_BODY
    p_gh.font.size = Pt(10)
    p_gh.font.bold = True
    p_gh.font.color.rgb = COLOR_TEXT_LIGHT
    
    p_link = tf_r.add_paragraph()
    p_link.text = "github.com/SayedELMASRY2/\nAnti-Fraud-Lakehouse"
    p_link.font.name = FONT_MONO
    p_link.font.size = Pt(11.5)
    p_link.font.bold = True
    p_link.font.color.rgb = COLOR_NAVY
    p_link.space_before = Pt(3)
    p_link.space_after = Pt(14)
    
    p_prompt = tf_r.add_paragraph()
    p_prompt.text = "Q&A / TECHNICAL DISCUSSION:"
    p_prompt.font.name = FONT_BODY
    p_prompt.font.size = Pt(10)
    p_prompt.font.bold = True
    p_prompt.font.color.rgb = COLOR_TEXT_LIGHT
    
    p_qa = tf_r.add_paragraph()
    p_qa.text = "Open for questions on streaming throughput, Redis sliding window math, LightGBM feature parity, or Databricks lakehouse design."
    p_qa.font.name = FONT_BODY
    p_qa.font.size = Pt(11)
    p_qa.font.color.rgb = COLOR_TEXT_MUTED
    p_qa.space_before = Pt(3)


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    repo_root = Path(__file__).resolve().parent.parent
    output_path = repo_root / "SIC_Ingestion_Pipeline_Architecture.pptx"
    
    print(f"Generating presentation at: {output_path}")
    prs = create_deck()
    
    # Build all 22 slides
    build_slide_01_cover(prs)
    build_slide_02_problem_and_invariants(prs)
    build_slide_03_architecture_overview(prs)
    build_slide_04_four_data_paths(prs)
    build_slide_05_path_1_streaming(prs)
    build_slide_06_path_2_cdc(prs)
    build_slide_07_path_3_batch(prs)
    build_slide_08_bronze_storage(prs)
    build_slide_09_fraud_detection(prs)
    build_slide_10_feature_engineering(prs)
    build_slide_11_redis_state_store(prs)
    build_slide_12_model_evolution(prs)
    build_slide_13_dlq_remediation(prs)
    build_slide_14_offset_management(prs)
    build_slide_15_databricks_medallion(prs)
    build_slide_16_orchestration_airflow(prs)
    build_slide_17_security_network(prs)
    build_slide_18_architecture_tradeoffs(prs)
    build_slide_19_tech_stack_matrix(prs)
    build_slide_20_validation_tests(prs)
    build_slide_21_production_gotchas(prs)
    build_slide_22_conclusion(prs)
    
    prs.save(str(output_path))
    print(f"Successfully generated {len(prs.slides)} slides in {output_path}!")


if __name__ == "__main__":
    main()
