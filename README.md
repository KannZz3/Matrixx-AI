# Metrixx-AI

This repository documents a futures market intelligence research and development project for **Metrixx AI**, focusing on futures data analysis, market structure research, and trading signal framework development for the MY DESK market intelligence platform.

## Project Overview

The project focuses on transforming futures, physical commodity, positioning, and intraday market-structure data into structured trading intelligence. The core work centers on data-driven futures analysis, basis sentiment interpretation, order-flow behavior, and market structure signal construction.

The project emphasizes the analytical layer behind market intelligence generation: identifying actionable trading signals, defining market structure logic, validating commodity data inputs, and converting futures-market observations into standardized intelligence outputs.

## Core Areas

- Futures market structure analysis
- Intraday order-flow and volume behavior
- Market Profile / auction-market interpretation
- VWAP, breakout acceptance, absorption, and failed breakout logic
- Candlestick reversal and continuation signal design
- Commodity basis and physical-vs-futures spread analysis
- CFTC COT positioning interpretation
- EIA, USDA, CME settlement, and macro data validation
- MY DESK market intelligence workflow support

## Current Work

Current work includes the development and documentation of trading signal frameworks for:

- **Hanging Man reversal risk**
- **Volume spike impact and breakout validation**
- **VWAP-based institutional benchmark behavior**
- **Breakout acceptance / rejection**
- **Absorption and failed auction conditions**
- **Basis sentiment and commodity positioning analysis**

These frameworks are designed to support concise market intelligence outputs inside the MY DESK platform.

## Basis Sentiment Service

Part of the project supports the **BASIS SENTIMENT SERVICE**, which combines physical commodity data, futures settlement prices, COT positioning, and commodity news context into structured market intelligence.

Relevant data components include:

- EIA energy spot and inventory-related data
- USDA agricultural cash market data
- CFTC COT positioning data
- CME / NYMEX / CBOT futures settlement data
- FRED macro overlays
- Commodity event and narrative signals

The objective is to support a production-style pipeline where normalized market data can be used for basis analysis, sentiment interpretation, and subscriber-facing commodity intelligence.

## Methodology

The project combines futures trading domain knowledge with structured data interpretation. Key concepts include:

- Price acceptance vs. rejection
- Volume expansion and price response
- VWAP hold / reclaim behavior
- POC, VAH, VAL, HVN, and LVN interaction
- Auction imbalance and failed breakout behavior
- COT positioning pressure and commercial / managed money flow
- Basis movement between physical and futures markets

## Planned Updates

This repository will continue to be updated as the project progresses. Future additions may include:

- Additional futures market signal frameworks
- Basis Sentiment Service documentation
- COT scoring and positioning interpretation notes
- Commodity basis and spread analysis examples
- Market Profile / ROOTS methodology notes
- Data source validation notes
- Sample structured market intelligence outputs

## Disclaimer

This repository is for educational and research documentation purposes only. It does not constitute investment advice, trading advice, or a recommendation to buy or sell any financial instrument.
