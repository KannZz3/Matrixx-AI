# Metrixx-AI

This repository documents a futures market intelligence research and development project for **Metrixx AI**, focusing on futures data analysis, market structure research, and trading signal framework development for the MY DESK market intelligence platform.

本仓库记录了我在 **Metrixx AI** 参与的商品期货市场情报研发项目，重点包括期货数据分析、市场结构研究，以及面向 MY DESK 市场情报平台的交易信号框架开发。

---

## Project Overview  
## 项目概览

The project focuses on transforming futures, physical commodity, positioning, and intraday market-structure data into structured trading intelligence. The core work centers on data-driven futures analysis, basis sentiment interpretation, order-flow behavior, and market structure signal construction.

本项目旨在将期货价格、现货商品、持仓结构以及日内市场结构数据转化为结构化交易情报。核心工作包括数据驱动的期货分析、基差情绪解读、订单流行为分析以及市场结构信号构建。

The project emphasizes the analytical layer behind market intelligence generation: identifying actionable trading signals, defining market structure logic, validating commodity data inputs, and converting futures-market observations into standardized intelligence outputs.

项目重点在于构建市场情报背后的分析层：识别可操作的交易信号，定义市场结构逻辑，验证商品数据输入，并将期货市场观察转化为标准化情报输出。

---

## Core Areas  
## 核心方向

- Futures market structure analysis  
  期货市场结构分析

- Intraday order-flow and volume behavior  
  日内订单流与成交量行为分析

- Market Profile / auction-market interpretation  
  Market Profile 与拍卖市场逻辑解读

- VWAP, breakout acceptance, absorption, and failed breakout logic  
  VWAP、突破接受、吸收盘与假突破逻辑

- Candlestick reversal and continuation signal design  
  K 线反转与延续信号设计

- Commodity basis and physical-vs-futures spread analysis  
  商品基差与现货-期货价差分析

- CFTC COT positioning interpretation  
  CFTC COT 持仓结构解读

- EIA, USDA, CME settlement, and macro data validation  
  EIA、USDA、CME 结算价与宏观数据验证

- Reuters / EIA narrative event-flag generation  
  Reuters / EIA 市场叙事事件标记生成

- MY DESK market intelligence workflow support  
  MY DESK 市场情报工作流支持

---

## Current Work  
## 当前工作

Current work includes the development and documentation of trading signal frameworks for:

当前工作包括以下交易信号框架的设计与文档化：

- **Hanging Man reversal risk**  
  **Hanging Man 吊人线反转风险**

- **Volume spike impact and breakout validation**  
  **成交量异常放大影响与突破验证**

- **VWAP-based institutional benchmark behavior**  
  **基于 VWAP 的机构基准行为分析**

- **Breakout acceptance / rejection**  
  **突破接受 / 拒绝判断**

- **Absorption and failed auction conditions**  
  **吸收盘与失败拍卖条件**

- **Basis sentiment and commodity positioning analysis**  
  **基差情绪与商品持仓结构分析**

- **Reuters / EIA narrative sentiment event flags**  
  **Reuters / EIA 市场叙事情绪事件标记**

These frameworks are designed to support concise market intelligence outputs inside the MY DESK platform.

这些框架旨在支持 MY DESK 平台内简洁、标准化的市场情报输出。

---

## Basis Sentiment Service  
## 基差情绪服务

Part of the project supports the **BASIS SENTIMENT SERVICE**, which combines physical commodity data, futures settlement prices, COT positioning, and commodity news context into structured market intelligence.

项目的一部分支持 **BASIS SENTIMENT SERVICE（基差情绪服务）**，该服务将现货商品数据、期货结算价、COT 持仓数据以及商品市场新闻背景整合为结构化市场情报。

Relevant data components include:

相关数据组件包括：

- EIA energy spot and inventory-related data  
  EIA 能源现货与库存相关数据

- USDA agricultural cash market data  
  USDA 农产品现货市场数据

- CFTC COT positioning data  
  CFTC COT 持仓数据

- CME / NYMEX / CBOT futures settlement data  
  CME / NYMEX / CBOT 期货结算价数据

- FRED macro overlays  
  FRED 宏观经济辅助数据

- Commodity event and narrative signals  
  商品事件与市场叙事信号

The Basis Sentiment Service also includes a Reuters / EIA narrative sentiment pipeline for event-flag generation. Using Reuters Commodities RSS and EIA Today in Energy at the headline / snippet level only, the pipeline detects commodity tags, directional bias, and confidence scores before sending paraphrased signals to the Claude preprocessing layer.

基差情绪服务还包括 Reuters / EIA 市场叙事情绪管线，用于生成商品事件标记。该模块仅使用 Reuters Commodities RSS 与 EIA Today in Energy 的 headline / snippet 信息，识别商品标签、方向性影响与置信度评分，并将改写后的信号传递至 Claude 预处理层。

Reuters RSS is treated as review-only: event flagging and paraphrase only, with no full-article copying, no direct reuse of Reuters text, and no Reuters original content in subscriber-facing outputs.

Reuters RSS 按 review-only 方式处理：仅用于事件标记与改写，不复制全文，不直接复用 Reuters 原文，也不将 Reuters 原文放入面向订阅用户的输出。

The objective is to support a production-style pipeline where normalized market data can be used for basis analysis, sentiment interpretation, and subscriber-facing commodity intelligence.

该模块的目标是支持一个接近生产环境的数据管线，使标准化后的市场数据可用于基差分析、情绪解读，以及面向订阅用户的商品市场情报输出。

---

## Methodology  
## 方法论

The project combines futures trading domain knowledge with structured data interpretation. Key concepts include:

本项目结合期货交易领域知识与结构化数据解读方法，核心概念包括：

- Price acceptance vs. rejection  
  价格接受与价格拒绝

- Volume expansion and price response  
  成交量扩张与价格反应

- VWAP hold / reclaim behavior  
  VWAP 持稳与重新站上行为

- POC, VAH, VAL, HVN, and LVN interaction  
  POC、VAH、VAL、HVN 与 LVN 关键位置互动

- Auction imbalance and failed breakout behavior  
  拍卖失衡与失败突破行为

- COT positioning pressure and commercial / managed money flow  
  COT 持仓压力与商业交易者 / 管理基金资金流向

- Basis movement between physical and futures markets  
  现货与期货市场之间的基差变化

- News-driven event flagging and paraphrased narrative signals  
  新闻事件驱动的事件标记与改写型市场叙事信号

---

## Planned Updates  
## 后续计划

This repository will continue to be updated as the project progresses. Future additions may include:

本仓库将随着项目推进持续更新，后续可能加入以下内容：

- Additional futures market signal frameworks  
  更多期货市场信号框架

- Basis Sentiment Service documentation  
  基差情绪服务文档

- Reuters / EIA narrative sentiment pipeline notes  
  Reuters / EIA 市场叙事情绪管线说明

- COT scoring and positioning interpretation notes  
  COT 打分与持仓解读笔记

- Commodity basis and spread analysis examples  
  商品基差与价差分析案例

- Market Profile / ROOTS methodology notes  
  Market Profile / ROOTS 方法论笔记

- Data source validation notes  
  数据源验证说明

- Sample structured market intelligence outputs  
  结构化市场情报输出样例

---

## Disclaimer  
## 免责声明

This repository is for educational and research documentation purposes only. It does not constitute investment advice, trading advice, or a recommendation to buy or sell any financial instrument.

本仓库仅用于教育、研究与项目文档记录目的，不构成投资建议、交易建议，也不构成任何金融工具的买入或卖出推荐。
