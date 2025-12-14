# social-media-misinformation-analysis-

## 📌 Project Overview

This project is an **end-to-end Business Intelligence (BI) solution** focused on analyzing online news data to **detect and monitor misinformation trends**, assess **source reliability**, and identify **high-risk entities** over time.

The project is developed as part of a **Business Intelligence course** and follows the complete BI lifecycle:

* Business understanding
* Data preparation & ETL
* Dimensional data modeling
* Visualization & dashboards
* Insights & recommendations

Visual analytics and dashboards will be developed using **Microsoft Power BI**.

---

## 🎯 Business Problem

The rapid growth of online news and social media platforms has increased the spread of misinformation, making it difficult for organizations, regulators, and media watchdogs to:

* Identify unreliable news sources
* Detect misinformation trends early
* Monitor entities frequently targeted by misinformation

This project aims to provide a **data-driven BI solution** that enables structured monitoring and analysis of misinformation using historical news data.

---

## 📂 Dataset Description

The dataset consists of approximately **10,000 news records** with the following main attributes:

* `News_Headline` – Headline text of the news article or claim
* `Link_Of_News` – URL of the news source
* `Source` – Publisher or origin of the claim
* `Stated_On` – Date when the claim was stated
* `Date` – Publication or fact-check date
* `Label` – Classification label indicating reliability (e.g. true, false, pants-fire)

Additional features are derived during ETL, such as headline length, label grouping, and time attributes.

---

## 🧩 BI Architecture

### 🔹 Fact Table

* **Fact_News** – One row per news article, including labels and derived metrics

### 🔹 Dimension Tables

* **Dim_Source** – News source information
* **Dim_Time** – Date, month, and year attributes
* **Dim_Entity** – Entities mentioned (`Stated_On`)
* **Dim_Label** – News reliability categories

The data model follows a **star schema** to support efficient analytical queries.

---

## 📊 Key KPIs

The project focuses on the following KPIs:

* Misinformation Rate (%)
* Misinformation Volume
* Source Misinformation Rate (%)
* Top Risk Sources
* Entity Risk Score
* Source Diversity Index
* Headline Length Risk Indicator
* Misinformation Growth Rate (time-based)

---

## 📈 Visualization & Dashboards

Dashboards will be built using **Power BI** and will include:

* Executive summary dashboard (overall misinformation trends)
* Source reliability analysis
* Time-based trend analysis
* Entity-focused risk views

Interactive slicers (date, source, label) will be used to support exploration and decision-making.

---

## 🛠 Tools & Technologies

* **Python** (Pandas, NumPy) – Data cleaning & ETL
* **Jupyter Notebook** – ETL documentation
* **Power BI** – Dashboards & visualization
* **GitHub** – Version control & collaboration

---

## 📁 Repository Structure

```
/data_cleaned        -> Cleaned datasets
/etl                 -> ETL notebooks and scripts
/model               -> Data model diagrams
/dashboard           -> Power BI dashboards
/report              -> Use case & insights reports
/presentation        -> Final presentation slides
```

---

## 👥 Team

**Group:** Major IT – Minor BA
**Members:**

* Chiheb Bahri
* Nejmedine Zahra
* Skander Triki
* Mouheb Ouselati

---

## 📌 Academic Note

This project is developed for **educational purposes**. Any derived features or transformations are clearly documented as part of the ETL process.

---

## 🚀 Future Improvements

* Advanced text analytics on headlines
* Integration of social engagement metrics
* Real-time misinformation monitoring dashboards
* Expansion to multilingual news sources

