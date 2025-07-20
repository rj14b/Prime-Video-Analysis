# 📊 Amazon Prime Video ETL Analytics Dashboard (Azure + Databricks + Power BI)

## 🚀 Project Overview

This comprehensive data analytics project builds a cloud-native ETL pipeline using Azure Data Factory, Data Lake Gen2, Azure Databricks, and Power BI. The project focuses on analyzing Amazon Prime Video’s content and viewer behavior, delivering insights on content popularity, binge patterns, sentiment analysis, genre preferences, age segmentation, and more. It simulates a production-grade OTT streaming platform analytics system.

This project uses a Medallion architecture pattern: Bronze (raw), Silver (cleaned), and Gold (aggregated/insightful) to design and manage the data pipeline efficiently.

## 🧱 End-to-End Architecture

```
📥 GitHub CSV Source
    ↓
🔁 Azure Data Factory (ADF) – Orchestrates ingestion
    ↓ [🥉 Bronze Layer]
🗂️ Azure Data Lake Storage Gen2 – Stores raw CSVs
    ↓ [🥈 Silver Layer]
🔧 Azure Databricks (PySpark) – Cleans, enriches, and transforms
    ↓ [🥇 Gold Layer]
🧊 Delta Lake – Optimized, query-ready layer
    ↓
📊 Power BI – Visualizes business insights
```

## 💡 Why Medallion Architecture?

| Layer     | Description                          | Purpose                                                    |
| --------- | ------------------------------------ | ---------------------------------------------------------- |
| 🥉 Bronze | Raw data as-is from source           | Full fidelity, rollback-friendly, best for audit/debugging |
| 🥈 Silver | Cleaned & structured data            | Applied cleansing (nulls, splits), ready for analytics/ML  |
| 🥇 Gold   | Enriched, business-logic transformed | Sentiment scoring, viewership logic, KPIs for BI tools     |

## 📂 Dataset Used

* **Source**: Kaggle – Amazon Prime Titles Dataset
* **Fields**: show_id, title, type, director, release_year, duration, rating, country, listed_in, description

The data was extended in Databricks with synthetic fields like `viewer_count`, `stars`, `review_text`, `watch_time`, and `popularity_score`.


## 🧠 Databricks Transformations (Gold Layer)

> Notebook: `Prime_Gold_Layer_Transformation.py`

More than 30 transformations were applied. Key ones include:

### 🎯 Genre Extraction

```py
df_gold = df_gold.withColumn("category1", split(df_gold["listed_in"], ",")[0])
df_gold = df_gold.withColumn("category2", split(df_gold["listed_in"], ",")[1])
```

Breaks multi-genre strings into separate columns.

### 📅 Content Freshness Classification

```py
df_gold = df_gold.withColumn("movie_release_type",
    when((year(current_date()) - col("release_year")) <= 2, "New")
    .when((year(current_date()) - col("release_year")) <= 5, "Moderate")
    .otherwise("Old"))
```

### ⭐ IMDb Rating + Star Emojis

```py
df_gold = df_gold.withColumn("imdb_rating", round(rand() * 5.5 + 4, 1))
df_gold = df_gold.withColumn("stars", repeat(lit("⭐"), floor(col("imdb_rating") / 2)))
```

Simulated ratings between 4.0 to 9.5 and converted into star indicators.

### 💬 Sentiment Review Generator

```py
df_gold = df_gold.withColumn("review_text",
    when(col("imdb_rating") >= 8.5, concat(lit("An outstanding "), col("type"), lit("...")))
    .otherwise(lit("A below-average " + col("type") + " experience...")))
```

Auto-generates content review blurbs.

### 👀 Viewership Estimate + Watch Behavior

```py
df_gold = df_gold.withColumn("viewer_count", floor(rand(seed=42) * 999000 + 1000))
df_gold = df_gold.withColumn("watch_level",
    when(col("viewer_count") >= 1000000, "Highly Watched"))
df_gold = df_gold.withColumn("most_traffic_time",
    when(col("listed_in").rlike("(?i)Horror|Thriller"), "10PM–1AM")
    .when(col("listed_in").rlike("(?i)Kids|Animation"), "8AM–12PM")
    .otherwise("12PM–4PM"))
```

### 🔁 Replay Value & Nostalgia Score

```py
df_gold = df_gold.withColumn("replay_button_probability",
    when((col("imdb_rating") > 8) & (col("duration") < "90 min"), "High"))
df_gold = df_gold.withColumn("nostalgia_factor",
    when(col("release_year") < 2010, "High").otherwise("Low"))
```

### 🎬 Sequel Potential Estimator

```py
df_gold = df_gold.withColumn("sequel_potential",
    when((col("viewer_count") > 500000) & (col("imdb_rating") > 7), "Yes")
    .otherwise("Maybe"))
```

### 🧊 Delta Write to Gold

```py
df_gold.write.format("delta").mode("overwrite").save("<ADLS gold layer path>")
```


## 📊 Power BI Dashboard Breakdown
## 📊 Power BI Dashboard Pages Breakdown

---

### 🔍 Content Analysis Page (Title-Level Insight)

> **Purpose**: View a detailed profile of any selected title — powered by AI metrics and viewer data.

#### 🎯 Main Features:

- **🔽 Title Slicer**: Select any movie/show to update the page dynamically.
- **📇 Metadata Panel**: Displays title, type, year, duration, rating, genre, director, cast, and languages.
- **💬 AI Review Text**: Auto-generated based on IMDb rating (e.g., “An outstanding Movie...”).
- **⭐ Star Emoji Rating**: Rendered using rating logic (e.g., ⭐⭐⭐⭐).
- **👁️ Viewer Metrics**:
  - Viewer Count (e.g., 148M)
  - Replay Button Probability (High/Low)
  - Sequel Potential (Yes/Maybe)
  - Nostalgia Factor (High/Low)

#### 🏷 Verdict Tags:

- **Hot or Not**: Trending vs Not Trending
- **Audience Verdict**: Loved / Average / Mixed
- **Binge-Worthiness**: High / Limited
- **Box Office Tag**: e.g., Hidden Gem

✅ This page provides AI-enriched insights for evaluating a title’s quality, popularity, and rewatch value — ideal for content teams, marketers, and strategy analysts.

![Content Analysis](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Content%20Analysis.png)

---

### 📊 Performance Summary Page

> **Purpose**: Monitor overall platform performance by analyzing content types, viewer engagement, and yearly trends.

#### 📌 Key KPIs:

- **Total Titles on Platform**: 907
- **Total Audience Reach**: 446M
- **Average IMDb Rating**: 6.78
- **Replay Worthy Content %**: 46.31%
- **Bingeability Score**: 20.51%
- **Popularity Health**: 36.16%

#### 📈 Visuals & Insights:

- **📊 Content Share vs Viewer Share**:
  - Compares the proportion of Movies vs TV Shows in terms of content count and total viewership.
- **📆 Yearly Evolution Chart**:
  - Displays trends in content production and audience growth from 2018 to 2021.
- **🌍 Country-wise Contribution (Top 5)**:
  - Visualizes content and viewership share from major producing countries like USA, India, UK, etc.

✅ This page helps stakeholders track platform growth, consumption patterns, and content mix efficiency over time.

![Performance Summary](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Performance%20Summary.png)


---

### 👥 Audience Pulse Page

> **Purpose**: Understand how different age groups interact with genres and what ratings dominate viewing patterns.

#### 🧑‍🤝‍🧑 Audience Segmentation:

- **Age Groups**: Kids, Teens, Adults, General Audience
- **Top Age Group by Viewership**: General Audience
- **Average Viewers per Genre**: 2.24M

#### 🎬 Genre Preferences by Age:

- **Heatmap / Matrix View**:
  - Rows: Age Groups
  - Columns: Genres (e.g., Action, Comedy, Documentary)
  - Values: Viewer Count

#### 📺 Rating Dominance:

- **13+, 16+, 18+, R, PG** — rating share comparison
- **Most Dominant Rating**: 13+

#### 📌 Additional Metrics:

- **Family Content Contribution**: 6.73%
- **Family Viewers Contribution**: 7.59%

✅ This page gives a demographic-driven lens on content strategy — helping target genres, ratings, and age-appropriate recommendations.

![Audience Pulse](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Audience%20Pulse.png)

---

### ⏱️ Streaming Hours Uncovered Page

> **Purpose**: Analyze when and how long users are watching—identify peak slots and session durations.

#### 🕒 Time-Slot Breakdown:

- **Bar Chart**: Viewer count by time window  
  - 8 AM–12 PM  
  - 12 PM–4 PM  
  - 5 PM–8 PM  
  - 7 PM–10 PM  
  - 10 PM–1 AM  
- **Peak Viewing Hour**: 12 PM–4 PM

#### 📅 Day-Type Comparison:

- **Stacked Bars**:  
  - Weekday Watch: 36.71%  
  - Weekend Watch: 50.83%  
  - Family Watch: 8.72%  
  - Late‑Night Watch: (remaining %)  

#### ⏳ Session Duration:

- **Average Watch Time Per User**: 57.23 minutes  
- **Peak Binge Time**: 12 PM–4 PM  

✅ Use this page to optimize content scheduling, marketing blitz times, and personalized send‑time recommendations.
![Streaming Hours](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Streaming%20Hours%20Uncovered.png)

---

### 🌐 Views by Voice Page

> **Purpose**: Understand the distribution of content and viewership across languages and regional formats.

#### 📊 Language-Wise Content Library:

- **Content Count**:
  - English: 735 titles (46.34%)
  - Hindi: 696 titles (43.88%)
  - Regional (Tamil, Telugu, etc.): 135 titles (8.51%)
  - Japanese: 15 titles
  - Chinese: 5 titles

#### 👁️ Viewer Reach by Language:

- **Viewer Count (Billions)**:
  - English: 0.18B
  - Hindi: 0.10B
  - Hindi-Tamil-Telugu Mix: 0.05B
  - Spanish: 0.02B

#### 📈 Engagement Metrics:

- **Popularity vs Bingeability Score**:
  - Scatter plot comparing titles’ popularity and how binge-worthy they are

✅ This page supports localization strategy — highlighting top-performing languages, under-leveraged regions, and future content planning across geographies.
![Language & Region](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Views%20by%20Voice.png)


---
## ✅ Key Takeaways

* End-to-end Azure data pipeline with industry-grade architecture
* Real-world transformations with PySpark and Delta Lake
* AI-inspired review generation and KPI logic
* Business-friendly Power BI dashboard for OTT domain
* Built with modularity, performance, and stakeholder usability in mind


---

## 🖼️ Dashboard Snapshots

| Page                        | Screenshot |
|----------------------------|------------|
| 🏠 Home            | ![Home](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Home.png) |
| 🔍 Content Analysis         | ![Content Analysis](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Content%20Analysis.png) |
| 📊 Performance Summary      | ![Performance](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Performance%20Summary.png) |
| 👥 Audience Pulse           | ![Audience Pulse](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Audience%20Pulse.png) |
| ⏱️ Streaming Hours Uncovered          | ![Streaming Hours](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Streaming%20Hours%20Uncovered.png) |
| 🌐 Views by Voice     | ![Language & Region](https://github.com/rj14b/Prime-Video-Analysis/blob/main/image/Views%20by%20Voice.png) |

