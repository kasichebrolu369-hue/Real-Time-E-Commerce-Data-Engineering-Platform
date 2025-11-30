# 🚀 Quick Start - Real-Time Dashboard

## Option 1: Direct Launch (RECOMMENDED - No Docker Needed!)

### Prerequisites
- Python 3.8+ installed
- Virtual environment activated

### Steps

1. **Activate Virtual Environment** (if not already active)
```powershell
.\ecom\Scripts\Activate.ps1
```

2. **Install Dashboard Dependencies** (one-time)
```powershell
pip install streamlit plotly pandas numpy scikit-learn matplotlib seaborn
```

3. **Run Complete Launcher** (does everything automatically!)
```powershell
.\run_dashboard.ps1
```

**The launcher will:**
- ✅ Check Python and packages
- ✅ Validate data (generate if missing)
- ✅ Check ML models (train if missing)
- ✅ Launch dashboard at http://localhost:8501

**That's it! The dashboard will open automatically!** 🎉

---

## Option 2: Manual Launch

### Step 1: Check Data Exists
```powershell
ls data\raw\users\
ls data\raw\products\
ls data\raw\transactions\
```

If no data:
```powershell
cd data_generators
python run_all_generators.py
cd ..
```

### Step 2: Check ML Models Exist
```powershell
ls models\
```

If missing models:
```powershell
cd scripts\ml
python train_recommendations.py
python train_churn_prediction.py
cd ..\..
```

### Step 3: Launch Dashboard
```powershell
streamlit run dashboard\realtime_dashboard.py --server.port 8501
```

### Step 4: Open Browser
Navigate to: **http://localhost:8501**

---

## Option 3: Docker Launch

### Prerequisites
- Docker Desktop installed and running
- 8GB+ RAM allocated to Docker

### Steps

1. **Build and Start Services**
```powershell
docker-compose -f docker-compose-minimal.yml up -d --build
```

2. **Check Services Running**
```powershell
docker ps
```

You should see:
- ecommerce-spark-master
- ecommerce-spark-worker
- ecommerce-jupyter
- ecommerce-dashboard

3. **Access Services**

| Service | URL | Purpose |
|---------|-----|---------|
| **Dashboard** | http://localhost:8501 | Real-time analytics |
| Jupyter | http://localhost:8888 | Interactive notebooks |
| Spark Master UI | http://localhost:8080 | Spark cluster status |
| Spark Worker UI | http://localhost:8081 | Worker status |

4. **Stop Services**
```powershell
docker-compose -f docker-compose-minimal.yml down
```

---

## 🎯 Dashboard Features

### Real-Time Metrics
- 💰 Total Revenue
- 🛍️ Total Orders
- 📈 Average Order Value
- 👥 Customer Count
- ✅ Completion Rate

### Interactive Charts
- 📊 Daily Revenue Trend
- 🎯 Transaction Funnel
- 📦 Category Performance
- 👥 Customer Segments
- 🔮 Churn Risk Distribution

### ML Insights
- **Churn Prediction**: View high-risk customers
- **Recommendations**: Search user-specific product suggestions
- **Risk Segmentation**: High/Medium/Low risk breakdown

### Dashboard Controls
- 🔄 Manual Refresh: Update data instantly
- ⏰ Auto-Refresh: Enable 30-second auto-updates
- 📅 Date Filter: Select custom date ranges
- 🏷️ Category Filter: Filter by product category

---

## 📊 Sample Dashboard Views

### Main Dashboard
```
┌─────────────────────────────────────────────────────────┐
│  💰 Total Revenue    🛍️ Orders    📈 AOV    👥 Customers │
│  $11.7B              500K         $23.4K    100K         │
└─────────────────────────────────────────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│  📈 Revenue Trend    │  │  🎯 Funnel           │
│  [Line Chart]        │  │  [Funnel Chart]      │
└──────────────────────┘  └──────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│  📦 Category Sales   │  │  👥 Customer Segments │
│  [Bar Chart]         │  │  [Pie Chart]          │
└──────────────────────┘  └──────────────────────┘
```

### ML Insights Tab
```
Churn Prediction:
┌────────────────────────────────────────────┐
│  🔴 High Risk: 24,256 customers            │
│  🟡 Medium Risk: 819 customers             │
│  🟢 Low Risk: 16,102 customers             │
└────────────────────────────────────────────┘

High-Risk Customers (Top 10):
┌──────────────┬─────────────────┬────────────┐
│ User ID      │ Churn Prob      │ Predicted  │
├──────────────┼─────────────────┼────────────┤
│ USER-12345   │ 98.5%           │ Yes        │
│ USER-67890   │ 97.2%           │ Yes        │
└──────────────┴─────────────────┴────────────┘
```

---

## 🔧 Troubleshooting

### Issue: "streamlit not found"
```powershell
pip install streamlit plotly
```

### Issue: "No data found"
```powershell
cd data_generators
python run_all_generators.py
```

### Issue: "Models not found"
```powershell
cd scripts\ml
python train_recommendations.py
python train_churn_prediction.py
```

### Issue: Port 8501 already in use
```powershell
# Use different port
streamlit run dashboard\realtime_dashboard.py --server.port 8502
```

### Issue: Dashboard is slow
- Reduce date range filter
- Select specific category filter
- Disable auto-refresh
- Close other applications

### Issue: Docker container fails to start
```powershell
# Check Docker is running
docker ps

# View logs
docker logs ecommerce-dashboard

# Rebuild
docker-compose -f docker-compose-minimal.yml up -d --build
```

---

## 💡 Tips & Tricks

### Performance Optimization
1. **Use date filters**: Narrow down to recent data
2. **Filter by category**: Analyze specific product lines
3. **Disable auto-refresh**: Manually refresh when needed

### Data Refresh
- Data is cached for 5 minutes (300 seconds)
- Click "🔄 Refresh Data" to reload immediately
- Enable auto-refresh for real-time monitoring

### Keyboard Shortcuts
- `R` - Refresh dashboard
- `Ctrl+C` - Stop dashboard (in terminal)
- `F11` - Fullscreen mode (browser)

### Export Data
- Right-click charts → "Download plot as PNG"
- Click table → Copy to clipboard
- Use browser's built-in screenshot tools

---

## 📈 Next Steps

### Immediate
1. ✅ Launch dashboard
2. ✅ Explore key metrics
3. ✅ Check ML insights
4. ✅ Filter by date/category

### Short-term
1. Share dashboard with team
2. Create custom views
3. Export reports
4. Set up monitoring alerts

### Long-term
1. Deploy to cloud (AWS/Azure/GCP)
2. Add user authentication
3. Integrate with BI tools (PowerBI/Tableau)
4. Schedule automated reports

---

## 🎉 You're Ready!

Run this command and you're all set:

```powershell
.\run_dashboard.ps1
```

The dashboard will:
1. Check everything automatically
2. Generate missing data if needed
3. Train models if needed
4. Launch at http://localhost:8501

**It's that simple!** 🚀

---

## 📞 Need Help?

- **Launcher Script**: `.\run_dashboard.ps1` (does everything)
- **Manual Launch**: `streamlit run dashboard\realtime_dashboard.py`
- **Docker Launch**: `docker-compose -f docker-compose-minimal.yml up -d`

**Choose the launcher script for the easiest experience!** ✨
