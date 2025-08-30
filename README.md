# 🚀 LLM-as-a-Judge Data Pipeline

Automated Snowflake → Google Sheets data pipeline for LLM evaluation metrics.

## 📊 Features

- **18+ Metrics Supported**: Sentiment Analysis, Categorizing, Tools, Transfer metrics, and more
- **Multi-Department**: Handles 9 departments with department-specific processing
- **Smart Processing**: Special behaviors for different metric types (dual tabs, quad tabs, A/B formatting)
- **Robust Error Handling**: API limits, retries, batch processing for large datasets
- **Professional Scheduling**: 3x daily automated runs via GitHub Actions

## 🏗️ Architecture

```
Snowflake DB ──> Python Script ──> Google Sheets
    │               │                    │
    ├─ Raw Tables   ├─ Data Processing   ├─ Raw Data Tabs
    └─ Summary      └─ Column Mapping    └─ Snapshot Updates
```

## ⚙️ Setup Instructions

### 1. Fork/Clone Repository

```bash
git clone https://github.com/your-username/llm-judge-pipeline.git
cd llm-judge-pipeline
```

### 2. Environment Variables

Set these in **GitHub Repository Settings → Secrets and variables → Actions**:

```bash
# Snowflake Connection
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password  
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Google Service Account (entire JSON file content)
GOOGLE_CREDENTIALS_JSON={"type":"service_account","project_id":"..."}
```

### 3. Google Service Account Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create/select a project
3. Enable **Google Sheets API**
4. Create **Service Account** 
5. Download **credentials.json**
6. Copy entire JSON content to `GOOGLE_CREDENTIALS_JSON` secret

### 4. Google Sheets Permissions

Share your target Google Sheets with the service account email:
```
your-service-account@project-id.iam.gserviceaccount.com
```
Grant **Editor** permissions.

## 🕐 Scheduling

Pipeline runs automatically:
- **11:00 AM UTC** daily
- **2:00 PM UTC** daily  
- **6:00 PM UTC** daily

Adjust timezone in `.github/workflows/pipeline.yml` cron expressions.

## 🧪 Testing

### Manual Trigger
1. Go to **Actions** tab in GitHub
2. Select **"LLM Judge Data Pipeline"**
3. Click **"Run workflow"**
4. Optional: Set parameters (date, metric, departments)

### Local Testing
```bash
# Copy credentials.json to project root
cp /path/to/credentials.json .

# Create .env file with your variables
cp .env.example .env

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python "Snowflake Direct to Sheets/snowflake_to_sheets.py"

# Test specific metric
ONLY_METRIC=tools python "Snowflake Direct to Sheets/snowflake_to_sheets.py"

# Test specific department
ONLY_DEPARTMENTS="CC Sales" python "Snowflake Direct to Sheets/snowflake_to_sheets.py"
```

## 📋 Supported Metrics

| Metric | Departments | Raw Table | Special Behavior |
|--------|-------------|-----------|------------------|
| Sentiment Analysis | All | `sa_raw_data` | Standard |
| Categorizing | MV Resolvers, Doctors | `categorizing_raw_data` | Dual tabs |
| Tools | Filipina, CC Sales, Doctors, MV Resolvers | Multiple | Quad tabs (MV), Dual tabs (Others) |
| Transfer Escalation | CC Sales | `TRANSFER_ESCALATION_RAW_DATA` | A (B) format |
| Transfer Known Flow | CC Sales | `TRANSFER_KNOWN_FLOW_RAW_DATA` | A (B) format |
| Policy Violation | Filipina, MV Resolvers | Multiple | Combined A (B) format |
| Loss of Interest | Filipina | `loss_interest_raw_data` | Cell merging |
| Clinic Rec Reason | Doctors | `CLINIC_RECOMMENDATION_REASON_RAW_DATA` | Raw + Summary |
| + 10 more... | Various | Various | Various |

## 🔧 Configuration

### Metric Aliases
Use these shortcuts with `ONLY_METRIC`:
- `sa` → sentiment_analysis
- `tools` → tools
- `te` → transfer_escalation
- `tkf` → transfer_known_flow
- `pv` → policy_violation
- `loi` → loss_of_interest
- `crr` → clinic_recommendation_reason

### Department Mapping
Environment variable format: `ONLY_DEPARTMENTS="CC Sales,Doctors"`

Valid departments:
- MV Resolvers, MV Sales
- CC Resolvers, CC Sales  
- Filipina, Ethiopian, African
- Doctors, Delighters

## 📊 Monitoring

### View Logs
1. Go to **Actions** tab
2. Click on workflow run
3. Expand job steps to see detailed logs

### Error Alerts
GitHub automatically sends email notifications for failed runs.

## 🛠️ Troubleshooting

### Common Issues
- **403 Permission Error**: Share Google Sheets with service account
- **Snowflake Connection**: Check credentials and network access
- **No Data Found**: Verify date format and department names
- **API Limits**: Pipeline handles retries automatically

### Debug Commands
```bash
# Check specific date
DATE_STR=2025-08-27 python "Snowflake Direct to Sheets/snowflake_to_sheets.py"

# Skip snapshots for faster testing  
SKIP_SNAPSHOTS=true ONLY_METRIC=tools python "Snowflake Direct to Sheets/snowflake_to_sheets.py"
```

## 🔄 Making Updates

1. **Edit code locally**
2. **Test changes** with manual runs
3. **Commit and push** to GitHub:
   ```bash
   git add .
   git commit -m "Add new metric: xyz"
   git push origin main
   ```
4. **Next scheduled run** uses updated code automatically! ✨

## 📈 Cost

- **$0/month** - Completely free on GitHub Actions
- **2,000 free minutes/month** (this pipeline uses ~15 minutes/month)
- **Private repository supported**

---

**🎯 Ready to deploy!** Push to GitHub and your pipeline will start running automatically at 11 AM, 2 PM, and 6 PM daily.
