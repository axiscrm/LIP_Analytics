# LIP Analytics Dashboard

A Flask-based analytics dashboard for tracking life insurance adviser performance. Displays talk time, quotes, applications, inforce policies, and lead pipeline metrics with daily breakdowns and trend charts.

## Project Structure

```
├── app.py                  # Flask application (routes, DB queries, business logic)
├── db.py                   # MySQL connection pool
├── requirements.txt        # Python dependencies
├── settings.json           # Persisted dashboard settings (targets/thresholds)
├── Procfile                # Gunicorn config for PaaS deployments
├── .env                    # Environment variables (not committed)
├── .env.example            # Template for .env
├── templates/
│   ├── dashboard.html      # Main dashboard template
│   ├── login.html          # Password login page
│   └── error.html          # Error page
└── static/
    └── avatars/            # Adviser profile images
```

## Prerequisites

- Python 3.10+
- MySQL database with the required tables (`noojee_callrecord`, `reports_userstats`, `leads_lead`, `leads_leadquote`, `leads_leadaction`, `leads_leadschedule`, `auth_user`, `account_userprofile`, `account_usergroup_users`)

## Local Setup

1. **Clone and create a virtual environment:**

   ```bash
   git clone <repo-url> && cd LIP_Analytics
   python -m venv venv
   source venv/bin/activate   # Linux/macOS
   venv\Scripts\activate      # Windows
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**

   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your database credentials and settings:

   | Variable             | Description                                                     |
   |----------------------|-----------------------------------------------------------------|
   | Variable             | Description                                                     |
   | `DB_HOST`            | MySQL host                                                      |
   | `DB_PORT`            | MySQL port (default `3306`)                                     |
   | `DB_NAME`            | Database name                                                   |
   | `DB_USER`            | Database username                                               |
   | `DB_PASSWORD`        | Database password                                               |
   | `LIP_GROUP_ID`       | User group ID to filter advisers (default `56`)                 |
   | `SECRET_KEY`         | Flask session secret key                                        |
   | `DASHBOARD_PASSWORD` | Password for dashboard login (leave empty to disable auth)      |
   | `AWS_SECRET_NAME`    | *(Optional)* AWS Secrets Manager secret name for DB credentials |
   | `AWS_REGION`         | *(Optional)* AWS region (default `ap-southeast-2`)              |

4. **Run the development server:**

   ```bash
   python app.py
   ```

   The app will be available at `http://localhost:5001`.

## Production Deployment (Ubuntu + Gunicorn + Nginx)

### 1. Set up the application

```bash
cd /var/www/vhosts/lip_analytics
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit with production values
```

### 2. Create the systemd service

Create `/etc/systemd/system/lip_analytics.service`:

```ini
[Unit]
Description=lip_analytics gunicorn service
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/vhosts/lip_analytics
Environment="PATH=/var/www/vhosts/lip_analytics/venv/bin"
ExecStart=/var/www/vhosts/lip_analytics/venv/bin/gunicorn \
  --workers 2 \
  --timeout 120 \
  --bind 127.0.0.1:8000 \
  app:app
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

> **Note:** `--timeout 120` is required. The dashboard route runs multiple sequential DB queries that can exceed the default 30-second worker timeout.

```bash
systemctl daemon-reload
systemctl enable lip_analytics
systemctl start lip_analytics
```

### 3. Configure Nginx

Create `/etc/nginx/sites-available/lip_analytics`:

```nginx
server {
    listen 80;
    server_name lipdashboard.axiscrm.com.au;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
    }
}
```

```bash
ln -s /etc/nginx/sites-available/lip_analytics /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
```

### 4. Management commands

```bash
systemctl status lip_analytics    # check status
systemctl restart lip_analytics   # restart after code changes
journalctl -u lip_analytics -f    # tail logs
```

## AWS Secrets Manager (Optional)

Instead of storing DB credentials in `.env`, you can load them from AWS Secrets Manager.

1. Create a secret in AWS Secrets Manager with the following JSON keys:

   ```json
   {
     "DB_HOST": "your-rds-host.amazonaws.com",
     "DB_PORT": "3306",
     "DB_NAME": "your_db_name",
     "DB_USER": "your_db_username",
     "DB_PASSWORD": "your_db_password"
   }
   ```

2. Add the secret name to `.env`:

   ```
   AWS_SECRET_NAME=my-app/db-credentials
   AWS_REGION=ap-southeast-2
   ```

3. Ensure the server has AWS credentials available (IAM role, environment variables, or `~/.aws/credentials`).

The app will try to load DB credentials from the secret first. If the secret is not set, not found, or unreachable, it falls back to the `DB_*` variables in `.env`.

## How It Works

- **Authentication:** Simple password-based login controlled by `DASHBOARD_PASSWORD`. Leave empty to disable.
- **Data source:** Reads from a shared MySQL database (Axis CRM) via a connection pool (`db.py`).
- **Dashboard tabs:**
  - **Performance** -- talk time, quotes, applications, and inforce metrics per adviser with colour-coded thresholds.
  - **Leads Pipeline** -- assigned, contacted, no-contact, and booked funnel with conversion rates.
  - **Daily Checks** -- snapshot of today's activity per adviser.
- **Charts:** Daily trend charts for each metric, filterable by adviser and date range.
- **Auto-refresh:** A background thread polls the DB every 5 minutes. When new data appears, an SSE stream notifies the browser to reload.
- **Settings:** Dashboard targets and thresholds are saved to `settings.json` via the `/api/settings` endpoint.
