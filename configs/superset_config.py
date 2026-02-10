# Superset configuration for PostgreSQL metadata store
import os

# Database URI for PostgreSQL
SQLALCHEMY_DATABASE_URI = 'postgresql://airflow:airflow@postgres-metadata:5432/superset'

# Secret key (should match docker-compose)
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', '9wc5+erMt60+lxrXDf3RjeIR+zONpEFusO00Np7JzfliMTI1e+RXnHcQ')

# Disable debug mode
DEBUG = False

# Enable CSRF protection
WTF_CSRF_ENABLED = True

# Session configuration
SESSION_TYPE = 'filesystem'
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

# Cache configuration (optional, using simple cache)
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Timezone
DEFAULT_TIMEZONE = 'Europe/Moscow'
