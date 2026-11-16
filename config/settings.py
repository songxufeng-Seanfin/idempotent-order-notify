"""订单通知幂等消费演示项目配置。

设计取向：开箱即跑。默认使用 SQLite + 数据库模拟 Stream，
不依赖任何外部服务；设置 NOTIFY_BACKEND=redis 才启用真实 Redis Stream。
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get(
    "NOTIFY_SECRET_KEY", "demo-only-notify-secret-key-change-me-in-production"
)
DEBUG = os.environ.get("NOTIFY_DEBUG", "1") != "0"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "notify",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = []
LANGUAGE_CODE = "zh-hans"
TIME_ZONE = "Asia/Shanghai"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ---------------------------------------------------------------------------
# 事件队列后端
#   db    —— 默认。用 NotifyEvent 表模拟 Stream 的投递/认领/确认语义
#   redis —— 真实 Redis Stream（XADD / XREADGROUP / XACK / XPENDING）
# ---------------------------------------------------------------------------
NOTIFY_BACKEND = os.environ.get("NOTIFY_BACKEND", "db").lower()
NOTIFY_REDIS_URL = os.environ.get("NOTIFY_REDIS_URL", "redis://127.0.0.1:6379/0")
NOTIFY_STREAM_KEY = os.environ.get("NOTIFY_STREAM_KEY", "order:events")
NOTIFY_GROUP = os.environ.get("NOTIFY_GROUP", "order-consumer-group")
NOTIFY_CONSUMER = os.environ.get("NOTIFY_CONSUMER", "worker-1")
