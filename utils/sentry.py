import sentry_sdk
from sentry_sdk.integrations.loguru import LoguruIntegration
from sentry_sdk.integrations.loguru import LoggingLevels
from decouple import config

sentry_loguru = LoguruIntegration(
    level=LoggingLevels.INFO.value,        # Capture info and above as breadcrumbs
    event_level=LoggingLevels.ERROR.value  # Send errors as events
)
if dsn := config('SENTRY_DSN_URL'):
    # Success - dsn has been init
    sentry_sdk.init(
        dsn=dsn,
        traces_sample_rate=0.5,
        include_local_variables=True,
        integrations=[
            sentry_loguru,
        ],
    )
else:
    raise Exception("Sentry config not found")