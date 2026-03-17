# Gemini Compatibility Layer

`src/gemini` is kept as a lightweight compatibility wrapper around
`vlm_pipeline.lib.gemini`.

What is restored here:
- CLI entrypoint for video analysis
- wrapper module for legacy imports
- environment-based config helper
- local requirements list

Docker install source of truth:
- `docker/app/requirements.txt`

Compatibility-only dependency file:
- `src/gemini/requirements.txt`

Gemini credentials are expected from environment variables, in this order:
1. `GEMINI_GOOGLE_APPLICATION_CREDENTIALS`
2. `GOOGLE_APPLICATION_CREDENTIALS`
3. `GEMINI_SERVICE_ACCOUNT_JSON`

For Docker Compose in this repository, put these values in `docker/.env`:

```dotenv
GEMINI_PROJECT=gmail-361002
GEMINI_LOCATION=us-central1
GEMINI_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"gmail-361002","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n","client_email":"gemini-api@gmail-361002.iam.gserviceaccount.com","client_id":"...","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"https://www.googleapis.com/robot/v1/metadata/x509/gemini-api%40gmail-361002.iam.gserviceaccount.com","universe_domain":"googleapis.com"}
```

When `GEMINI_SERVICE_ACCOUNT_JSON` is present, the runtime writes an ephemeral
credential file to `/tmp/gemini-service-account.json` and points
`GOOGLE_APPLICATION_CREDENTIALS` to it automatically.
