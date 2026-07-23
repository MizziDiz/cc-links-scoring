#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

is_true() {
    case "${1:-}" in
        1 | true | TRUE | yes | YES | on | ON) return 0 ;;
        *) return 1 ;;
    esac
}

set -- countries

if [ -n "${PIPELINE_CONFIG:-}" ]; then
    set -- "$@" --config "$PIPELINE_CONFIG"
fi
if [ -n "${PIPELINE_DB:-}" ]; then
    set -- "$@" --db "$PIPELINE_DB"
fi
if [ -n "${PIPELINE_CANDIDATES_FILE:-}" ]; then
    set -- "$@" --candidates-file "$PIPELINE_CANDIDATES_FILE"
fi
if [ -n "${PIPELINE_CATEGORIES_FILE:-}" ]; then
    set -- "$@" --categories-file "$PIPELINE_CATEGORIES_FILE"
fi
if [ -n "${PIPELINE_COUNTRIES:-}" ]; then
    # ccTLDs are intentionally split on spaces: "co cl pe".
    # shellcheck disable=SC2086
    set -- "$@" --countries $PIPELINE_COUNTRIES
fi
if [ -n "${PIPELINE_PRIORITIES_FILE:-}" ]; then
    set -- "$@" --priorities "$PIPELINE_PRIORITIES_FILE"
fi
if [ -n "${PIPELINE_EXCLUDE_FILE:-}" ]; then
    set -- "$@" --exclude-file "$PIPELINE_EXCLUDE_FILE"
fi
if [ -n "${PIPELINE_CRAWL:-}" ]; then
    set -- "$@" --crawl "$PIPELINE_CRAWL"
fi
if [ -n "${PIPELINE_PER_CATEGORY_LIMIT:-}" ]; then
    set -- "$@" --per-category-limit "$PIPELINE_PER_CATEGORY_LIMIT"
fi
if [ -n "${PIPELINE_TOTAL_LIMIT:-}" ]; then
    set -- "$@" --total-limit "$PIPELINE_TOTAL_LIMIT"
fi
if [ -n "${PIPELINE_PER_COUNTRY_LIMIT:-}" ]; then
    set -- "$@" --per-country-limit "$PIPELINE_PER_COUNTRY_LIMIT"
fi
if [ -n "${PIPELINE_MAX_PER_DOMAIN:-}" ]; then
    set -- "$@" --max-per-domain "$PIPELINE_MAX_PER_DOMAIN"
fi
if [ -n "${PIPELINE_MAX_PARTS:-}" ]; then
    set -- "$@" --max-parts "$PIPELINE_MAX_PARTS"
fi
if [ -n "${PIPELINE_DISCOVER_DELAY:-}" ]; then
    set -- "$@" --discover-delay "$PIPELINE_DISCOVER_DELAY"
fi
if [ -n "${PIPELINE_WORKERS:-}" ]; then
    set -- "$@" --workers "$PIPELINE_WORKERS"
fi
if [ -n "${PIPELINE_RATE_LIMIT:-}" ]; then
    set -- "$@" --rate-limit "$PIPELINE_RATE_LIMIT"
fi
if [ -n "${PIPELINE_COMMIT_EVERY:-}" ]; then
    set -- "$@" --commit-every "$PIPELINE_COMMIT_EVERY"
fi
if [ -n "${PIPELINE_SHARD:-}" ]; then
    set -- "$@" --shard "$PIPELINE_SHARD"
fi

case "${PIPELINE_SOURCE:-cloudfront}" in
    cloudfront | s3)
        set -- "$@" --source "$PIPELINE_SOURCE"
        ;;
    gateway)
        if [ -z "${GATEWAY_HOST:-}" ]; then
            echo "PIPELINE_SOURCE=gateway requires GATEWAY_HOST in .env" >&2
            exit 2
        fi
        if [ -z "${GATEWAY_CRED:-}" ] || [ "$GATEWAY_CRED" = "**PUT_IN_ENV**" ]; then
            echo "PIPELINE_SOURCE=gateway requires GATEWAY_CRED in .env" >&2
            exit 2
        fi
        CC_GATEWAY_PROXY="${GATEWAY_SCHEME:-http}://${GATEWAY_CRED}@${GATEWAY_HOST}"
        export CC_GATEWAY_PROXY
        set -- "$@" --source cloudfront
        ;;
    *)
        echo "PIPELINE_SOURCE must be cloudfront, s3, or gateway" >&2
        exit 2
        ;;
esac

if is_true "${PIPELINE_DISCOVERY_ONLY:-false}"; then
    set -- "$@" --discovery-only
fi
if is_true "${PIPELINE_SKIP_DISCOVERY:-false}"; then
    set -- "$@" --skip-discovery
fi
if is_true "${PIPELINE_NO_LINKS:-true}"; then
    set -- "$@" --no-links
else
    set -- "$@" --store-links
fi

exec python /app/pipeline.py "$@"
