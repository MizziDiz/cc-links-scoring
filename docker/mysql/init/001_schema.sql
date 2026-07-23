CREATE TABLE IF NOT EXISTS pages (
    url_hash BINARY(32) NOT NULL,
    url TEXT NOT NULL,
    domain VARCHAR(253),
    crawl VARCHAR(64),
    timestamp VARCHAR(32),
    tld VARCHAR(32),
    country VARCHAR(255),
    bucket VARCHAR(255),
    engine_category VARCHAR(255),
    engine_name VARCHAR(255),
    outlink_count INT,
    fetched_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (url_hash),
    INDEX idx_pages_domain (domain),
    INDEX idx_pages_bucket (bucket),
    INDEX idx_pages_engine_name (engine_name),
    INDEX idx_pages_engine_category (engine_category),
    INDEX idx_pages_country (country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS links (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_url_hash BINARY(32) NOT NULL,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    target_domain VARCHAR(253),
    anchor_text TEXT,
    PRIMARY KEY (id),
    INDEX idx_links_source (source_url_hash),
    INDEX idx_links_target_domain (target_domain),
    CONSTRAINT fk_links_page
        FOREIGN KEY (source_url_hash) REFERENCES pages (url_hash)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
